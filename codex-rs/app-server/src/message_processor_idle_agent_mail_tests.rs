//! Exercises native child eviction and reload while an unrelated client stays connected.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::ensure;
use app_test_support::MockResponsesConfig;
use app_test_support::write_models_cache;
use codex_app_server_protocol::InitializeResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ThreadLoadedListResponse;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::ThreadUnsubscribeResponse;
use codex_app_server_protocol::ThreadUnsubscribeStatus;
use codex_app_server_protocol::TurnCompletedNotification;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::TurnStatus;
use codex_core::config::ConfigBuilder;
use codex_features::Feature;
use codex_login::AuthManager;
use codex_protocol::ThreadId;
use core_test_support::responses;
use serde::de::DeserializeOwned;
use serde_json::json;
use tempfile::TempDir;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;
use tokio::time::timeout;

use super::ConnectionSessionState;
use super::MessageProcessor;
use super::message_processor_tracing_tests::build_test_processor;
use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::OutgoingEnvelope;
use crate::outgoing_message::OutgoingMessage;
use crate::transport::AppServerTransport;
use crate::transport::ConnectionOrigin;

const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
async fn unrelated_client_does_not_pin_v2_child_mail_roundtrip() -> Result<()> {
    let server = responses::start_mock_server().await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .with_model("gpt-5.5")
        .enable_feature(Feature::Collab)
        .enable_feature(Feature::MultiAgentV2)
        .with_root_config("thread_unload_delay_secs = 0\napprovals_reviewer = \"user\"")
        .write(home.path())?;
    write_models_cache(home.path()).await?;
    let config = Arc::new(
        ConfigBuilder::default()
            .codex_home(home.path().to_path_buf())
            .build()
            .await?,
    );
    let auth =
        AuthManager::shared_from_config(config.as_ref(), /*enable_codex_api_key_env*/ false)
            .await?;
    let (processor, outgoing) = build_test_processor(config, auth).await;
    let (recorded, outgoing_task) = RecordedOutgoing::start(outgoing);
    let owner_id = ConnectionId(1);
    let owner = Arc::new(ConnectionSessionState::new(ConnectionOrigin::WebSocket));
    let unrelated_id = ConnectionId(2);
    let unrelated = Arc::new(ConnectionSessionState::new(ConnectionOrigin::WebSocket));
    let mut created = processor.thread_created_receiver();
    let mut listener_created = processor.thread_created_receiver();
    let (stop_listeners, mut stop_listener_rx) = oneshot::channel();
    // Mirror lib.rs's creation callback for both the initial child and its reload.
    // The production attachment code must filter these initialized clients itself.
    let listener_task = tokio::spawn({
        let processor = Arc::clone(&processor);
        async move {
            let mut attachments = JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut stop_listener_rx => break,
                    created = listener_created.recv() => {
                        let Ok(thread_id) = created else {
                            break;
                        };
                        let processor = Arc::clone(&processor);
                        attachments.spawn(async move {
                            processor
                                .try_attach_thread_listener(thread_id, vec![owner_id, unrelated_id])
                                .await;
                        });
                    }
                    completed = attachments.join_next(), if !attachments.is_empty() => {
                        if let Some(completed) = completed {
                            completed.expect("child listener attachment task");
                        }
                    }
                }
            }
            // Reap every attachment before cleanup releases processor/runtime state.
            attachments.shutdown().await;
        }
    });

    let result: Result<()> = async {
        for (connection_id, session, request_id, name) in [
            (owner_id, &owner, 1, "parent-owner"),
            (unrelated_id, &unrelated, 100, "unrelated-client"),
        ] {
            send_request(
                &processor,
                connection_id,
                session,
                json!({"id": request_id, "method": "initialize", "params": {
                    "clientInfo": {"name": name, "version": "1"},
                    "capabilities": {"experimentalApi": true}
                }}),
            )
            .await;
            let _: InitializeResponse = recorded.response(connection_id, request_id).await?;
            processor
                .connection_initialized(connection_id, session.request_attestation())
                .await;
        }
        send_request(
            &processor,
            owner_id,
            &owner,
            json!({"id": 2, "method": "thread/start", "params": {"ephemeral": false}}),
        )
        .await;
        let parent: ThreadStartResponse = recorded.response(owner_id, 2).await?;
        let parent_id = ThreadId::from_string(&parent.thread.id)?;
        let mut child_id = None;
        let mut original_child_runtime = None;

        for (phase, (tool, args)) in [
            (
                "spawn_agent",
                json!({
                    "task_name": "worker", "fork_turns": "none", "message": "Initial child task"
                }),
            ),
            (
                "send_message",
                json!({"target": "worker", "message": "Unread note for later"}),
            ),
            (
                "followup_task",
                json!({"target": "worker", "message": "Continue the child task"}),
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let call_id = format!("idle-eviction-{tool}");
            let _call = responses::mount_sse_once_match(
                &server,
                wiremock::matchers::header("thread-id", parent.thread.id.clone()),
                responses::sse(vec![
                    responses::ev_function_call_with_namespace(
                        &call_id,
                        "collaboration",
                        tool,
                        &args.to_string(),
                    ),
                    responses::ev_completed("parent-call"),
                ]),
            )
            .await;
            let _parent_done = responses::mount_sse_once_match(
                &server,
                move |request: &wiremock::Request| {
                    String::from_utf8_lossy(&request.body).contains(&call_id)
                },
                responses::sse(vec![responses::ev_completed("parent-done")]),
            )
            .await;
            if tool != "send_message" {
                let parent_id = parent.thread.id.clone();
                let _child = responses::mount_sse_once_match(
                    &server,
                    move |request: &wiremock::Request| {
                        request
                            .headers
                            .get("thread-id")
                            .is_some_and(|id| id != parent_id.as_str())
                    },
                    responses::sse(vec![
                        responses::ev_assistant_message(
                            "child-result",
                            "Remember the initial child result",
                        ),
                        responses::ev_completed("child-done"),
                    ]),
                )
                .await;
            }
            let first_event = recorded.len().await;
            let request_id = 10 + phase as i64;
            send_request(
                &processor,
                owner_id,
                &owner,
                json!({"id": request_id, "method": "turn/start", "params": {
                    "threadId": parent.thread.id,
                    "input": [{"type": "text", "text": format!("Run {tool}")}]
                }}),
            )
            .await;
            let started: TurnStartResponse = recorded.response(owner_id, request_id).await?;

            if tool != "send_message" {
                let created_id = timeout(WAIT_TIMEOUT, created.recv())
                    .await
                    .context("child was not created or reloaded")??;
                if let Some(child_id) = child_id {
                    ensure!(created_id == child_id, "reload must preserve the child ID");
                } else {
                    child_id = Some(created_id);
                    let runtime = processor
                        .thread_processor
                        .thread_manager
                        .get_thread(created_id)
                        .await?;
                    original_child_runtime = Some(Arc::downgrade(&runtime));
                }
            }
            // The recorder preserves early completion notifications while RPC replies
            // are read, and the turn ID prevents a prior parent turn satisfying this wait.
            recorded
                .turn_completed(
                    first_event,
                    owner_id,
                    &parent.thread.id,
                    Some(&started.turn.id),
                )
                .await?;
            let child_id = child_id.context("spawned child")?;
            if tool != "send_message" {
                recorded
                    .turn_completed(first_event, owner_id, &child_id.to_string(), None)
                    .await?;
                continue;
            }

            // Queue-only mail must survive native unloading. Keep the owner subscribed
            // to the parent and never unsubscribe or disconnect the unrelated client.
            send_request(
                &processor,
                owner_id,
                &owner,
                json!({"id": 20, "method": "thread/unsubscribe", "params": {
                    "threadId": child_id.to_string()
                }}),
            )
            .await;
            let unsubscribed: ThreadUnsubscribeResponse = recorded.response(owner_id, 20).await?;
            ensure!(
                unsubscribed.status == ThreadUnsubscribeStatus::Unsubscribed,
                "the parent owner must inherit the initial child subscription"
            );
            let old_runtime = original_child_runtime
                .as_ref()
                .context("original child runtime")?;
            timeout(WAIT_TIMEOUT, async {
                loop {
                    let loaded = processor
                        .thread_processor
                        .thread_manager
                        .list_thread_ids()
                        .await;
                    if loaded == vec![parent_id] && old_runtime.upgrade().is_none() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .context("unrelated initialized client kept the child runtime loaded")?;
            recorded
                .wait_for(first_event, "child thread/closed", |(_, message)| {
                    let OutgoingMessage::AppServerNotification(notification) = message else {
                        return None;
                    };
                    matches!(
                        &notification.notification,
                        ServerNotification::ThreadClosed(closed)
                            if closed.thread_id == child_id.to_string()
                    )
                    .then_some(())
                })
                .await?;
            // Also prove the unrelated connection remains usable without subscribing it.
            send_request(
                &processor,
                unrelated_id,
                &unrelated,
                json!({"id": 101, "method": "thread/loaded/list", "params": {}}),
            )
            .await;
            let loaded: ThreadLoadedListResponse = recorded.response(unrelated_id, 101).await?;
            ensure!(loaded.data == vec![parent.thread.id.clone()]);
            ensure!(
                responses::received_responses_requests(&server)
                    .await
                    .iter()
                    .filter(|request| {
                        request.header("thread-id").as_deref() == Some(child_id.to_string().as_str())
                    })
                    .count()
                    == 1,
                "queue-only mail must not start another child turn"
            );
        }

        let child_id = child_id.context("spawned child")?.to_string();
        let requests = responses::received_responses_requests(&server).await;
        let child_requests: Vec<_> = requests
            .iter()
            .filter(|request| request.header("thread-id").as_deref() == Some(child_id.as_str()))
            .collect();
        ensure!(child_requests.len() == 2, "expected exactly two child turns");
        ensure!(
            child_requests[1]
                .inputs_of_type("message")
                .into_iter()
                .filter(|item| item["role"] == "assistant")
                .collect::<Vec<_>>()
                == vec![json!({
                    "type": "message", "role": "assistant",
                    "content": [{"type": "output_text", "text": "Remember the initial child result"}],
                })],
            "reloaded child must retain its prior assistant reply"
        );
        for message in ["Unread note for later", "Continue the child task"] {
            ensure!(
                child_requests[1]
                    .inputs_of_type("agent_message")
                    .iter()
                    .filter(|item| item.to_string().contains(message))
                    .count()
                    == 1,
                "reloaded child must receive {message} exactly once"
            );
        }
        Ok(())
    }
    .await;

    let _ = stop_listeners.send(());
    let listeners_stopped = listener_task.await;
    processor.clear_runtime_references();
    processor.shutdown_threads().await;
    outgoing_task.abort();
    let _ = outgoing_task.await;
    listeners_stopped.context("creation listener task failed")?;
    result
}

type RecordedMessage = (Option<ConnectionId>, OutgoingMessage);

#[derive(Clone, Default)]
struct RecordedOutgoing {
    messages: Arc<Mutex<Vec<RecordedMessage>>>,
    updated: Arc<Notify>,
}

impl RecordedOutgoing {
    fn start(mut outgoing: mpsc::Receiver<OutgoingEnvelope>) -> (Self, JoinHandle<()>) {
        let recorded = Self::default();
        let task = tokio::spawn({
            let recorded = recorded.clone();
            async move {
                // The test processor's outgoing channel is bounded. Drain throughout
                // core waits and acknowledge writes without retaining completion senders.
                while let Some(envelope) = outgoing.recv().await {
                    let message = match envelope {
                        OutgoingEnvelope::ToConnection {
                            connection_id,
                            message,
                            write_complete_tx,
                        } => {
                            if let Some(write_complete_tx) = write_complete_tx {
                                let _ = write_complete_tx.send(());
                            }
                            (Some(connection_id), message)
                        }
                        OutgoingEnvelope::Broadcast { message } => (None, message),
                    };
                    recorded.messages.lock().await.push(message);
                    // There is one sequential waiter; notify_one retains a permit if
                    // the next event arrives between inspecting the log and awaiting it.
                    recorded.updated.notify_one();
                }
            }
        });
        (recorded, task)
    }

    async fn len(&self) -> usize {
        self.messages.lock().await.len()
    }

    async fn wait_for<T>(
        &self,
        first_event: usize,
        description: &str,
        mut select: impl FnMut(&RecordedMessage) -> Option<T>,
    ) -> Result<T> {
        timeout(WAIT_TIMEOUT, async {
            loop {
                let updated = self.updated.notified();
                if let Some(found) = self.messages.lock().await[first_event..]
                    .iter()
                    .find_map(&mut select)
                {
                    return found;
                }
                updated.await;
            }
        })
        .await
        .with_context(|| format!("timed out waiting for {description}"))
    }

    async fn response<T: DeserializeOwned>(
        &self,
        expected_connection_id: ConnectionId,
        request_id: i64,
    ) -> Result<T> {
        self.wait_for(
            0,
            &format!("RPC response {request_id} on connection {expected_connection_id}"),
            |(connection_id, message)| {
                if *connection_id != Some(expected_connection_id) {
                    return None;
                }
                match message {
                    OutgoingMessage::Response(response)
                        if response.id == RequestId::Integer(request_id) =>
                    {
                        Some(
                            serde_json::to_value(&response.result)
                                .and_then(serde_json::from_value)
                                .context("response payload should deserialize"),
                        )
                    }
                    OutgoingMessage::Error(error) if error.id == RequestId::Integer(request_id) => {
                        Some(Err(anyhow::anyhow!(
                            "RPC {request_id} failed: {:?}",
                            error.error
                        )))
                    }
                    _ => None,
                }
            },
        )
        .await?
    }

    async fn turn_completed(
        &self,
        first_event: usize,
        expected_connection_id: ConnectionId,
        thread_id: &str,
        turn_id: Option<&str>,
    ) -> Result<()> {
        let completed: TurnCompletedNotification = self
            .wait_for(
                first_event,
                "matching turn/completed",
                |(connection_id, message)| {
                    if *connection_id != Some(expected_connection_id) {
                        return None;
                    }
                    let OutgoingMessage::AppServerNotification(notification) = message else {
                        return None;
                    };
                    let ServerNotification::TurnCompleted(completed) = &notification.notification
                    else {
                        return None;
                    };
                    (completed.thread_id == thread_id
                        && turn_id.is_none_or(|turn_id| completed.turn.id == turn_id))
                    .then(|| completed.clone())
                },
            )
            .await?;
        ensure!(
            completed.turn.status == TurnStatus::Completed,
            "turn did not complete successfully: {:?}",
            completed.turn
        );
        Ok(())
    }
}

async fn send_request(
    processor: &Arc<MessageProcessor>,
    connection_id: ConnectionId,
    session: &Arc<ConnectionSessionState>,
    request: serde_json::Value,
) {
    processor
        .process_request(
            connection_id,
            serde_json::from_value(request).expect("JSON-RPC request"),
            &AppServerTransport::WebSocket {
                bind_address: "127.0.0.1:0".parse().expect("loopback address"),
            },
            Arc::clone(session),
        )
        .await;
}
