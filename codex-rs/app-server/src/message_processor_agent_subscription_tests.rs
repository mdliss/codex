//! Exercises subscription inheritance against live agent trees and listener replacement.

use std::future::Future;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::ensure;
use app_test_support::MockResponsesConfig;
use app_test_support::write_models_cache_with_models;
use codex_analytics::AppServerRpcTransport;
use codex_app_server_protocol::InitializeResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::ThreadUnsubscribeParams;
use codex_app_server_protocol::ThreadUnsubscribeResponse;
use codex_app_server_protocol::ThreadUnsubscribeStatus;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::TurnStatus;
use codex_arg0::Arg0DispatchPaths;
use codex_config::CloudConfigBundleLoader;
use codex_config::LoaderOverrides;
use codex_core::CodexThread;
use codex_core::ThreadEvictionOutcome;
use codex_core::TurnInputRequest;
use codex_core::TurnInputSubmission;
use codex_core::config::Config;
use codex_core::config::ConfigBuilder;
use codex_exec_server::EnvironmentManager;
use codex_features::Feature;
use codex_feedback::CodexFeedback;
use codex_login::AuthManager;
use codex_protocol::ThreadId;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::MultiAgentVersion;
use codex_protocol::protocol::SessionSource;
use codex_protocol::user_input::UserInput;
use core_test_support::responses;
use core_test_support::streaming_sse::StreamingSseChunk;
use core_test_support::streaming_sse::start_streaming_sse_server;
use serde::de::DeserializeOwned;
use serde_json::json;
use tempfile::TempDir;
use test_case::test_case;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use wiremock::MockServer;

use super::ConnectionSessionState;
use super::MessageProcessor;
use super::MessageProcessorArgs;
use crate::analytics_utils::analytics_events_client_from_config;
use crate::config_manager::ConfigManager;
use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::ConnectionRequestId;
use crate::outgoing_message::OutgoingEnvelope;
use crate::outgoing_message::OutgoingMessage;
use crate::outgoing_message::OutgoingMessageSender;
use crate::plugin_config_reload::PluginStartupConfig;
use crate::transport::AppServerTransport;
use crate::transport::ConnectionOrigin;

const WAIT_TIMEOUT: Duration = Duration::from_secs(10);
const OWNER: ConnectionId = ConnectionId(1);
const WATCHER: ConnectionId = ConnectionId(2);
const UNRELATED: ConnectionId = ConnectionId(3);

#[test_case(false; "loaded_intermediate_without_listener")]
#[test_case(true; "stored_intermediate_without_runtime")]
#[tokio::test]
async fn v2_grandchild_inherits_root_owner_before_parent_listener(
    evict_intermediate: bool,
) -> Result<()> {
    let server = responses::start_mock_server().await;
    let mut fixture = SubscriptionFixture::new(&server.uri(), 3600).await?;
    let result: Result<()> = async {
        let root_id = fixture.start_root().await?;
        let intermediate_id = spawn_child(&fixture, &server, root_id, "intermediate").await?;
        let grandchild_id = spawn_child(&fixture, &server, intermediate_id, "leaf").await?;
        ensure!(
            fixture.unsubscribe(OWNER, intermediate_id).await?
                == ThreadUnsubscribeStatus::NotSubscribed,
            "fixture must leave the immediate parent's listener unattached"
        );
        if evict_intermediate {
            let old_runtime = evict_child(&fixture, intermediate_id).await?;
            wait_until("intermediate runtime release", || async {
                old_runtime.upgrade().is_none()
            })
            .await?;
            ensure!(
                fixture
                    .processor
                    .thread_processor
                    .thread_manager
                    .get_thread(intermediate_id)
                    .await
                    .is_err(),
                "ancestor lookup must use the stored intermediate"
            );
        }

        // Deliver the grandchild's creation callback before its parent's callback.
        // Both edges were created by actual spawn_agent calls in the same agent tree.
        fixture
            .processor
            .try_attach_thread_listener(grandchild_id, vec![OWNER, UNRELATED])
            .await;
        ensure!(
            fixture.unsubscribe(UNRELATED, grandchild_id).await?
                == ThreadUnsubscribeStatus::NotSubscribed,
            "an unrelated initialized client must not inherit a grandchild"
        );
        let reply = responses::mount_sse_once_match(
            &server,
            wiremock::matchers::header("thread-id", grandchild_id.to_string()),
            responses::sse(vec![
                responses::ev_assistant_message("grandchild", "Inherited listener works"),
                responses::ev_completed("grandchild"),
            ]),
        )
        .await;
        let runtime = fixture
            .processor
            .thread_processor
            .thread_manager
            .get_thread(grandchild_id)
            .await?;
        let turn_id = start_core_turn(&runtime, "Verify the inherited grandchild listener").await?;
        fixture
            .wait_for_turn_completed(OWNER, grandchild_id, &turn_id)
            .await?;
        ensure!(reply.requests().len() == 1);
        drop(runtime);
        ensure!(
            fixture.unsubscribe(OWNER, grandchild_id).await?
                == ThreadUnsubscribeStatus::Unsubscribed,
            "the root owner must inherit the grandchild without an attached immediate parent"
        );
        Ok(())
    }
    .await;
    fixture.shutdown().await;
    result
}

#[tokio::test]
async fn explicit_v2_child_watcher_survives_parent_owner_disconnect() -> Result<()> {
    let (release_spawn, spawn_gate) = oneshot::channel();
    let (release_parent, parent_gate) = oneshot::channel();
    let mut release_spawn = Some(release_spawn);
    let done = responses::sse(vec![
        responses::ev_assistant_message("done", "Done"),
        responses::ev_completed("done"),
    ]);
    let (server, _completions) = start_streaming_sse_server(vec![
        vec![
            StreamingSseChunk {
                gate: None,
                body: responses::sse(vec![
                    responses::ev_response_created("spawn"),
                    responses::ev_function_call_with_namespace(
                        "spawn",
                        "collaboration",
                        "spawn_agent",
                        &json!({"task_name": "worker", "fork_turns": "none", "message": "Reply done"})
                            .to_string(),
                    ),
                ]),
            },
            StreamingSseChunk {
                gate: Some(spawn_gate),
                body: responses::sse(vec![responses::ev_completed("spawn")]),
            },
        ],
        vec![StreamingSseChunk {
            gate: None,
            body: done.clone(),
        }],
        vec![StreamingSseChunk {
            gate: Some(parent_gate),
            body: done.clone(),
        }],
        vec![StreamingSseChunk {
            gate: None,
            body: done,
        }],
    ])
    .await;
    let mut fixture = SubscriptionFixture::new(server.uri(), 1).await?;
    let result: Result<()> = async {
        let root_id = fixture.start_root().await?;
        let mut created = fixture.processor.thread_created_receiver();
        let _: TurnStartResponse = fixture
            .request(
                OWNER,
                "turn/start",
                json!({"threadId": root_id.to_string(), "input": [
                    {"type": "text", "text": "Spawn a child and wait"}
                ]}),
            )
            .await?;
        let child_id = timeout(WAIT_TIMEOUT, created.recv()).await??;
        fixture
            .processor
            .try_attach_thread_listener(child_id, vec![OWNER])
            .await;
        timeout(WAIT_TIMEOUT, server.wait_for_request_count(2)).await?;
        release_spawn
            .take()
            .context("spawn completion gate")?
            .send(())
            .map_err(|_| anyhow::anyhow!("spawn response stopped before release"))?;
        timeout(WAIT_TIMEOUT, server.wait_for_request_count(3)).await?;
        let child = fixture
            .processor
            .thread_processor
            .thread_manager
            .get_thread(child_id)
            .await?;
        wait_until("initial child completion", || async {
            matches!(child.agent_status().await, AgentStatus::Completed(_))
        })
        .await?;
        drop(child);

        let resumed: ThreadResumeResponse = fixture
            .request(
                WATCHER,
                "thread/resume",
                json!({"threadId": child_id.to_string()}),
            )
            .await?;
        ensure!(resumed.thread.id == child_id.to_string());
        ensure!(
            fixture.unsubscribe(WATCHER, root_id).await? == ThreadUnsubscribeStatus::NotSubscribed,
            "the explicit child watcher must not be an ancestor subscriber"
        );
        let owner = fixture.session(OWNER);
        fixture.processor.connection_closed(OWNER, &owner).await;
        fixture
            .processor
            .try_attach_thread_listener(child_id, vec![WATCHER, UNRELATED])
            .await;

        // The parent is still sampling behind its gate, so this tests child subscription
        // retention independently of root-tree shutdown after the owner disconnects.
        tokio::time::sleep(Duration::from_millis(1200)).await;
        ensure!(
            fixture
                .processor
                .thread_processor
                .thread_manager
                .get_thread(child_id)
                .await
                .is_ok(),
            "an explicitly watched child must survive its idle deadline"
        );
        ensure!(
            fixture.unsubscribe(UNRELATED, child_id).await?
                == ThreadUnsubscribeStatus::NotSubscribed,
            "reattachment must not add an unrelated connection"
        );
        let runtime = fixture
            .processor
            .thread_processor
            .thread_manager
            .get_thread(child_id)
            .await?;
        let turn_id = start_core_turn(&runtime, "Verify the explicit child watcher").await?;
        fixture
            .wait_for_turn_completed(WATCHER, child_id, &turn_id)
            .await?;
        drop(runtime);
        ensure!(
            fixture.unsubscribe(WATCHER, child_id).await? == ThreadUnsubscribeStatus::Unsubscribed,
            "the explicit child subscription must remain intact"
        );
        Ok(())
    }
    .await;
    if let Some(release_spawn) = release_spawn {
        let _ = release_spawn.send(());
    }
    let _ = release_parent.send(());
    fixture.shutdown().await;
    server.shutdown().await;
    result
}

#[derive(Clone, Copy)]
enum ParentOwnership {
    Retained,
    Unsubscribed,
    Disconnected,
}

#[test_case(ParentOwnership::Retained; "fresh_listener_after_old_generation_teardown")]
#[test_case(ParentOwnership::Unsubscribed; "parent_unsubscribes_during_cleanup")]
#[test_case(ParentOwnership::Disconnected; "parent_disconnects_during_cleanup")]
#[tokio::test]
async fn v2_child_attachment_rechecks_ownership_after_old_cleanup(
    ownership: ParentOwnership,
) -> Result<()> {
    let server = responses::start_mock_server().await;
    let mut fixture = SubscriptionFixture::new(&server.uri(), 3600).await?;
    let mut attachment: Option<JoinHandle<()>> = None;
    let mut cleanup_sender = None;
    let mut cleanup_child = None;
    let result: Result<()> = async {
        let root_id = fixture.start_root().await?;
        let child_id = spawn_child(&fixture, &server, root_id, "worker").await?;
        cleanup_child = Some(child_id);
        fixture
            .processor
            .try_attach_thread_listener(child_id, vec![OWNER])
            .await;
        let old_runtime = evict_child(&fixture, child_id).await?;
        let (cleanup_complete, initial_receiver) = watch::channel(());
        drop(initial_receiver);
        fixture
            .processor
            .thread_processor
            .pending_thread_unloads
            .lock()
            .await
            .insert(child_id, cleanup_complete.clone());
        cleanup_sender = Some(cleanup_complete);
        attachment = Some(tokio::spawn({
            let processor = Arc::clone(&fixture.processor);
            async move {
                processor
                    .try_attach_thread_listener(child_id, vec![OWNER, UNRELATED])
                    .await;
            }
        }));
        let sender = cleanup_sender.as_ref().context("cleanup sender")?;
        let task = attachment.as_ref().context("attachment task")?;
        wait_until("attachment to observe pending cleanup", || async {
            sender.receiver_count() > 0 || task.is_finished()
        })
        .await?;
        ensure!(
            sender.receiver_count() == 1 && !task.is_finished(),
            "attachment must wait for old cleanup before looking up the replacement"
        );

        // This real endpoint finalizes stale app-server state/watch entries because core
        // has evicted the old runtime. Keep our sender alive after it removes the marker.
        ensure!(fixture.unsubscribe(OWNER, child_id).await? == ThreadUnsubscribeStatus::NotLoaded);
        ensure!(
            !fixture
                .processor
                .thread_processor
                .pending_thread_unloads
                .lock()
                .await
                .contains_key(&child_id),
            "old-generation finalization must remove its cleanup marker"
        );
        match ownership {
            ParentOwnership::Retained => {}
            ParentOwnership::Unsubscribed => {
                ensure!(
                    fixture.unsubscribe(OWNER, root_id).await?
                        == ThreadUnsubscribeStatus::Unsubscribed
                );
            }
            ParentOwnership::Disconnected => {
                let owner = fixture.session(OWNER);
                fixture.processor.connection_closed(OWNER, &owner).await;
            }
        }
        fixture
            .processor
            .thread_processor
            .thread_manager
            .ensure_multi_agent_v2_child_loaded(child_id)
            .await?;
        wait_until("old runtime release after listener teardown", || async {
            old_runtime.upgrade().is_none()
        })
        .await?;
        ensure!(
            sender.receiver_count() == 1 && !task.is_finished(),
            "the held sender must keep attachment waiting after map cleanup and reload"
        );
        drop(cleanup_sender.take());
        timeout(
            WAIT_TIMEOUT,
            attachment.as_mut().context("attachment task")?,
        )
        .await
        .context("replacement listener attachment did not finish")??;
        attachment.take();

        if matches!(ownership, ParentOwnership::Retained) {
            // Positive proof: the automatic callback must create a functioning listener
            // for the replacement, without an explicit resume repairing it first.
            let reply = responses::mount_sse_once_match(
                &server,
                wiremock::matchers::header("thread-id", child_id.to_string()),
                responses::sse(vec![
                    responses::ev_assistant_message("replacement", "Replacement listener works"),
                    responses::ev_completed("replacement"),
                ]),
            )
            .await;
            let runtime = fixture
                .processor
                .thread_processor
                .thread_manager
                .get_thread(child_id)
                .await?;
            let turn_id = start_core_turn(&runtime, "Verify the replacement listener").await?;
            fixture
                .wait_for_turn_completed(OWNER, child_id, &turn_id)
                .await?;
            ensure!(reply.requests().len() == 1);
            ensure!(
                fixture.unsubscribe(OWNER, child_id).await?
                    == ThreadUnsubscribeStatus::Unsubscribed
            );
        } else {
            // Query the same unsubscribe handler directly after disconnect, when the
            // departed connection's RPC gate is closed. This does not revive the client.
            let payload = fixture
                .processor
                .thread_processor
                .thread_unsubscribe(
                    &ConnectionRequestId {
                        connection_id: OWNER,
                        request_id: RequestId::Integer(-1),
                    },
                    ThreadUnsubscribeParams {
                        thread_id: child_id.to_string(),
                    },
                )
                .await
                .map_err(|error| anyhow::anyhow!("unsubscribe readback failed: {error:?}"))?
                .context("unsubscribe response")?;
            let response: ThreadUnsubscribeResponse =
                serde_json::from_value(serde_json::to_value(payload)?)?;
            ensure!(
                response.status == ThreadUnsubscribeStatus::NotSubscribed,
                "attachment must not restore the departed ancestor subscription"
            );
        }
        ensure!(
            fixture.unsubscribe(UNRELATED, child_id).await?
                == ThreadUnsubscribeStatus::NotSubscribed,
            "the unrelated client must remain outside the replacement subscription"
        );
        Ok(())
    }
    .await;

    if let Some(attachment) = attachment {
        attachment.abort();
        let _ = attachment.await;
    }
    drop(cleanup_sender);
    if let Some(child_id) = cleanup_child {
        fixture
            .processor
            .thread_processor
            .pending_thread_unloads
            .lock()
            .await
            .remove(&child_id);
    }
    fixture.shutdown().await;
    result
}

async fn spawn_child(
    fixture: &SubscriptionFixture,
    server: &MockServer,
    parent_id: ThreadId,
    task_name: &str,
) -> Result<ThreadId> {
    let call_id = format!("spawn-{task_name}");
    let call = responses::mount_sse_once_match(
        server,
        wiremock::matchers::header("thread-id", parent_id.to_string()),
        responses::sse(vec![
            responses::ev_function_call_with_namespace(
                &call_id,
                "collaboration",
                "spawn_agent",
                &json!({"task_name": task_name, "fork_turns": "none", "message": "Reply done"})
                    .to_string(),
            ),
            responses::ev_completed("spawn"),
        ]),
    )
    .await;
    let parent_done = responses::mount_sse_once_match(
        server,
        move |request: &wiremock::Request| {
            String::from_utf8_lossy(&request.body).contains(&call_id)
        },
        responses::sse(vec![responses::ev_completed("parent-done")]),
    )
    .await;
    let child_reply = responses::mount_sse_once_match(
        server,
        move |request: &wiremock::Request| {
            request
                .headers
                .get("thread-id")
                .is_some_and(|id| id != parent_id.to_string().as_str())
        },
        responses::sse(vec![
            responses::ev_assistant_message("child-done", "Done"),
            responses::ev_completed("child-done"),
        ]),
    )
    .await;
    let mut created = fixture.processor.thread_created_receiver();
    let parent = fixture
        .processor
        .thread_processor
        .thread_manager
        .get_thread(parent_id)
        .await?;
    start_core_turn(&parent, &format!("Spawn {task_name}")).await?;
    let child_id = timeout(WAIT_TIMEOUT, created.recv())
        .await
        .with_context(|| format!("spawn_agent did not create {task_name} under {parent_id}"))??;
    let child = fixture
        .processor
        .thread_processor
        .thread_manager
        .get_thread(child_id)
        .await?;
    ensure!(child.multi_agent_version() == Some(MultiAgentVersion::V2));
    ensure!(child.config_snapshot().await.parent_thread_id == Some(parent_id));
    wait_until("parent and child tool turns to complete", || async {
        call.requests().len() == 1
            && parent_done.requests().len() == 1
            && child_reply.requests().len() == 1
            && matches!(parent.agent_status().await, AgentStatus::Completed(_))
            && matches!(child.agent_status().await, AgentStatus::Completed(_))
    })
    .await?;
    Ok(child_id)
}

async fn start_core_turn(thread: &CodexThread, message: &str) -> Result<String> {
    match thread
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: message.to_string(),
            text_elements: Vec::new(),
        }]))
        .await?
    {
        TurnInputSubmission::Started { turn_id, .. } => Ok(turn_id),
        other => anyhow::bail!("expected a fresh turn, got {other:?}"),
    }
}

async fn evict_child(
    fixture: &SubscriptionFixture,
    child_id: ThreadId,
) -> Result<Weak<CodexThread>> {
    let manager = &fixture.processor.thread_processor.thread_manager;
    let runtime = manager.get_thread(child_id).await?;
    let weak = Arc::downgrade(&runtime);
    timeout(WAIT_TIMEOUT, async {
        loop {
            match manager.try_evict_v2_thread(Arc::clone(&runtime)).await? {
                ThreadEvictionOutcome::Evicted => return Ok::<_, anyhow::Error>(()),
                ThreadEvictionOutcome::Busy => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                ThreadEvictionOutcome::NotCurrent => {
                    anyhow::bail!("child changed before native eviction");
                }
            }
        }
    })
    .await
    .context("native child eviction did not finish")??;
    Ok(weak)
}

struct SubscriptionFixture {
    _home: TempDir,
    processor: Arc<MessageProcessor>,
    sessions: Vec<(ConnectionId, Arc<ConnectionSessionState>)>,
    responses: mpsc::Receiver<OutgoingEnvelope>,
    notifications: Arc<Mutex<Vec<(Option<ConnectionId>, ServerNotification)>>>,
    outgoing_task: JoinHandle<()>,
    next_request_id: i64,
}

impl SubscriptionFixture {
    async fn new(server_uri: &str, unload_delay_secs: u64) -> Result<Self> {
        let home = TempDir::new()?;
        MockResponsesConfig::new(server_uri)
            .with_model("gpt-5.5")
            .enable_feature(Feature::Collab)
            .enable_feature(Feature::MultiAgentV2)
            .with_root_config(&format!(
                "thread_unload_delay_secs = {unload_delay_secs}\napprovals_reviewer = \"user\""
            ))
            .write(home.path())?;
        let config = Arc::new(
            ConfigBuilder::default()
                .codex_home(home.path().to_path_buf())
                .build()
                .await?,
        );
        // Child sessions expose native V2 tools only when the model advertises V2.
        // The feature flag alone enables them only for the root session.
        let mut model = codex_core::test_support::construct_model_info_offline("gpt-5.5", &config);
        model.multi_agent_version = Some(MultiAgentVersion::V2);
        write_models_cache_with_models(home.path(), vec![model]).await?;
        let auth = AuthManager::shared_from_config(
            config.as_ref(),
            /*enable_codex_api_key_env*/ false,
        )
        .await?;
        let (processor, mut outgoing) = build_persisted_processor(config, auth).await?;
        let (response_tx, responses) = mpsc::channel(16);
        let notifications = Arc::new(Mutex::new(Vec::new()));
        let outgoing_task = tokio::spawn({
            let notifications = Arc::clone(&notifications);
            async move {
                while let Some(envelope) = outgoing.recv().await {
                    let (connection_id, message) = match envelope {
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
                    if let OutgoingMessage::AppServerNotification(notification) = message {
                        notifications
                            .lock()
                            .await
                            .push((connection_id, notification.notification));
                    } else if let Some(connection_id) = connection_id {
                        let _ = response_tx
                            .send(OutgoingEnvelope::ToConnection {
                                connection_id,
                                message,
                                write_complete_tx: None,
                            })
                            .await;
                    }
                }
            }
        });
        let mut fixture = Self {
            _home: home,
            processor,
            sessions: [OWNER, WATCHER, UNRELATED]
                .into_iter()
                .map(|id| {
                    (
                        id,
                        Arc::new(ConnectionSessionState::new(ConnectionOrigin::WebSocket)),
                    )
                })
                .collect(),
            responses,
            notifications,
            outgoing_task,
            next_request_id: 1,
        };
        for connection_id in [OWNER, WATCHER, UNRELATED] {
            let _: InitializeResponse = fixture
                .request(
                    connection_id,
                    "initialize",
                    json!({
                        "clientInfo": {"name": format!("subscription-test-{connection_id}"), "version": "1"},
                        "capabilities": {"experimentalApi": true}
                    }),
                )
                .await?;
            fixture
                .processor
                .connection_initialized(
                    connection_id,
                    fixture.session(connection_id).request_attestation(),
                )
                .await;
        }
        Ok(fixture)
    }

    fn session(&self, connection_id: ConnectionId) -> Arc<ConnectionSessionState> {
        Arc::clone(
            &self
                .sessions
                .iter()
                .find(|(id, _)| *id == connection_id)
                .expect("test connection")
                .1,
        )
    }

    async fn request<T: DeserializeOwned>(
        &mut self,
        connection_id: ConnectionId,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T> {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        self.processor
            .process_request(
                connection_id,
                serde_json::from_value(
                    json!({"id": request_id, "method": method, "params": params}),
                )?,
                &AppServerTransport::WebSocket {
                    bind_address: "127.0.0.1:0".parse()?,
                },
                self.session(connection_id),
            )
            .await;
        timeout(WAIT_TIMEOUT, async {
            while let Some(envelope) = self.responses.recv().await {
                let OutgoingEnvelope::ToConnection {
                    connection_id: actual_id,
                    message,
                    ..
                } = envelope
                else {
                    continue;
                };
                if actual_id != connection_id {
                    continue;
                }
                match message {
                    OutgoingMessage::Response(response)
                        if response.id == RequestId::Integer(request_id) =>
                    {
                        return Ok(serde_json::from_value(serde_json::to_value(
                            response.result,
                        )?)?);
                    }
                    OutgoingMessage::Error(error) if error.id == RequestId::Integer(request_id) => {
                        anyhow::bail!("{method} failed: {:?}", error.error);
                    }
                    _ => {}
                }
            }
            anyhow::bail!("outgoing channel closed before {method} response")
        })
        .await
        .with_context(|| format!("timed out waiting for {method}"))?
    }

    async fn start_root(&mut self) -> Result<ThreadId> {
        let root: ThreadStartResponse = self
            .request(OWNER, "thread/start", json!({"ephemeral": false}))
            .await?;
        Ok(ThreadId::from_string(&root.thread.id)?)
    }

    async fn unsubscribe(
        &mut self,
        connection_id: ConnectionId,
        thread_id: ThreadId,
    ) -> Result<ThreadUnsubscribeStatus> {
        let response: ThreadUnsubscribeResponse = self
            .request(
                connection_id,
                "thread/unsubscribe",
                json!({"threadId": thread_id.to_string()}),
            )
            .await?;
        Ok(response.status)
    }

    async fn wait_for_turn_completed(
        &self,
        connection_id: ConnectionId,
        thread_id: ThreadId,
        turn_id: &str,
    ) -> Result<()> {
        timeout(WAIT_TIMEOUT, async {
            loop {
                let completed = self
                    .notifications
                    .lock()
                    .await
                    .iter()
                    .find_map(|(id, event)| {
                        let ServerNotification::TurnCompleted(completed) = event else {
                            return None;
                        };
                        (*id == Some(connection_id)
                            && completed.thread_id == thread_id.to_string()
                            && completed.turn.id == turn_id)
                            .then(|| completed.clone())
                    });
                if let Some(completed) = completed {
                    ensure!(completed.turn.status == TurnStatus::Completed);
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("subscribed client did not receive the matching turn/completed")?
    }

    async fn shutdown(self) {
        drop(self.responses);
        self.processor.clear_runtime_references();
        self.processor.shutdown_threads().await;
        self.outgoing_task.abort();
        let _ = self.outgoing_task.await;
    }
}

// These tests resume native children, whose paginated history requires the same
// database-backed thread store as a real app server. The tracing harness omits it.
async fn build_persisted_processor(
    config: Arc<Config>,
    auth_manager: Arc<AuthManager>,
) -> Result<(Arc<MessageProcessor>, mpsc::Receiver<OutgoingEnvelope>)> {
    let state_db = codex_state::StateRuntime::init(
        config.sqlite_config().clone(),
        config.model_provider_id.clone(),
    )
    .await?;
    let (outgoing_tx, outgoing_rx) = mpsc::channel(16);
    let config_manager = ConfigManager::new(
        config.codex_home.to_path_buf(),
        Vec::new(),
        LoaderOverrides::with_managed_config_path_for_tests(
            config.codex_home.join("managed_config.toml").to_path_buf(),
        ),
        /*strict_config*/ false,
        CloudConfigBundleLoader::default(),
        Arg0DispatchPaths::default(),
        Arc::new(codex_config::NoopThreadConfigLoader),
    );
    let analytics_events_client =
        analytics_events_client_from_config(Arc::clone(&auth_manager), config.as_ref());
    let outgoing = Arc::new(OutgoingMessageSender::new(
        outgoing_tx,
        analytics_events_client.clone(),
    ));
    let processor = Arc::new(MessageProcessor::new(MessageProcessorArgs {
        outgoing,
        analytics_events_client,
        arg0_paths: Arg0DispatchPaths::default(),
        startup_cwd: Some(config.cwd.to_path_buf()),
        config,
        config_manager,
        environment_manager: Arc::new(EnvironmentManager::default_for_tests()),
        feedback: CodexFeedback::new(),
        log_db: None,
        state_db: Some(state_db),
        config_warnings: Vec::new(),
        session_source: SessionSource::VSCode,
        user_verification: Arc::new(crate::user_verification::Service::new(Arc::clone(
            &auth_manager,
        ))),
        auth_manager,
        installation_id: "11111111-1111-4111-8111-111111111111".to_string(),
        code_mode_session_provider: None,
        rpc_transport: AppServerRpcTransport::Stdio,
        remote_control_handle: None,
        plugin_startup_tasks: Some(PluginStartupConfig::Current),
    }));
    Ok((processor, outgoing_rx))
}

async fn wait_until<F, Fut>(description: &str, mut predicate: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    timeout(WAIT_TIMEOUT, async {
        while !predicate().await {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .with_context(|| format!("timed out waiting for {description}"))
}
