use crate::StartThreadOptions;
use crate::ThreadManager;
use crate::agent::LocalAgentControl;
use crate::agent::api::AgentControl;
use crate::agent::api::AgentInput;
use crate::agent::api::AgentTarget;
use crate::agent::api::SendRequest;
use crate::agent::api::SpawnRequest;
use crate::agent::child_config::build_agent_resume_config;
use crate::agent::next_thread_spawn_depth;
use crate::agent::registry::AgentRegistry;
use crate::agent::types::AgentMessage;
use crate::agent::types::MessageDeliveryMode;
use crate::agent::types::SpawnAgentOptions;
use crate::codex_thread::CodexThread;
use crate::config::Config;
use crate::config::ConfigBuilder;
use crate::config::test_config;
use crate::session::TurnInput;
use crate::thread_manager::ThreadManagerState;
use crate::tools::handlers::multi_agents_common::thread_spawn_source;
use codex_features::Feature;
use codex_history::InitialHistory;
use codex_history::ResumedHistory;
use codex_login::AuthManager;
use codex_login::CodexAuth;
use codex_protocol::ThreadId;
use codex_protocol::config_types::ModeKind;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::mcp::ClientMcpExtensions;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadSettingsOverrides;
use codex_protocol::protocol::ThreadSource;
use codex_protocol::protocol::TurnAbortReason;
use codex_protocol::protocol::TurnAbortedEvent;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnEnvironmentRequests;
use codex_protocol::protocol::TurnStartedEvent;
use codex_protocol::user_input::UserInput;
use codex_thread_store::LoadThreadHistoryParams;
use codex_thread_store::PersistContext;
use codex_thread_store::ReadThreadParams;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use test_case::test_case;
use tokio::sync::Notify;
use tokio::time::timeout;

#[test_case(false; "direct_eviction")]
#[test_case(true; "capacity_eviction")]
#[tokio::test]
async fn ephemeral_v2_agent_cannot_be_evicted(evict_for_capacity: bool) {
    let mut config = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    config.ephemeral = true;
    config.multi_agent_v2.max_concurrent_threads_per_session = 2;
    let temp_home = tempfile::tempdir().expect("create temp home");
    config.codex_home = temp_home.path().to_path_buf().try_into().unwrap();
    config.cwd = temp_home.path().to_path_buf().try_into().unwrap();
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let root = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await
        .expect("start ephemeral root thread");
    let control = manager.agent_control();
    let state = control
        .runtime
        .upgrade()
        .expect("thread manager should be live");
    let membership = control.runtime.admit_start().expect("admit residency work");
    let slot = control
        .reserve_v2_residency_slot(
            &state,
            &config,
            &membership,
            /*protected_thread_id*/ None,
        )
        .await
        .expect("first resident slot");
    let child = spawn_v2_subagent(
        &control,
        &state,
        config.clone(),
        root.thread_id,
        "ephemeral-worker",
    )
    .await;
    slot.commit(child.thread_id);
    mark_thread_completed(child.thread.as_ref()).await;

    if evict_for_capacity {
        match control
            .reserve_v2_residency_slot(
                &state,
                &config,
                &membership,
                /*protected_thread_id*/ None,
            )
            .await
        {
            Err(error) => assert!(matches!(
                error.details(),
                CodexErrorDetails::AgentLimitReached { max_threads: 1 }
            )),
            Ok(_) => panic!("ephemeral children must remain counted against residency capacity"),
        }
    } else {
        let outcome = manager
            .try_evict_v2_thread(Arc::clone(&child.thread))
            .await
            .expect("check whether the completed child can be evicted");
        assert_eq!(
            outcome,
            super::ThreadEvictionOutcome::Busy,
            "ephemeral children have no saved model context to reload",
        );
    }
    assert!(Arc::ptr_eq(
        &manager
            .get_thread(child.thread_id)
            .await
            .expect("ephemeral child must remain available for follow-up"),
        &child.thread,
    ));
}

#[tokio::test]
async fn residency_slot_reservation_unloads_oldest_idle_v2_agent() {
    let mut config = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    config.multi_agent_v2.max_concurrent_threads_per_session = 2;
    let temp_home = tempfile::tempdir().expect("create temp home");
    config.codex_home = temp_home.path().to_path_buf().try_into().unwrap();
    config.cwd = temp_home.path().to_path_buf().try_into().unwrap();
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let root = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await
        .expect("start root thread");
    let control = manager.agent_control();
    let state = control
        .runtime
        .upgrade()
        .expect("thread manager should be live");
    let membership = control.runtime.admit_start().expect("admit residency work");

    let first_slot = control
        .reserve_v2_residency_slot(
            &state,
            &config,
            &membership,
            /*protected_thread_id*/ None,
        )
        .await
        .expect("first resident slot");
    let first =
        spawn_v2_subagent(&control, &state, config.clone(), root.thread_id, "worker-1").await;
    first_slot.commit(first.thread_id);
    mark_thread_completed(first.thread.as_ref()).await;

    let second_slot = control
        .reserve_v2_residency_slot(
            &state,
            &config,
            &membership,
            /*protected_thread_id*/ None,
        )
        .await
        .expect("second resident slot should evict the first idle agent");
    match manager.get_thread(first.thread_id).await {
        Err(err) => match err.details() {
            CodexErrorDetails::ThreadNotFound(thread_id) => assert_eq!(*thread_id, first.thread_id),
            _ => panic!("expected evicted thread to be missing, got {err:?}"),
        },
        Ok(_) => panic!("expected evicted thread to be missing"),
    }
    let second = spawn_v2_subagent(&control, &state, config, root.thread_id, "worker-2").await;
    second_slot.commit(second.thread_id);

    assert!(manager.get_thread(root.thread_id).await.is_ok());
    assert!(manager.get_thread(second.thread_id).await.is_ok());
}

#[tokio::test]
async fn interrupted_v2_agent_is_lost_after_residency_eviction() {
    let mut config = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    config.multi_agent_v2.max_concurrent_threads_per_session = 2;
    let temp_home = tempfile::tempdir().expect("create temp home");
    config.codex_home = temp_home.path().to_path_buf().try_into().unwrap();
    config.cwd = temp_home.path().to_path_buf().try_into().unwrap();
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let root = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await
        .expect("start root thread");
    let control = manager.agent_control();
    let state = control
        .runtime
        .upgrade()
        .expect("thread manager should be live");
    let membership = control.runtime.admit_start().expect("admit residency work");

    let first_slot = control
        .reserve_v2_residency_slot(
            &state,
            &config,
            &membership,
            /*protected_thread_id*/ None,
        )
        .await
        .expect("first resident slot");
    let first =
        spawn_v2_subagent(&control, &state, config.clone(), root.thread_id, "worker-1").await;
    first_slot.commit(first.thread_id);
    mark_thread_interrupted(first.thread.as_ref()).await;

    let second_slot = control
        .reserve_v2_residency_slot(
            &state,
            &config,
            &membership,
            /*protected_thread_id*/ None,
        )
        .await
        .expect("second resident slot should evict the first interrupted idle agent");
    match manager.get_thread(first.thread_id).await {
        Err(err) => match err.details() {
            CodexErrorDetails::ThreadNotFound(thread_id) => assert_eq!(*thread_id, first.thread_id),
            _ => panic!("expected evicted thread to be missing, got {err:?}"),
        },
        Ok(_) => panic!("expected evicted thread to be missing"),
    }
    let second =
        spawn_v2_subagent(&control, &state, config.clone(), root.thread_id, "worker-2").await;
    second_slot.commit(second.thread_id);
    mark_thread_completed(second.thread.as_ref()).await;

    let err = control
        .ensure_v2_agent_loaded(config, first.thread_id, /*parent*/ None)
        .await
        .expect_err("evicted interrupted agent should stay lost");
    match err.details() {
        CodexErrorDetails::ThreadNotFound(thread_id) => assert_eq!(*thread_id, first.thread_id),
        _ => panic!("expected ThreadNotFound, got {err:?}"),
    }

    assert!(manager.get_thread(root.thread_id).await.is_ok());
    assert!(manager.get_thread(second.thread_id).await.is_ok());
    match manager.get_thread(first.thread_id).await {
        Err(err) => match err.details() {
            CodexErrorDetails::ThreadNotFound(thread_id) => assert_eq!(*thread_id, first.thread_id),
            _ => panic!("expected evicted thread to be missing, got {err:?}"),
        },
        Ok(_) => panic!("expected evicted thread to be missing"),
    }
}

#[tokio::test]
async fn ephemeral_v2_root_cannot_be_evicted() {
    let harness = RootResidencyHarness::new(None).await;
    let mut config = harness.config.clone();
    config.ephemeral = true;
    let root = harness
        .manager
        .start_thread(StartThreadOptions {
            environments: Some(Vec::new()),
            ..StartThreadOptions::new(config)
        })
        .await
        .expect("start ephemeral V2 root")
        .thread;
    mark_thread_completed(&root).await;
    assert_eq!(
        root.multi_agent_version(),
        Some(codex_protocol::protocol::MultiAgentVersion::V2),
    );
    assert!(root.config().await.ephemeral);
    assert_eq!(
        harness
            .manager
            .try_evict_v2_thread(Arc::clone(&root))
            .await
            .unwrap(),
        super::ThreadEvictionOutcome::Busy,
        "an ephemeral root has no saved model context to reload",
    );
    assert!(Arc::ptr_eq(
        &harness
            .manager
            .get_thread(root.session.thread_id)
            .await
            .unwrap(),
        &root,
    ));
    assert!(
        root.session
            .services
            .local_agent_runtime
            .admit_start()
            .is_ok()
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn manager_shutdown_fences_root_eviction_before_it_can_retain_tree() {
    let gate = Arc::new(RootResumeGate::default());
    let harness = RootResidencyHarness::new(Some(Arc::clone(&gate))).await;
    let (root, child, control) = harness.start_tree().await;
    let root_id = root.session.thread_id;
    let child_id = child.session.thread_id;
    let registry = Arc::downgrade(&control.runtime.registry);
    harness
        .send_root_mail(&control, child_id, root_id, ROOT_MAIL)
        .await;
    let delivery_barrier = timeout(
        Duration::from_secs(5),
        Arc::clone(&root.residency_gate).write_owned(),
    )
    .await
    .expect("accepted root mail finishes dispatch before eviction");
    drop(delivery_barrier);

    gate.block_stop.store(true, Ordering::SeqCst);
    let eviction = tokio::spawn({
        let manager = Arc::clone(&harness.manager);
        let root = Arc::clone(&root);
        async move { manager.try_evict_v2_thread(root).await }
    });
    timeout(Duration::from_secs(5), gate.stop_entered.notified())
        .await
        .expect("native root eviction enters session teardown before shutdown races it");
    assert!(Arc::ptr_eq(
        &harness.manager.get_thread(root_id).await.unwrap(),
        &root
    ));

    gate.block_secondary_stop.store(true, Ordering::SeqCst);
    let shutdown = tokio::spawn({
        let manager = Arc::clone(&harness.manager);
        async move {
            manager
                .shutdown_all_threads_bounded(Duration::from_secs(5))
                .await
        }
    });
    timeout(Duration::from_secs(5), control.runtime.shutdown.cancelled())
        .await
        .expect("full shutdown fences the tree while root eviction is still stopping");
    assert!(control.runtime.admit_start().is_err());
    timeout(
        Duration::from_secs(5),
        gate.secondary_stop_entered.notified(),
    )
    .await
    .expect("child teardown holds full shutdown before final registry removal");
    assert!(!shutdown.is_finished(), "root teardown is still gated");
    gate.stop_release.notify_one();

    let eviction = timeout(Duration::from_secs(5), eviction)
        .await
        .expect("eviction finishes after releasing session teardown")
        .expect("eviction task completes")
        .expect("eviction succeeds");
    assert_eq!(
        eviction,
        super::ThreadEvictionOutcome::Evicted,
        "root eviction finishes while manager shutdown is still waiting for its child",
    );
    assert!(!shutdown.is_finished());
    gate.secondary_stop_release.notify_one();
    let report = timeout(Duration::from_secs(5), shutdown)
        .await
        .expect("manager shutdown finishes")
        .expect("manager shutdown task completes");
    assert!(report.submit_failed.is_empty());
    assert!(report.timed_out.is_empty());
    assert!(harness.manager.list_thread_ids().await.is_empty());
    assert!(control.take_mailbox(root_id).is_empty());
    let old_root = Arc::downgrade(&root);
    let old_child = Arc::downgrade(&child);
    drop((root, child, control));
    assert_thread_released(old_root).await;
    assert_thread_released(old_child).await;
    timeout(Duration::from_secs(5), async {
        while registry.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("finishing eviction cannot insert the shutdown tree back into the root cache");
}

#[tokio::test]
async fn root_eviction_retains_tree_while_first_child_is_still_starting() {
    let gate = Arc::new(RootResumeGate::default());
    let harness = RootResidencyHarness::new(Some(Arc::clone(&gate))).await;
    let root = harness.start_root().await;
    let root_id = root.session.thread_id;
    let control = root
        .session
        .services
        .local_agent_runtime
        .control(root.session.session_id());
    let registry = Arc::downgrade(&control.runtime.registry);
    let request = registered_child_request(&harness.config, &root);
    gate.block_child_start.store(true, Ordering::SeqCst);
    let starting_child = tokio::spawn({
        let control = control.clone();
        async move { control.spawn(request).await }
    });
    timeout(Duration::from_secs(5), gate.child_start_entered.notified())
        .await
        .expect("first child holds an admitted reservation before registration");
    assert_eq!(harness.manager.list_thread_ids().await, vec![root_id]);
    assert!(!root.session.input_queue.has_pending_mailbox_items().await);
    harness.evict(&root).await;
    let old_root = Arc::downgrade(&root);
    drop(root);
    assert_thread_released(old_root).await;
    assert!(harness.manager.list_thread_ids().await.is_empty());

    // There is neither unread mail nor a published child to recover this tree from.
    // The pending spawn reservation must be enough to retain it during root eviction.
    let resumed = harness.resume(root_id).await;
    assert!(Arc::ptr_eq(
        &registry
            .upgrade()
            .expect("pending child still owns its original tree"),
        &resumed.session.services.local_agent_runtime.registry,
    ));
    gate.child_start_release.notify_one();
    let (child, _) = timeout(Duration::from_secs(5), starting_child)
        .await
        .expect("pending child completes after root resume")
        .expect("child startup task completes")
        .expect("pending child is still admitted to the resumed root's tree");
    let child = harness.manager.get_thread(child.thread_id).await.unwrap();
    persist_idle_thread(&child, "child history after pending startup").await;
    assert!(Arc::ptr_eq(
        &resumed.session.services.local_agent_runtime.registry,
        &child.session.services.local_agent_runtime.registry,
    ));
    let saved = EvictedRoot {
        root_id,
        child_id: child.session.thread_id,
        registry,
    };
    assert_preserved_root(&saved, &resumed).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn evicted_root_resume_preserves_tree_and_mail_after_all_children_unload() {
    let harness = RootResidencyHarness::new(None).await;
    let saved = harness.cache_tree().await;

    let resumed = harness.resume(saved.root_id).await;
    assert_preserved_root(&saved, &resumed).await;
    assert_root_mail(&resumed, &[ROOT_MAIL]).await;
    assert!(resumed.session.active_turn.lock().await.is_none());

    timeout(
        Duration::from_secs(5),
        harness
            .manager
            .ensure_multi_agent_v2_child_loaded(saved.child_id),
    )
    .await
    .expect("child reload completes")
    .expect("normal root resume retains the child's owner and reload path");
    let child = harness
        .manager
        .get_thread(saved.child_id)
        .await
        .expect("retained child reloads with the same ID");
    assert!(Arc::ptr_eq(
        &resumed.session.services.local_agent_runtime.registry,
        &child.session.services.local_agent_runtime.registry,
    ));
    harness.shutdown().await;
}

#[test_case(false; "idle_child")]
#[test_case(true; "running_child")]
#[tokio::test]
async fn sender_reloads_root_without_consuming_child_residency(child_running: bool) {
    let harness = RootResidencyHarness::new(None).await;
    let (root, child, control) = harness.start_tree().await;
    let root_id = root.session.thread_id;
    let child_id = child.session.thread_id;
    if child_running {
        let turn = child.session.new_default_turn().await;
        child
            .session
            .send_event(
                &turn,
                EventMsg::TurnStarted(TurnStartedEvent {
                    turn_attribution: None,
                    turn_id: turn.sub_id.clone(),
                    root_turn_id: None,
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: ModeKind::Default,
                }),
            )
            .await;
        assert_eq!(
            child.agent_status().await,
            crate::agent::AgentStatus::Running
        );
    }

    // Reproduce the ordinary root unload route: its live child retains the tree,
    // but there is no local-eviction marker that would queue mail without loading.
    root.shutdown_and_wait().await.expect("stop idle root");
    assert!(
        harness
            .manager
            .remove_thread_if_matches(&root_id, &root)
            .await
            .is_some()
    );
    let old_root = Arc::downgrade(&root);
    drop(root);
    assert_thread_released(old_root).await;

    harness
        .send_root_mail(&control, child_id, root_id, ROOT_MAIL)
        .await;
    let reloaded = harness
        .manager
        .get_thread(root_id)
        .await
        .expect("queue-only child send reloads the persisted root");
    assert!(!reloaded.session_source.is_non_root_agent());
    assert!(Arc::ptr_eq(
        &harness
            .manager
            .get_thread(child_id)
            .await
            .expect("keep child"),
        &child,
    ));
    assert!(Arc::ptr_eq(
        &control.runtime.registry,
        &reloaded.session.services.local_agent_runtime.registry,
    ));
    assert_root_mail(&reloaded, &[ROOT_MAIL]).await;
    assert!(reloaded.session.active_turn.lock().await.is_none());
    harness.shutdown().await;
}

#[tokio::test]
async fn root_eviction_waits_for_sender_residency_pin() {
    let harness = RootResidencyHarness::new(None).await;
    let (root, _child, control) = harness.start_tree().await;
    let pin = control
        .ensure_v2_agent_loaded(harness.config.clone(), root.session.thread_id, None)
        .await
        .expect("pin the sender's loaded root")
        .expect("roots need the same delivery protection as children");
    assert_eq!(
        harness
            .manager
            .try_evict_v2_thread(Arc::clone(&root))
            .await
            .unwrap(),
        super::ThreadEvictionOutcome::Busy,
    );
    assert!(Arc::ptr_eq(
        &harness
            .manager
            .get_thread(root.session.thread_id)
            .await
            .unwrap(),
        &root,
    ));
    drop(pin);
    harness.evict(&root).await;
    harness.shutdown().await;
}

#[derive(Clone, Copy, Debug)]
enum CachedRootRemoval {
    Client,
    Tree,
    Manager,
}

#[test_case(false; "loaded_root")]
#[test_case(true; "cached_root")]
#[tokio::test]
async fn removing_root_detaches_and_releases_its_loaded_child(root_cached: bool) {
    let harness = RootResidencyHarness::new(None).await;
    let (root, child, control) = harness.start_tree().await;
    let unrelated_root = harness.start_root().await;
    let root_id = root.session.thread_id;
    let child_id = child.session.thread_id;
    let old_root = Arc::downgrade(&root);
    let old_child = Arc::downgrade(&child);
    let registry = Arc::downgrade(&control.runtime.registry);
    let retained_root = if root_cached {
        harness.evict(&root).await;
        drop(root);
        assert_thread_released(old_root.clone()).await;
        None
    } else {
        Some(root)
    };
    assert!(Arc::ptr_eq(
        &harness
            .manager
            .get_thread(child_id)
            .await
            .expect("child remains loaded before removal"),
        &child,
    ));

    let removed = timeout(
        Duration::from_secs(5),
        harness.manager.remove_thread_for_client(&root_id),
    )
    .await
    .expect("client root removal completes")
    .expect("client can remove its root tree");
    assert_eq!(removed.is_some(), !root_cached);
    if let (Some(removed), Some(original)) = (&removed, &retained_root) {
        assert!(Arc::ptr_eq(removed, original));
    }
    assert!(harness.manager.get_thread(root_id).await.is_err());
    assert!(
        harness.manager.get_thread(child_id).await.is_err(),
        "closing the tree must not strand a stopped child in the loaded registry",
    );
    assert!(control.runtime.admit_start().is_err());
    assert!(Arc::ptr_eq(
        &harness
            .manager
            .get_thread(unrelated_root.session.thread_id)
            .await
            .unwrap(),
        &unrelated_root,
    ));
    assert!(
        unrelated_root
            .session
            .services
            .local_agent_runtime
            .admit_start()
            .is_ok()
    );

    drop((removed, retained_root, child, control));
    assert_thread_released(old_root).await;
    assert_thread_released(old_child).await;
    timeout(Duration::from_secs(5), async {
        while registry.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("root removal releases the exact tree without requiring manager shutdown");
    assert_eq!(
        harness.manager.list_thread_ids().await,
        vec![unrelated_root.session.thread_id]
    );
    harness.shutdown().await;
}

#[test_case(CachedRootRemoval::Client; "client_removal")]
#[test_case(CachedRootRemoval::Tree; "tree_shutdown")]
#[test_case(CachedRootRemoval::Manager; "manager_shutdown")]
#[tokio::test]
async fn explicit_removal_closes_unloaded_root_tree(removal: CachedRootRemoval) {
    let harness = RootResidencyHarness::new(None).await;
    let (root, child, stale_control) = harness.start_tree().await;
    let root_id = root.session.thread_id;
    let child_id = child.session.thread_id;
    let registry = Arc::downgrade(&stale_control.runtime.registry);
    harness
        .send_root_mail(&stale_control, child_id, root_id, ROOT_MAIL)
        .await;
    harness.evict(&root).await;
    harness.evict(&child).await;
    let old_root = Arc::downgrade(&root);
    let old_child = Arc::downgrade(&child);
    drop((root, child));
    assert_thread_released(old_root).await;
    assert_thread_released(old_child).await;
    assert!(harness.manager.list_thread_ids().await.is_empty());

    match removal {
        CachedRootRemoval::Client => {
            assert!(
                harness
                    .manager
                    .remove_thread_for_client(&root_id)
                    .await
                    .unwrap()
                    .is_none(),
                "client removal also retires the unloaded root's retained tree",
            );
        }
        CachedRootRemoval::Tree => {
            let shutdown = harness
                .manager
                .request_agent_tree_shutdown(root_id)
                .await
                .expect("unloaded roots still own a shutdown-capable tree");
            timeout(Duration::from_secs(5), shutdown.wait())
                .await
                .expect("cached tree shutdown completes")
                .expect("cached tree shutdown succeeds");
        }
        CachedRootRemoval::Manager => harness.shutdown().await,
    }
    assert!(stale_control.runtime.admit_start().is_err());
    assert!(stale_control.take_mailbox(root_id).is_empty());
    assert!(
        stale_control
            .send(root_mail_request(
                &harness.config,
                child_id,
                root_id,
                "stale send"
            ))
            .await
            .is_err(),
        "a stale controller cannot recreate an explicitly retired root",
    );
    assert!(harness.manager.list_thread_ids().await.is_empty());
    drop(stale_control);
    timeout(Duration::from_secs(5), async {
        while registry.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("explicit removal releases retained tree ownership");
    harness.shutdown().await;
}

#[tokio::test]
async fn failed_root_resume_preserves_cached_tree_and_mail() {
    let harness = RootResidencyHarness::new(None).await;
    let saved = harness.cache_tree().await;
    let result = harness
        .manager
        .start_thread(StartThreadOptions {
            initial_history: harness.history(saved.root_id).await,
            reserved_thread_id: Some(ThreadId::new()),
            ..StartThreadOptions::new(harness.config.clone())
        })
        .await;
    assert!(
        result.is_err(),
        "a resumed thread cannot use a different reserved ID"
    );
    assert!(harness.manager.list_thread_ids().await.is_empty());

    let resumed = harness.resume(saved.root_id).await;
    assert_preserved_root(&saved, &resumed).await;
    assert_root_mail(&resumed, &[ROOT_MAIL]).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn cancelled_unpublished_root_resume_preserves_cached_tree_and_mail() {
    let gate = Arc::new(RootResumeGate::default());
    let harness = RootResidencyHarness::new(Some(Arc::clone(&gate))).await;
    let saved = harness.cache_tree().await;
    gate.block_start.store(true, Ordering::SeqCst);
    let first = tokio::spawn(resume_root(
        Arc::clone(&harness.manager),
        harness.config.clone(),
        harness.history(saved.root_id).await,
    ));
    timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .expect("first resume reaches initialized thread-start lifecycle");
    assert!(harness.manager.get_thread(saved.root_id).await.is_err());

    let mut second = Box::pin(resume_root(
        Arc::clone(&harness.manager),
        harness.config.clone(),
        harness.history(saved.root_id).await,
    ));
    assert!(
        timeout(Duration::from_millis(50), &mut second)
            .await
            .is_err(),
        "another resume must wait for the unpublished root startup",
    );
    first.abort();
    assert!(matches!(first.await, Err(error) if error.is_cancelled()));
    let resumed = timeout(Duration::from_secs(5), second)
        .await
        .expect("retry waits for cancelled-start cleanup and then completes");
    assert_preserved_root(&saved, &resumed).await;
    assert_root_mail(&resumed, &[ROOT_MAIL]).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn cancelled_published_root_resume_keeps_runtime_and_accepted_mail() {
    let gate = Arc::new(RootResumeGate::default());
    let harness = RootResidencyHarness::new(Some(Arc::clone(&gate))).await;
    let saved = harness.cache_tree().await;
    gate.block_ready.store(true, Ordering::SeqCst);
    let first = tokio::spawn(resume_root(
        Arc::clone(&harness.manager),
        harness.config.clone(),
        harness.history(saved.root_id).await,
    ));
    timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .expect("first resume reaches the post-registration lifecycle");
    let published = harness.manager.get_thread(saved.root_id).await.unwrap();
    let control = published
        .session
        .services
        .local_agent_runtime
        .control(published.session.session_id());
    assert_eq!(
        control.runtime.registry.evicted_environments(saved.root_id),
        None,
        "publication clears the snapshot before lifecycle callbacks can be cancelled",
    );
    const ACCEPTED_MAIL: &str = "root-residency: accepted after publication";
    harness
        .send_root_mail(&control, saved.child_id, saved.root_id, ACCEPTED_MAIL)
        .await;

    let second = timeout(Duration::from_secs(5), harness.resume(saved.root_id))
        .await
        .expect("published root is available before its lifecycle callback returns");
    assert!(Arc::ptr_eq(&published, &second));
    first.abort();
    assert!(matches!(first.await, Err(error) if error.is_cancelled()));
    assert!(Arc::ptr_eq(
        &harness.manager.get_thread(saved.root_id).await.unwrap(),
        &published,
    ));
    assert_eq!(
        control.runtime.registry.evicted_environments(saved.root_id),
        None,
    );
    assert_preserved_root(&saved, &published).await;
    assert_root_mail(&published, &[ROOT_MAIL, ACCEPTED_MAIL]).await;
    assert!(control.runtime.admit_start().is_ok());
    harness.shutdown().await;
}

#[test_case(false; "client_resume")]
#[test_case(true; "sender_resume")]
#[tokio::test]
async fn cancelled_root_resume_preserves_environment_and_queue_only_delivery(sender_resume: bool) {
    let gate = Arc::new(RootResumeGate::default());
    let harness = RootResidencyHarness::new(Some(Arc::clone(&gate))).await;
    let root = harness
        .manager
        .start_thread(StartThreadOptions {
            history_mode: Some(ThreadHistoryMode::Paginated),
            thread_source: Some(ThreadSource::User),
            ..StartThreadOptions::new(harness.config.clone())
        })
        .await
        .expect("start root with its selected environment")
        .thread;
    persist_idle_thread(&root, "root history with an environment").await;
    let root_id = root.session.thread_id;
    let environments = root.environment_selections().await;
    assert!(!environments.is_empty());
    let control = root
        .session
        .services
        .local_agent_runtime
        .control(root.session.session_id());
    let (child, _) = control
        .spawn(registered_child_request(&harness.config, &root))
        .await
        .expect("spawn child before root eviction");
    let child_id = child.thread_id;
    let child = harness.manager.get_thread(child_id).await.unwrap();
    persist_idle_thread(&child, "child history").await;

    let child_cwd = harness.config.cwd.join("child-workspace");
    std::fs::create_dir(child_cwd.as_path()).expect("create a distinct child working directory");
    child
        .update_thread_settings(ThreadSettingsOverrides {
            environments: Some(TurnEnvironmentRequests::new(
                child_cwd.clone(),
                harness
                    .manager
                    .default_environment_requests(&child_cwd, std::slice::from_ref(&child_cwd)),
            )),
            ..Default::default()
        })
        .await
        .expect("give the child its own working directory");
    let child_turn = child
        .session
        .new_turn_with_default_settings("child-workspace".to_string(), Default::default())
        .await;
    let sender_config = build_agent_resume_config(&child_turn).expect("child resume settings");
    assert_eq!(sender_config.cwd, child_cwd);
    assert_ne!(sender_config.cwd, harness.config.cwd);

    harness
        .send_root_mail(&control, child_id, root_id, ROOT_MAIL)
        .await;
    harness.evict(&root).await;
    let old_root = Arc::downgrade(&root);
    drop(root);
    assert_thread_released(old_root).await;
    assert_eq!(
        control.runtime.registry.evicted_environments(root_id),
        Some(environments.clone()),
    );

    gate.block_start.store(true, Ordering::SeqCst);
    let resume = tokio::spawn(resume_root(
        Arc::clone(&harness.manager),
        harness.config.clone(),
        harness.history(root_id).await,
    ));
    timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .expect("resume restores metadata before its startup is cancelled");
    resume.abort();
    assert!(matches!(resume.await, Err(error) if error.is_cancelled()));
    let cleanup_barrier = timeout(
        Duration::from_secs(5),
        harness.state.root_runtimes.gate(root_id).lock_owned(),
    )
    .await
    .expect("cancelled startup releases the root gate after cleanup");
    drop(cleanup_barrier);
    assert_eq!(
        control.runtime.registry.evicted_environments(root_id),
        Some(environments.clone()),
        "unpublished resume must preserve the root's eviction snapshot",
    );

    const AFTER_CANCEL: &str = "root-residency: mail after cancelled reconnect";
    timeout(
        Duration::from_secs(5),
        control.send(root_mail_request(
            &sender_config,
            child_id,
            root_id,
            AFTER_CANCEL,
        )),
    )
    .await
    .expect("queue-only send finishes without reopening the root")
    .expect("accept child mail after cancelled reconnect");
    assert!(
        harness.manager.get_thread(root_id).await.is_err(),
        "queue-only mail must leave the evicted root unloaded",
    );
    assert_eq!(
        control.runtime.registry.evicted_environments(root_id),
        Some(environments.clone()),
    );

    let resumed = if sender_resume {
        let pin = control
            .ensure_v2_agent_loaded(sender_config, root_id, None)
            .await
            .expect("sender-driven resume uses the retained root environment");
        drop(pin);
        harness.manager.get_thread(root_id).await.unwrap()
    } else {
        harness.resume(root_id).await
    };
    assert_eq!(resumed.environment_selections().await, environments);
    assert_eq!(control.runtime.registry.evicted_environments(root_id), None);
    assert_root_mail(&resumed, &[ROOT_MAIL, AFTER_CANCEL]).await;
    assert!(resumed.session.active_turn.lock().await.is_none());
    assert!(Arc::ptr_eq(
        &harness.manager.get_thread(child_id).await.unwrap(),
        &child,
    ));
    harness.shutdown().await;
}

const ROOT_MAIL: &str = "root-residency: queued before unload";

struct RootResidencyHarness {
    _home: tempfile::TempDir,
    config: Config,
    manager: Arc<ThreadManager>,
    state: Arc<ThreadManagerState>,
}

struct EvictedRoot {
    root_id: ThreadId,
    child_id: ThreadId,
    registry: Weak<AgentRegistry>,
}

impl RootResidencyHarness {
    async fn new(gate: Option<Arc<RootResumeGate>>) -> Self {
        let home = tempfile::tempdir().expect("create root residency home");
        let mut config = ConfigBuilder::without_managed_config_for_tests()
            .codex_home(home.path().to_path_buf())
            .build()
            .await
            .expect("build persisted root config");
        config.cwd = home.path().to_path_buf().try_into().unwrap();
        config.model = Some("gpt-5.6-sol".to_string());
        config.model_catalog = Some(codex_models_manager::bundled_models_response().unwrap());
        config.model_provider.base_url = Some("http://127.0.0.1:1".to_string());
        config.multi_agent_v2.max_concurrent_threads_per_session = 2;
        let _ = config.features.enable(Feature::MultiAgentV2);
        let _ = config.features.enable(Feature::Sqlite);
        let _ = config.features.disable(Feature::WakeOnAgentMessages);
        let state_db = crate::init_state_db(&config).await;
        assert!(
            state_db.is_some(),
            "fixture requires persisted thread ownership"
        );
        let auth = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("dummy"));
        let mut extensions = codex_extension_api::ExtensionRegistryBuilder::new();
        if let Some(gate) = gate {
            extensions.thread_lifecycle_contributor(gate);
        }
        let manager = Arc::new(ThreadManager::new(
            &config,
            Arc::clone(&auth),
            crate::build_models_manager(&config, auth),
            crate::CodexAppsToolsCache::default(),
            SessionSource::Exec,
            Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
            Arc::new(extensions.build()),
            Arc::new(crate::test_support::EmptyUserInstructionsProvider),
            None,
            crate::thread_manager::passthrough_image_store(),
            crate::thread_store_from_config(&config, state_db.clone()),
            crate::local_agent_graph_store_from_state_db(state_db.as_ref()),
            "11111111-1111-4111-8111-111111111111".to_string(),
            None,
            None,
        ));
        let state = manager.agent_control().runtime.upgrade().unwrap();
        Self {
            _home: home,
            config,
            manager,
            state,
        }
    }

    async fn start_root(&self) -> Arc<CodexThread> {
        let root = self
            .manager
            .start_thread(StartThreadOptions {
                history_mode: Some(ThreadHistoryMode::Paginated),
                thread_source: Some(ThreadSource::User),
                environments: Some(Vec::new()),
                ..StartThreadOptions::new(self.config.clone())
            })
            .await
            .expect("start persisted root")
            .thread;
        persist_idle_thread(&root, "root history").await;
        root
    }

    async fn start_tree(&self) -> (Arc<CodexThread>, Arc<CodexThread>, LocalAgentControl) {
        let root = self.start_root().await;
        let control = root
            .session
            .services
            .local_agent_runtime
            .control(root.session.session_id());
        let (child, _) = control
            .spawn(registered_child_request(&self.config, &root))
            .await
            .expect("spawn registered child in the only child slot");
        let child = self.manager.get_thread(child.thread_id).await.unwrap();
        persist_idle_thread(&child, "child history").await;
        (root, child, control)
    }

    async fn cache_tree(&self) -> EvictedRoot {
        let (root, child, control) = self.start_tree().await;
        let saved = EvictedRoot {
            root_id: root.session.thread_id,
            child_id: child.session.thread_id,
            registry: Arc::downgrade(&control.runtime.registry),
        };
        self.send_root_mail(&control, saved.child_id, saved.root_id, ROOT_MAIL)
            .await;
        self.evict(&root).await;
        self.evict(&child).await;
        let old_root = Arc::downgrade(&root);
        let old_child = Arc::downgrade(&child);
        drop((root, child, control));
        assert_thread_released(old_root).await;
        assert_thread_released(old_child).await;
        assert!(self.manager.list_thread_ids().await.is_empty());
        assert!(
            saved.registry.upgrade().is_some(),
            "the manager retains the tree after every session and caller is released",
        );
        saved
    }

    async fn evict(&self, thread: &Arc<CodexThread>) {
        timeout(Duration::from_secs(5), async {
            loop {
                match self
                    .manager
                    .try_evict_v2_thread(Arc::clone(thread))
                    .await
                    .unwrap()
                {
                    super::ThreadEvictionOutcome::Evicted => break,
                    super::ThreadEvictionOutcome::Busy => tokio::task::yield_now().await,
                    super::ThreadEvictionOutcome::NotCurrent => panic!("fixture runtime changed"),
                }
            }
        })
        .await
        .expect("idle persisted thread unloads after accepted mail dispatch");
    }

    async fn send_root_mail(
        &self,
        control: &LocalAgentControl,
        child_id: ThreadId,
        root_id: ThreadId,
        message: &str,
    ) {
        let receipt = timeout(
            Duration::from_secs(5),
            control.send(root_mail_request(&self.config, child_id, root_id, message)),
        )
        .await
        .expect("root delivery does not wait on an unrelated residency transition")
        .expect("child message is accepted by its root");
        assert_eq!(receipt.thread_id, root_id);
    }

    async fn history(&self, thread_id: ThreadId) -> InitialHistory {
        let stored = self
            .state
            .read_stored_thread(ReadThreadParams {
                thread_id,
                include_archived: true,
                include_history: false,
            })
            .await
            .expect("read persisted root metadata");
        let context = self
            .state
            .load_latest_model_context(LoadThreadHistoryParams {
                thread_id,
                include_archived: true,
            })
            .await
            .expect("load persisted root context");
        InitialHistory::Resumed(ResumedHistory {
            history_revision: context.revision,
            conversation_id: thread_id,
            history: Arc::new(context.items),
            rollout_path: stored.rollout_path,
        })
    }

    async fn resume(&self, thread_id: ThreadId) -> Arc<CodexThread> {
        resume_root(
            Arc::clone(&self.manager),
            self.config.clone(),
            self.history(thread_id).await,
        )
        .await
    }

    async fn shutdown(&self) {
        let report = self
            .manager
            .shutdown_all_threads_bounded(Duration::from_secs(5))
            .await;
        assert!(report.submit_failed.is_empty());
        assert!(report.timed_out.is_empty());
    }
}

fn registered_child_request(config: &Config, root: &CodexThread) -> SpawnRequest {
    let root_id = root.session.thread_id;
    let source = thread_spawn_source(
        root_id,
        &root.session_source,
        next_thread_spawn_depth(&root.session_source),
        None,
        Some("worker".to_string()),
    )
    .expect("registered child source");
    SpawnRequest {
        caller: root_id,
        config: config.clone(),
        input: AgentInput::UserInput(vec![UserInput::Text {
            text: "child task".to_string(),
            text_elements: Vec::new(),
        }]),
        source,
        options: SpawnAgentOptions {
            parent_thread_id: Some(root_id),
            ..Default::default()
        },
    }
}

fn root_mail_request(
    config: &Config,
    child_id: ThreadId,
    root_id: ThreadId,
    message: &str,
) -> SendRequest {
    SendRequest {
        caller: child_id,
        target: AgentTarget::Id(root_id),
        resume_config: config.clone(),
        input: AgentInput::Message {
            message: AgentMessage::Plaintext(message.to_string()),
            mode: MessageDeliveryMode::QueueOnly,
        },
        start_options: Default::default(),
    }
}

async fn resume_root(
    manager: Arc<ThreadManager>,
    config: Config,
    history: InitialHistory,
) -> Arc<CodexThread> {
    timeout(
        Duration::from_secs(5),
        manager.resume_thread_with_history(
            config,
            history,
            manager.auth_manager(),
            None,
            ClientMcpExtensions::default(),
        ),
    )
    .await
    .expect("normal root resume completes")
    .expect("normal root resume succeeds")
    .thread
}

async fn persist_idle_thread(thread: &Arc<CodexThread>, marker: &str) {
    thread
        .session
        .abort_all_tasks(TurnAbortReason::Interrupted)
        .await;
    thread
        .inject_response_items(vec![ResponseItem::Message {
            status: None,
            encrypted_content: None,
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputText {
                text: marker.to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        }])
        .await
        .expect("record durable fixture context");
    mark_thread_completed(thread).await;
    thread
        .session
        .ensure_rollout_materialized(PersistContext::Standard)
        .await;
    thread
        .session
        .flush_rollout()
        .await
        .expect("flush completed fixture context");
}

async fn assert_thread_released(thread: Weak<CodexThread>) {
    timeout(Duration::from_secs(5), async {
        while thread.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("unloaded session is released, not hidden behind a retained thread handle");
}

async fn assert_preserved_root(saved: &EvictedRoot, root: &Arc<CodexThread>) {
    assert_eq!(root.session.thread_id, saved.root_id);
    assert!(!root.session_source.is_non_root_agent());
    assert!(Arc::ptr_eq(
        &saved
            .registry
            .upgrade()
            .expect("original tree remains retained"),
        &root.session.services.local_agent_runtime.registry,
    ));
    let child_id = root
        .session
        .services
        .agent_control
        .resolve(saved.root_id, None, &root.session_source, "/root/worker")
        .await
        .expect("normal resume keeps canonical child addressing");
    assert_eq!(child_id, saved.child_id);
}

async fn assert_root_mail(root: &Arc<CodexThread>, expected: &[&str]) {
    let mut messages = Vec::new();
    timeout(Duration::from_secs(5), async {
        loop {
            let (items, _) = root.session.input_queue.drain_mailbox_input_items().await;
            messages.extend(items.into_iter().filter_map(|item| match item {
                TurnInput::InterAgentCommunication(mail)
                    if mail.content.contains("root-residency:") =>
                {
                    Some(mail.content)
                }
                _ => None,
            }));
            if expected
                .iter()
                .all(|message| messages.iter().any(|mail| mail.contains(*message)))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("retained mail reaches the resumed session's next-turn input queue");
    // All accepted submissions have released their pins before the final drain.
    let _delivery_barrier = timeout(
        Duration::from_secs(5),
        Arc::clone(&root.residency_gate).write_owned(),
    )
    .await
    .expect("all accepted message submissions release their residency pins");
    let (remaining, _) = root.session.input_queue.drain_mailbox_input_items().await;
    messages.extend(remaining.into_iter().filter_map(|item| match item {
        TurnInput::InterAgentCommunication(mail) if mail.content.contains("root-residency:") => {
            Some(mail.content)
        }
        _ => None,
    }));
    assert_eq!(
        messages.len(),
        expected.len(),
        "mail is not duplicated across residency transitions"
    );
    for message in expected {
        assert_eq!(
            messages
                .iter()
                .filter(|mail| mail.contains(*message))
                .count(),
            1
        );
    }
}

#[derive(Default)]
struct RootResumeGate {
    block_start: AtomicBool,
    block_ready: AtomicBool,
    block_stop: AtomicBool,
    block_secondary_stop: AtomicBool,
    block_child_start: AtomicBool,
    entered: Notify,
    stop_entered: Notify,
    stop_release: Notify,
    secondary_stop_entered: Notify,
    secondary_stop_release: Notify,
    child_start_entered: Notify,
    child_start_release: Notify,
}

impl codex_extension_api::ThreadLifecycleContributor<Config> for RootResumeGate {
    fn on_thread_start<'a>(
        &'a self,
        input: codex_extension_api::ThreadStartInput<'a, Config>,
    ) -> codex_extension_api::ExtensionFuture<'a, ()> {
        Box::pin(async move {
            if !input.session_source.is_non_root_agent()
                && self.block_start.swap(false, Ordering::SeqCst)
            {
                self.entered.notify_one();
                std::future::pending::<()>().await;
            }
            if input.session_source.is_non_root_agent()
                && self.block_child_start.swap(false, Ordering::SeqCst)
            {
                self.child_start_entered.notify_one();
                self.child_start_release.notified().await;
            }
        })
    }

    fn on_thread_ready<'a>(
        &'a self,
        input: codex_extension_api::ThreadReadyInput<'a, Config>,
    ) -> codex_extension_api::ExtensionFuture<'a, ()> {
        Box::pin(async move {
            if !input.session_source.is_non_root_agent()
                && self.block_ready.swap(false, Ordering::SeqCst)
            {
                self.entered.notify_one();
                std::future::pending::<()>().await;
            }
        })
    }

    fn on_thread_stop<'a>(
        &'a self,
        _input: codex_extension_api::ThreadStopInput<'a>,
    ) -> codex_extension_api::ExtensionFuture<'a, ()> {
        Box::pin(async move {
            if self.block_stop.swap(false, Ordering::SeqCst) {
                self.stop_entered.notify_one();
                self.stop_release.notified().await;
            } else if self.block_secondary_stop.swap(false, Ordering::SeqCst) {
                self.secondary_stop_entered.notify_one();
                self.secondary_stop_release.notified().await;
            }
        })
    }
}

async fn spawn_v2_subagent(
    control: &LocalAgentControl,
    state: &Arc<ThreadManagerState>,
    config: Config,
    parent_thread_id: ThreadId,
    label: &str,
) -> crate::thread_manager::NewThread {
    state
        .spawn_child_thread(
            crate::thread_manager::StartThreadOptions {
                session_source: Some(SessionSource::SubAgent(SubAgentSource::Other(
                    label.to_string(),
                ))),
                thread_source: Some(ThreadSource::Subagent),
                ..crate::thread_manager::StartThreadOptions::new(config)
            },
            control.clone(),
            Some(parent_thread_id),
            /*inherited_exec_policy*/ None,
        )
        .await
        .expect("spawn v2 subagent")
}

async fn mark_thread_completed(thread: &CodexThread) {
    let turn = thread.session.new_default_turn().await;
    thread
        .session
        .send_event(
            turn.as_ref(),
            EventMsg::TurnComplete(TurnCompleteEvent {
                root_turn_id: None,
                turn_id: turn.sub_id.clone(),
                started_at: None,
                last_agent_message: Some("done".to_string()),
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            }),
        )
        .await;
    clear_active_turn(thread).await;
}

async fn mark_thread_interrupted(thread: &CodexThread) {
    let turn = thread.session.new_default_turn().await;
    thread
        .session
        .send_event(
            turn.as_ref(),
            EventMsg::TurnAborted(TurnAbortedEvent {
                root_turn_id: None,
                turn_id: Some(turn.sub_id.clone()),
                started_at: None,
                reason: TurnAbortReason::Interrupted,
                error: None,
                completed_at: None,
                duration_ms: None,
            }),
        )
        .await;
    clear_active_turn(thread).await;
}

async fn clear_active_turn(thread: &CodexThread) {
    // The fixture has no task runner to clear the turn after the terminal event.
    *thread.session.active_turn.lock().await = None;
}
