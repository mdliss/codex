//! Keeps tree identity and unread mail while a local V2 root session is unloaded.
//! Entries never retain a session; same-ID resumes and eviction share a weakly held gate.

use crate::agent::control::LocalAgentRuntime;
use codex_protocol::ThreadId;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::Weak;

#[derive(Default)]
pub(crate) struct RootRuntimes {
    state: Mutex<RootRuntimeState>,
}

#[derive(Default)]
struct RootRuntimeState {
    retained: HashMap<ThreadId, LocalAgentRuntime>,
    gates: HashMap<ThreadId, Weak<tokio::sync::Mutex<()>>>,
}

impl RootRuntimes {
    pub(crate) fn gate(&self, thread_id: ThreadId) -> Arc<tokio::sync::Mutex<()>> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        // Never replace a gate while a resume, eviction, or cancelled-start cleanup owns it.
        state.gates.retain(|_, gate| gate.strong_count() != 0);
        if let Some(gate) = state.gates.get(&thread_id).and_then(Weak::upgrade) {
            return gate;
        }
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        state.gates.insert(thread_id, Arc::downgrade(&gate));
        gate
    }

    pub(crate) fn get(&self, thread_id: ThreadId) -> Option<LocalAgentRuntime> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state
            .retained
            .get(&thread_id)
            .is_some_and(|runtime| runtime.shutdown.is_cancelled())
        {
            state.retained.remove(&thread_id);
        }
        // Startup can fail or be cancelled. Publication, not lookup, releases this owner.
        state.retained.get(&thread_id).cloned()
    }

    /// Called under the thread registry lock after the stopped session hands back its mail.
    pub(crate) fn retain(&self, thread_id: ThreadId, runtime: &LocalAgentRuntime) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if !runtime.shutdown.is_cancelled() && runtime.has_unloaded_state() {
            state.retained.insert(thread_id, runtime.clone());
        }
    }

    pub(crate) fn remove_if_matches(&self, thread_id: ThreadId, runtime: &LocalAgentRuntime) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state
            .retained
            .get(&thread_id)
            .is_some_and(|retained| retained.same_tree(runtime))
        {
            state.retained.remove(&thread_id);
        }
    }

    pub(crate) fn remove_tree(&self, runtime: &LocalAgentRuntime) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retained
            .retain(|_, retained| !retained.same_tree(runtime));
    }

    pub(crate) fn shutdown_all(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        for (_, runtime) in state.retained.drain() {
            runtime.request_shutdown();
        }
    }
}
