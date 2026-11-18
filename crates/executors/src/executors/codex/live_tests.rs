//! Live smoke test against the system `codex` binary. Needs a logged-in Codex CLI and spends
//! tokens, so it is ignored by default:
//! `cargo test -p executors codex_live -- --ignored --nocapture`

use std::{path::Path, sync::Arc, time::Duration};

use tokio::io::{AsyncBufReadExt, BufReader};
use workspace_utils::{log_msg::LogMsg, msg_store::MsgStore};

use super::{Codex, normalize_logs::CodexLogProcessor};
use crate::{
    env::{ExecutionEnv, RepoContext},
    executors::{SpawnedChild, StandardCodingAgentExecutor},
    logs::{NormalizedEntryType, utils::EntryIndexProvider},
};

const TURN_TIMEOUT: Duration = Duration::from_secs(240);

/// Collect stdout until the executor signals exit; returns (session ids, assistant messages).
async fn drain(mut spawned: SpawnedChild, cwd: &Path) -> (Vec<String>, Vec<String>) {
    let stdout = spawned.child.inner().stdout.take().expect("stdout");
    let exit = spawned.exit_signal.take().expect("exit signal");
    let store = Arc::new(MsgStore::new());
    let mut processor = CodexLogProcessor::new(
        store.clone(),
        EntryIndexProvider::test_new(),
        cwd.to_string_lossy().to_string(),
    );
    let mut lines = BufReader::new(stdout).lines();
    let read = async {
        while let Ok(Some(line)) = lines.next_line().await {
            println!("{}", line.chars().take(200).collect::<String>());
            processor.process_line(&line);
        }
    };
    tokio::select! {
        _ = read => {},
        _ = exit => {},
        _ = tokio::time::sleep(TURN_TIMEOUT) => panic!("codex turn timed out"),
    }
    let _ = spawned.child.kill().await;

    let mut sessions = Vec::new();
    let mut messages = Vec::new();
    for msg in store.get_history() {
        match msg {
            LogMsg::SessionId(id) => sessions.push(id),
            LogMsg::JsonPatch(patch) => {
                if let Some((_, entry)) =
                    crate::logs::utils::patch::extract_normalized_entry_from_patch(&patch)
                    && matches!(entry.entry_type, NormalizedEntryType::AssistantMessage)
                {
                    messages.push(entry.content);
                }
            }
            _ => {}
        }
    }
    (sessions, messages)
}

#[tokio::test]
#[ignore = "spawns the real codex CLI and spends tokens"]
async fn codex_live_turn_and_follow_up() {
    let cwd = std::env::temp_dir().join("vk-codex-live");
    std::fs::create_dir_all(&cwd).unwrap();
    let codex: Codex = serde_json::from_value(serde_json::json!({
        "model": "gpt-6-luna",
        "sandbox": "danger-full-access",
    }))
    .unwrap();
    let env = ExecutionEnv::new(RepoContext::new(cwd.clone(), vec![]), false);

    let first = codex
        .spawn(
            &cwd,
            "Reply with exactly the word PING and nothing else.",
            &env,
        )
        .await
        .unwrap();
    let (sessions, messages) = drain(first, &cwd).await;
    assert_eq!(sessions.len(), 1, "one thread id expected");
    assert!(messages.iter().any(|m| m.contains("PING")), "{messages:?}");

    let follow_up = codex
        .spawn_follow_up(
            &cwd,
            "What word did you reply with? Answer with that word only.",
            &sessions[0],
            None,
            &env,
        )
        .await
        .unwrap();
    let (fork_sessions, fork_messages) = drain(follow_up, &cwd).await;
    assert_eq!(fork_sessions.len(), 1);
    assert_ne!(
        fork_sessions[0], sessions[0],
        "follow-up must run on a fork"
    );
    assert!(
        fork_messages.iter().any(|m| m.contains("PING")),
        "{fork_messages:?}"
    );
}
