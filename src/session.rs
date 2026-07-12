//! Session lifecycle: get-or-create (opencode `404` → recreate) and the `ask`
//! permission posture installed on create.
//!
//! On create we PATCH an `ask` ruleset for the configured bash patterns
//! (`git commit*` / `git push*`, from `[permissions].ask`). opencode then fires
//! `permission.asked` when the agent hits one, and the permission relay (#13)
//! surfaces it as Telegram buttons. (Before #13 this was `deny`, since no
//! responder existed.) See `architecture.md` §2.6. Issues #5/#13.

use anyhow::Result;

use crate::config::Model;
use crate::opencode::client::OpencodeClient;
use crate::opencode::types::{CreateModel, PermissionAction, PermissionRule};

/// Build the `ask` ruleset for the given bash command `patterns` (#13). Each
/// pattern becomes `{ permission: "bash", pattern, action: ask }`, so opencode
/// gates it interactively rather than blocking or auto-allowing.
pub fn ask_rules(patterns: &[String]) -> Vec<PermissionRule> {
    patterns
        .iter()
        .map(|pattern| PermissionRule {
            permission: "bash".to_string(),
            pattern: pattern.clone(),
            action: PermissionAction::Ask,
        })
        .collect()
}

/// Resolve a live session id for a user.
///
/// A `stored` id that opencode no longer recognises (HTTP `404` — e.g. a wiped
/// opencode DB) is transparently recreated, so a lost server-side DB never
/// bricks a user. On create, the `ask` posture for `ask_patterns` is PATCHed
/// onto the new session before it is returned (#13).
pub async fn get_or_create(
    client: &OpencodeClient,
    stored: Option<&str>,
    model: &Model,
    ask_patterns: &[String],
) -> Result<String> {
    if let Some(id) = stored {
        if client.session_exists(id).await? {
            return Ok(id.to_string());
        }
        tracing::info!(
            session_id = id,
            "stored session unknown to opencode (404) — recreating"
        );
    }

    let created = client
        .create_session(None, Some(CreateModel::from(model)))
        .await?;

    let rules = ask_rules(ask_patterns);
    if !rules.is_empty() {
        client.patch_permission(&created.id, rules).await?;
    }
    Ok(created.id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ask_rules_maps_patterns_to_bash_ask() {
        let patterns = vec!["git commit*".to_string(), "git push*".to_string()];
        let rules = ask_rules(&patterns);
        assert_eq!(rules.len(), 2);
        for (rule, pattern) in rules.iter().zip(&patterns) {
            assert_eq!(rule.permission, "bash");
            assert_eq!(&rule.pattern, pattern);
            assert_eq!(rule.action, PermissionAction::Ask);
        }
    }

    #[test]
    fn ask_rules_empty_when_no_patterns() {
        assert!(ask_rules(&[]).is_empty());
    }
}
