//! Session lifecycle: get-or-create (opencode `404` → recreate) and the
//! deliberate `deny` permission posture installed on create.
//!
//! On create we PATCH a `deny` ruleset for the configured bash patterns
//! (`git commit*` / `git push*`). It is `deny`, **not** `ask`, on purpose: no
//! interactive responder exists until #13, so gating with `ask` would wedge the
//! turn. #13 flips these to `ask`. See `architecture.md` §2.6. Issues #5/#13.

// `get_or_create` is invoked by the per-user turn loop (#6); `deny_rules` is
// exercised by the unit tests below.
#![allow(dead_code)]

use anyhow::Result;

use crate::config::Model;
use crate::opencode::client::OpencodeClient;
use crate::opencode::types::{CreateModel, PermissionAction, PermissionRule};

/// Build the deliberate `deny` ruleset for the given bash command `patterns`.
/// Each pattern becomes `{ permission: "bash", pattern, action: deny }`.
pub fn deny_rules(patterns: &[String]) -> Vec<PermissionRule> {
    patterns
        .iter()
        .map(|pattern| PermissionRule {
            permission: "bash".to_string(),
            pattern: pattern.clone(),
            action: PermissionAction::Deny,
        })
        .collect()
}

/// Resolve a live session id for a user.
///
/// A `stored` id that opencode no longer recognises (HTTP `404` — e.g. a wiped
/// opencode DB) is transparently recreated, so a lost server-side DB never
/// bricks a user. On create, the `deny` posture for `deny_patterns` is PATCHed
/// onto the new session before it is returned.
pub async fn get_or_create(
    client: &OpencodeClient,
    stored: Option<&str>,
    model: &Model,
    deny_patterns: &[String],
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

    let rules = deny_rules(deny_patterns);
    if !rules.is_empty() {
        client.patch_permission(&created.id, rules).await?;
    }
    Ok(created.id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deny_rules_maps_patterns_to_bash_deny() {
        let patterns = vec!["git commit*".to_string(), "git push*".to_string()];
        let rules = deny_rules(&patterns);
        assert_eq!(rules.len(), 2);
        for (rule, pattern) in rules.iter().zip(&patterns) {
            assert_eq!(rule.permission, "bash");
            assert_eq!(&rule.pattern, pattern);
            assert_eq!(rule.action, PermissionAction::Deny);
        }
    }

    #[test]
    fn deny_rules_empty_when_no_patterns() {
        assert!(deny_rules(&[]).is_empty());
    }
}
