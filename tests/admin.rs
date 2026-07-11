//! Hermetic end-to-end test of the admin control socket (issue #38).
//!
//! Binds a real [`serve_admin`] on a tempdir Unix socket whose one slot points
//! at an in-process [`MockOpencode`], then drives the real [`send_request`]
//! client and asserts the live readiness probe reports the slot **connected** —
//! exercising the whole CLI ↔ daemon channel (transport + `Status` handler +
//! reqwest probe) with no network and no real opencode.
//!
//! `connected == false` (down slots), the `0600` permission enforcement, and the
//! stale-socket replacement are covered by the unit tests in `src/admin.rs`.

// Pull in only the opencode mock (not mock_telegram) so this crate has no unused
// support code; `#[allow(dead_code)]` covers the mock's unexercised helpers.
#[path = "support/mock_opencode.rs"]
#[allow(dead_code)]
mod mock_opencode;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use telegram_opencode_proxy::admin::{self, AdminRequest, AdminResponse, AdminState, SlotInfo};

use mock_opencode::MockOpencode;

/// A minimal [`AdminState`] with a fixed slot list — stands in for `AppState`.
struct FakeState {
    slots: Vec<SlotInfo>,
}

impl AdminState for FakeState {
    fn slots(&self) -> Vec<SlotInfo> {
        self.slots.clone()
    }
}

/// Retry `send_request` briefly so the test doesn't race the server bind.
async fn status_request(socket: &Path) -> AdminResponse {
    for _ in 0..50 {
        match admin::send_request(socket, &AdminRequest::Status).await {
            Ok(resp) => return resp,
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
    panic!("admin socket never became reachable");
}

#[tokio::test]
async fn status_reports_a_live_slot_as_connected() {
    let oc = MockOpencode::start().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let socket = dir.path().join("admin.sock");

    let state: Arc<dyn AdminState> = Arc::new(FakeState {
        slots: vec![SlotInfo {
            name: "you".to_string(),
            opencode_url: oc.url.clone(),
        }],
    });
    let server = tokio::spawn(admin::serve_admin(Arc::clone(&state), socket.clone()));

    match status_request(&socket).await {
        AdminResponse::Status { slots } => {
            assert_eq!(slots.len(), 1);
            assert_eq!(slots[0].name, "you");
            assert_eq!(slots[0].opencode_url, oc.url);
            assert!(
                slots[0].connected,
                "a slot pointing at a live opencode must report connected"
            );
        }
        other => panic!("expected Status, got {other:?}"),
    }

    server.abort();
}
