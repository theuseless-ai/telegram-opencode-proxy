//! Integration coverage for the opencode event relay (issue #7).
//!
//! Drives the REAL [`Subscription`] / [`backfill`] against the in-process
//! `mock_opencode`, over an actual HTTP + SSE round-trip (no network, no
//! opencode). Proves the three properties the unit tests can't reach without a
//! live wire: typed events flow off `/global/event`, a dropped connection
//! **reconnects** transparently, and the [`SeenParts`] dedup + `backfill`
//! reconcile parts across that reconnect.

// Only the opencode mock is needed here; `allow(dead_code)` covers its helpers
// that this crate doesn't exercise.
#[path = "support/mock_opencode.rs"]
#[allow(dead_code)]
mod mock_opencode;

use std::time::Duration;

use telegram_opencode_proxy::opencode::client::OpencodeClient;
use telegram_opencode_proxy::opencode::events::{Event, SeenParts, Subscription, backfill};

use mock_opencode::MockOpencode;

/// The A0-validated `/global/event` capture — real frames (deltas, tool
/// lifecycle, a permission gate, sync mirrors) replayed by the mock.
const GATED_GLOBAL: &str = include_str!("../fixtures/opencode/events/gated-global.sse");

/// A short, deterministic reconnect delay for the `EventSource` retry policy.
const RETRY: Duration = Duration::from_millis(50);

/// Drive `recv` until `done(collected)` holds or the overall budget elapses,
/// so a hung stream fails fast instead of blocking the suite.
async fn collect_until<F>(sub: &mut Subscription, mut done: F) -> Vec<Event>
where
    F: FnMut(&[Event]) -> bool,
{
    let mut out = Vec::new();
    let deadline = Duration::from_secs(10);
    let collected = tokio::time::timeout(deadline, async {
        while let Some(event) = sub.recv().await {
            out.push(event);
            if done(&out) {
                break;
            }
        }
        out
    })
    .await;
    collected.expect("event collection timed out — stream hung")
}

#[tokio::test]
async fn subscription_surfaces_typed_events_over_the_wire() {
    let mock = MockOpencode::start().await;
    mock.set_event_stream(GATED_GLOBAL);

    let http = reqwest::Client::new();
    let mut sub = Subscription::connect(&http, &mock.url, RETRY).expect("connect subscription");

    // Collect one connection's worth: stop once the permission gate arrives (the
    // last surfaced event of interest in the fixture).
    let events = collect_until(&mut sub, |evs| {
        evs.iter().any(|e| matches!(e, Event::Permission(_)))
    })
    .await;

    assert!(
        events.first() == Some(&Event::Connected),
        "first surfaced event is server.connected"
    );
    assert!(
        events.iter().any(|e| matches!(e, Event::Delta(_))),
        "streaming text deltas surface"
    );
    assert!(
        events.iter().any(|e| matches!(e, Event::PartUpdated(_))),
        "part lifecycle updates surface"
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::Status {
                status: telegram_opencode_proxy::opencode::events::SessionStatus::Busy,
                ..
            }
        )),
        "a busy session.status surfaces"
    );
    let gate = events
        .iter()
        .find_map(|e| match e {
            Event::Permission(p) => Some(p),
            _ => None,
        })
        .expect("permission gate surfaces");
    assert_eq!(gate.permission, "bash");
    assert_eq!(gate.command.as_deref(), Some("echo hi"));

    // No mirror frame ever leaks through as an event.
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::Other { kind } if kind == "sync" || kind.ends_with(".1"))),
        "sync/mirror frames stay dropped over the wire"
    );
}

#[tokio::test]
async fn subscription_reconnects_and_dedup_suppresses_replayed_parts() {
    let mock = MockOpencode::start().await;
    // The mock serves the whole body then closes, so the Constant-retry
    // EventSource re-subscribes and replays it — a real reconnect.
    mock.set_event_stream(GATED_GLOBAL);

    let http = reqwest::Client::new();
    let mut sub = Subscription::connect(&http, &mock.url, RETRY).expect("connect subscription");

    // Run until we've seen `server.connected` twice — i.e. across a reconnect.
    let events = collect_until(&mut sub, |evs| {
        evs.iter().filter(|e| matches!(e, Event::Connected)).count() >= 2
    })
    .await;

    let connects = events
        .iter()
        .filter(|e| matches!(e, Event::Connected))
        .count();
    assert!(
        connects >= 2,
        "expected a reconnect (>=2 Connected), got {connects}"
    );
    assert!(
        mock.event_connections() >= 2,
        "mock saw >=2 /global/event connections"
    );

    // Every delta part id was replayed on the second connection; SeenParts dedup
    // must treat the replay as already-rendered.
    let mut seen = SeenParts::new();
    let mut newly = 0usize;
    let mut distinct = std::collections::HashSet::new();
    for event in &events {
        if let Event::Delta(d) = event {
            distinct.insert(d.part_id.clone());
            if seen.mark(&d.part_id) {
                newly += 1;
            }
        }
    }
    assert!(!distinct.is_empty(), "deltas arrived");
    assert_eq!(
        newly,
        distinct.len(),
        "each distinct part id is newly-seen exactly once; replays are deduped"
    );
    assert!(
        newly
            < events
                .iter()
                .filter(|e| matches!(e, Event::Delta(_)))
                .count(),
        "the reconnect replayed deltas (more delta events than distinct parts)"
    );
}

#[tokio::test]
async fn backfill_returns_unseen_assistant_text_and_dedups() {
    let mock = MockOpencode::start().await;
    // A message list: a user turn plus an assistant turn with step/reasoning and
    // two visible text parts. Backfill must return only the assistant text.
    mock.set_message_list(
        r#"[
          {"info":{"id":"msg_u","sessionID":"ses_1","role":"user"},
           "parts":[{"type":"text","id":"prt_u","text":"hi"}]},
          {"info":{"id":"msg_a","sessionID":"ses_1","role":"assistant","finish":"stop"},
           "parts":[
             {"type":"step-start","id":"prt_s","sessionID":"ses_1","messageID":"msg_a"},
             {"type":"reasoning","id":"prt_r","text":"thinking","sessionID":"ses_1","messageID":"msg_a"},
             {"type":"text","id":"prt_t1","text":"Hello ","sessionID":"ses_1","messageID":"msg_a"},
             {"type":"text","id":"prt_t2","text":"world","sessionID":"ses_1","messageID":"msg_a"}
           ]}
        ]"#,
    );

    let client = OpencodeClient::new(&mock.url).expect("client");

    // Fresh dedup set → both assistant text parts recovered, user/reasoning/step
    // skipped, in wire order.
    let mut seen = SeenParts::new();
    let missed = backfill(&client, "ses_1", &mut seen)
        .await
        .expect("backfill");
    let ids: Vec<&str> = missed.iter().map(|m| m.part_id.as_str()).collect();
    let text: String = missed.iter().map(|m| m.text.as_str()).collect();
    assert_eq!(ids, ["prt_t1", "prt_t2"]);
    assert_eq!(text, "Hello world");

    // A second backfill with the now-populated set returns nothing — the parts
    // are already rendered (dedup by part id).
    let again = backfill(&client, "ses_1", &mut seen)
        .await
        .expect("backfill");
    assert!(again.is_empty(), "already-seen parts are not re-emitted");

    // Pre-seeding one part id means only the still-unseen part comes back.
    let mut partial = SeenParts::new();
    partial.mark("prt_t1");
    let missed = backfill(&client, "ses_1", &mut partial)
        .await
        .expect("backfill");
    assert_eq!(
        missed
            .iter()
            .map(|m| m.part_id.as_str())
            .collect::<Vec<_>>(),
        ["prt_t2"]
    );
}
