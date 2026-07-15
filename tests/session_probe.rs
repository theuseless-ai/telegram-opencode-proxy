//! `session_exists` vs `get_session` on a malformed-but-2xx body (issue #89
//! review follow-up).
//!
//! `get_session` decodes `GET /session/:id`'s body into `SessionResponse` so
//! the permission relay can read `parentID`/`agent`. `session_exists` only
//! needs to know the session is known to opencode at all — it must not fail
//! just because the body couldn't be decoded, or `session::get_or_create`'s
//! `session_exists(id).await?` would abort the whole turn instead of falling
//! through to recreating the session, on a response a 404 would have handled
//! gracefully.

#[path = "support/mock_opencode.rs"]
#[allow(dead_code)]
mod mock_opencode;

use telegram_opencode_proxy::opencode::client::OpencodeClient;

use mock_opencode::MockOpencode;

#[tokio::test]
async fn session_exists_treats_an_undecodable_2xx_body_as_existing() {
    let oc = MockOpencode::start().await;
    oc.set_undecodable_session("ses_broken");
    let client = OpencodeClient::new(&oc.url).expect("client");

    let exists = client
        .session_exists("ses_broken")
        .await
        .expect("session_exists must not error on a malformed-but-2xx body");
    assert!(
        exists,
        "a 2xx response means opencode knows the session, regardless of whether \
         the body decodes"
    );
}

#[tokio::test]
async fn get_session_does_surface_a_decode_error_for_the_same_body() {
    let oc = MockOpencode::start().await;
    oc.set_undecodable_session("ses_broken");
    let client = OpencodeClient::new(&oc.url).expect("client");

    assert!(
        client.get_session("ses_broken").await.is_err(),
        "get_session needs the decoded parentID/agent shape, so a malformed \
         body is legitimately an error there"
    );
}
