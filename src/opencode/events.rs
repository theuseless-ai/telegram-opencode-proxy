//! Subscribe `/global/event` (SSE); parse the wrapped frame
//! `{directory, project, payload:{id, type, properties}}` — `message.part.delta`,
//! `session.*`, `permission.asked`; reconnect + dedup-by-part-id.
//! See `docs/design/architecture.md` §10. Issue #7.
