//! reqwest client: `create_session`, `prompt` (blocking `POST /session/:id/message`),
//! `get_messages`, permission reply, `read_file`. `model` is the split object
//! `{providerID, modelID}` on message vs `{id, providerID}` on create (§10).
//! See `docs/design/architecture.md` §10. Issue #5.
