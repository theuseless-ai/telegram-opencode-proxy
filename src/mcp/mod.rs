//! MCP file-transfer server (issue #65): the proxy hosts one stateless
//! `type:"remote"` MCP server that gives the opencode agent two self-describing
//! tools — `send_file_to_user` and `fetch_user_file` — so moving a file is a
//! tool call, not a filesystem convention. See `docs/design/architecture.md` §11.
//!
//! This module currently exposes only [`store`], the concurrency-safe,
//! disk-backed [`store::FileStore`] that backs both tools. The `rmcp` `FilesMcp`
//! server struct, its tool router, and the single `/mcp` axum mount are added in
//! a later task.

pub mod store;
