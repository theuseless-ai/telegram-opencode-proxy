//! Shared test-support: in-process mock servers for the hermetic harness.
//!
//! Files under `tests/` subdirectories are compiled as *modules* of the test
//! crate (not as their own test binaries), so both mocks live here and are
//! pulled into `tests/harness.rs` via `mod support;`.

pub mod mock_opencode;
pub mod mock_telegram;
