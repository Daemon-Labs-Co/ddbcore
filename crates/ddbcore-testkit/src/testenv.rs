//! Loads `.env.testing` from the workspace root so integration tests and
//! docker-compose.testing.yml share one source of truth for test-container
//! image tags and credentials.
//!
//! Every getter has a hardcoded fallback matching the committed
//! `.env.testing`, so `cargo test` still works if the file is missing
//! (e.g. in a minimal CI checkout).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::OnceLock;

fn load() -> &'static HashMap<String, String> {
    static VARS: OnceLock<HashMap<String, String>> = OnceLock::new();
    VARS.get_or_init(|| {
        let mut vars = HashMap::new();
        if let Some(path) = find_env_file() {
            if let Ok(contents) = std::fs::read_to_string(path) {
                for line in contents.lines() {
                    let line = line.trim();
                    if line.is_empty() || line.starts_with('#') {
                        continue;
                    }
                    if let Some((key, value)) = line.split_once('=') {
                        vars.insert(key.trim().to_string(), value.trim().to_string());
                    }
                }
            }
        }
        vars
    })
}

/// Walks up from the calling crate's manifest dir to find `.env.testing`
/// at the workspace root (works no matter which crate's tests are running).
fn find_env_file() -> Option<PathBuf> {
    let mut dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").ok()?);
    loop {
        let candidate = dir.join(".env.testing");
        if candidate.is_file() {
            return Some(candidate);
        }
        if !dir.pop() {
            return None;
        }
    }
}

/// Real environment variables take precedence over the file, so CI or a
/// developer can override any value without editing `.env.testing`.
pub fn var(key: &str, default: &str) -> String {
    std::env::var(key).ok().or_else(|| load().get(key).cloned()).unwrap_or_else(|| default.to_string())
}

pub fn pg_tag() -> String {
    var("DDBCORE_TEST_PG_TAG", "16-alpine")
}

pub fn pg_database() -> String {
    var("DDBCORE_TEST_PG_DATABASE", "postgres")
}

pub fn pg_user() -> String {
    var("DDBCORE_TEST_PG_USER", "postgres")
}

pub fn pg_password() -> String {
    var("DDBCORE_TEST_PG_PASSWORD", "postgres")
}

pub fn mariadb_tag() -> String {
    var("DDBCORE_TEST_MARIADB_TAG", "11.3")
}

pub fn mariadb_database() -> String {
    var("DDBCORE_TEST_MARIADB_DATABASE", "ddbcore_test")
}

pub fn mariadb_root_password() -> String {
    var("DDBCORE_TEST_MARIADB_ROOT_PASSWORD", "ddbcore")
}
