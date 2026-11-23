// SPDX-License-Identifier: Apache-2.0
//! Helpers shared by the unit tests of several modules.

use std::time::Duration;

use crate::git::GitConfig;

/// A `GitConfig` for unit tests: the real `git`, no upstream credential, no
/// upload-pack cap and a 60 second fetch TTL. Override what a test needs with
/// struct update syntax, so a new field is added here and not at every site.
pub(crate) fn test_git_config() -> GitConfig {
    GitConfig {
        git_binary: "git".into(),
        upstream_auth_header: None,
        big_file_threshold: "8m".into(),
        fetch_ttl: Duration::from_secs(60),
        max_wants: 100,
        max_upload_packs: 0,
        pack_threads: None,
        pack_window_memory: None,
        refresh: None,
    }
}

#[cfg(unix)]
use std::path::Path;

/// Write `script` through a short-lived `sh`, so no writable descriptor for it
/// lives in this process: a child forked by a parallel test would inherit one
/// and the later `exec` would fail with "Text file busy".
#[cfg(unix)]
pub(crate) fn write_test_script(path: &Path, script: &str) {
    assert!(
        std::process::Command::new("sh")
            .args(["-c", "printf '%s' \"$2\" > \"$1\"", "sh"])
            .arg(path)
            .arg(script)
            .status()
            .unwrap()
            .success()
    );
}
