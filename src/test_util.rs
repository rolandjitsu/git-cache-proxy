// SPDX-License-Identifier: Apache-2.0
//! Helpers shared by the unit tests of several modules.

use std::path::Path;

/// Write `script` through a short-lived `sh`, so no writable descriptor for it
/// lives in this process: a child forked by a parallel test would inherit one
/// and the later `exec` would fail with "Text file busy".
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
