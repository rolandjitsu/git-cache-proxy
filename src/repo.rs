// SPDX-License-Identifier: Apache-2.0
//! Request-path -> (upstream URL, on-disk cache dir) resolution, hardened
//! against path traversal.

use std::path::{Path, PathBuf};

use anyhow::{Result, bail};

/// A resolved repository: where to fetch it from and where it is cached.
#[derive(Debug, Clone)]
pub struct RepoRef {
    /// Normalised repo path, e.g. `group/team/foo.git`. Used as the cache key
    /// and as the sub-path under the cache root.
    pub name: String,
    /// Full upstream clone URL.
    pub upstream_url: String,
    /// On-disk bare mirror directory.
    pub cache_dir: PathBuf,
}

/// Strip a smart-HTTP endpoint suffix (`/info/refs`, `/git-upload-pack`) from a
/// request path and return the repo name. `None` if the suffix isn't present.
pub fn repo_name_from_path(path: &str, suffix: &str) -> Option<String> {
    let repo = path
        .trim_start_matches('/')
        .strip_suffix(suffix.trim_start_matches('/'))?;
    Some(repo.trim_end_matches('/').to_string())
}

/// Reserved suffix for the staging directory a mirror is cloned into before its
/// atomic rename into place (see `git::clone_mirror`). `resolve` rejects any
/// client path containing it, so a request can never resolve onto another repo's
/// in-flight clone directory.
pub const INCOMING_SUFFIX: &str = ".__incoming__";

/// Reserved suffix for the trash directory a mirror is renamed to during eviction
/// before its (possibly slow) removal (see `git::GitCache::evict`). Reserved for
/// the same reason as `INCOMING_SUFFIX`: a client path must never alias a mirror
/// mid-eviction.
pub const EVICTING_SUFFIX: &str = ".__evicting__";

/// Validate a repo path (no traversal / absolute / NUL) and resolve it against
/// the upstream base and cache root.
pub fn resolve(name: &str, upstream_base: &str, cache_root: &Path) -> Result<RepoRef> {
    if name.is_empty() || name.contains('\0') || name.starts_with('/') {
        bail!("invalid repo path: {name:?}");
    }
    for comp in name.split('/') {
        if comp.is_empty() || comp == "." || comp == ".." {
            bail!("invalid repo path component in {name:?}");
        }
    }
    // Reserved: the clone staging and eviction trash dirs are siblings of the
    // cache dir carrying these suffixes, so a client path containing one could
    // alias an in-flight clone or a mirror mid-eviction.
    if name.contains(INCOMING_SUFFIX) || name.contains(EVICTING_SUFFIX) {
        bail!("invalid repo path (reserved suffix) in {name:?}");
    }
    let cache_dir = cache_root.join(name);
    // Defence in depth against traversal that slipped past the component checks.
    if !cache_dir.starts_with(cache_root) {
        bail!("repo path escapes cache root: {name:?}");
    }
    let upstream_url = format!("{}/{}", upstream_base.trim_end_matches('/'), name);
    Ok(RepoRef {
        name: name.to_string(),
        upstream_url,
        cache_dir,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_suffixes() {
        assert_eq!(
            repo_name_from_path("/group/team/foo.git/info/refs", "/info/refs").as_deref(),
            Some("group/team/foo.git")
        );
        assert_eq!(
            repo_name_from_path("/a/b.git/git-upload-pack", "/git-upload-pack").as_deref(),
            Some("a/b.git")
        );
        assert_eq!(repo_name_from_path("/nope", "/info/refs"), None);
    }

    #[test]
    fn rejects_traversal() {
        let root = Path::new("/cache");
        assert!(resolve("../etc/passwd", "https://up", root).is_err());
        assert!(resolve("a/../../b", "https://up", root).is_err());
        assert!(resolve("/abs", "https://up", root).is_err());
        let ok = resolve("g/r.git", "https://up/", root).unwrap();
        assert_eq!(ok.upstream_url, "https://up/g/r.git");
        assert_eq!(ok.cache_dir, Path::new("/cache/g/r.git"));
    }

    #[test]
    fn rejects_reserved_suffixes() {
        let root = Path::new("/cache");
        // A client must not be able to name a repo that maps onto the staging dir
        // (`<cache_dir>.__incoming__`) or eviction trash (`<cache_dir>.__evicting__`)
        // of another.
        assert!(resolve(&format!("foo{INCOMING_SUFFIX}"), "https://up", root).is_err());
        assert!(resolve(&format!("a/b{INCOMING_SUFFIX}"), "https://up", root).is_err());
        assert!(resolve(&format!("foo{EVICTING_SUFFIX}"), "https://up", root).is_err());
        assert!(resolve(&format!("a/b{EVICTING_SUFFIX}"), "https://up", root).is_err());
    }
}
