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

/// Reserved top-level directory under the cache root holding cached git-LFS objects
/// (content-addressed by oid, shared across repos - see `lfs_object_path`). Reserved
/// like the suffixes above so a client repo path can never resolve into the LFS store.
pub const LFS_OBJECTS_DIR: &str = ".__lfs__";

/// The path marker that identifies a git-LFS endpoint, `<repo>/info/lfs/objects/...`.
const LFS_MARKER: &str = "/info/lfs/objects/";

/// Repo name for the LFS batch endpoint (`<repo>/info/lfs/objects/batch`), or `None`
/// if the path is not a batch request.
pub fn lfs_batch_repo(path: &str) -> Option<String> {
    repo_name_from_path(path, &format!("{LFS_MARKER}batch"))
}

/// Split an LFS object path (`<repo>/info/lfs/objects/<oid>`) into `(repo, oid)`.
/// Any query string must be stripped by the caller. `None` if the path is not an
/// object request or the oid is malformed.
pub fn lfs_object_from_path(path: &str) -> Option<(String, String)> {
    let p = path.trim_start_matches('/');
    let idx = p.find(LFS_MARKER)?;
    let repo = &p[..idx];
    let oid = &p[idx + LFS_MARKER.len()..];
    if repo.is_empty() || !valid_lfs_oid(oid) {
        return None;
    }
    Some((repo.to_string(), oid.to_string()))
}

/// An LFS oid is the lowercase-hex sha256 of the object's content: 64 hex digits.
/// Validated before use as both a filesystem path component and the cache key.
pub fn valid_lfs_oid(oid: &str) -> bool {
    oid.len() == 64
        && oid
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Cache key of an LFS object: its path relative to the cache root, `/`-joined so it
/// matches the keys the eviction index uses for mirrors. Content-addressed and shared
/// across all repos (the oid is the content hash), sharded by the first two hex chars
/// so no single directory holds every object. Caller must pass a `valid_lfs_oid`.
pub fn lfs_object_key(oid: &str) -> String {
    format!("{LFS_OBJECTS_DIR}/{}/{oid}", &oid[..2])
}

/// On-disk path of a cached LFS object (`cache_root` joined with `lfs_object_key`).
pub fn lfs_object_path(cache_root: &Path, oid: &str) -> PathBuf {
    cache_root.join(lfs_object_key(oid))
}

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
    // cache dir carrying these suffixes, and the LFS store is a reserved top-level
    // dir - so a client path containing any of these could alias an in-flight clone,
    // a mirror mid-eviction, or the LFS object store.
    if name.contains(INCOMING_SUFFIX)
        || name.contains(EVICTING_SUFFIX)
        || name.contains(LFS_OBJECTS_DIR)
    {
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
        // of another, or the LFS object store (`.__lfs__`).
        assert!(resolve(&format!("foo{INCOMING_SUFFIX}"), "https://up", root).is_err());
        assert!(resolve(&format!("a/b{INCOMING_SUFFIX}"), "https://up", root).is_err());
        assert!(resolve(&format!("foo{EVICTING_SUFFIX}"), "https://up", root).is_err());
        assert!(resolve(&format!("a/b{EVICTING_SUFFIX}"), "https://up", root).is_err());
        assert!(resolve(LFS_OBJECTS_DIR, "https://up", root).is_err());
        assert!(resolve(&format!("{LFS_OBJECTS_DIR}/ab/cd"), "https://up", root).is_err());
    }

    #[test]
    fn parses_lfs_batch_and_object_paths() {
        assert_eq!(
            lfs_batch_repo("/group/foo.git/info/lfs/objects/batch").as_deref(),
            Some("group/foo.git")
        );
        assert_eq!(lfs_batch_repo("/group/foo.git/info/refs"), None);

        let oid = "a".repeat(64);
        let (repo, got) =
            lfs_object_from_path(&format!("/g/r.git/info/lfs/objects/{oid}")).unwrap();
        assert_eq!(repo, "g/r.git");
        assert_eq!(got, oid);
        // A non-hex or wrong-length oid is not an object path.
        assert!(lfs_object_from_path("/g/r.git/info/lfs/objects/NOTHEX").is_none());
        assert!(lfs_object_from_path("/g/r.git/info/lfs/objects/abc").is_none());
        // The batch endpoint is not an object (batch is not a valid oid).
        assert!(lfs_object_from_path("/g/r.git/info/lfs/objects/batch").is_none());
    }

    #[test]
    fn validates_oids_and_shards_the_object_path() {
        assert!(valid_lfs_oid(&"0".repeat(64)));
        assert!(valid_lfs_oid(&format!(
            "{}{}",
            "a".repeat(32),
            "f".repeat(32)
        )));
        assert!(!valid_lfs_oid(&"A".repeat(64))); // uppercase is not git-lfs's form
        assert!(!valid_lfs_oid(&"a".repeat(63)));
        assert!(!valid_lfs_oid(&"g".repeat(64))); // not hex

        let oid = format!("ab{}", "c".repeat(62));
        assert_eq!(
            lfs_object_path(Path::new("/cache"), &oid),
            Path::new("/cache")
                .join(LFS_OBJECTS_DIR)
                .join("ab")
                .join(&oid)
        );
    }
}
