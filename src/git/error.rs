// SPDX-License-Identifier: Apache-2.0
//! Upstream git failures: the typed errors, their classification from git's
//! stderr, and the redacted stderr summary logged with a final failure.

use std::process::ExitStatus;

const MAX_STDERR_SUMMARY_LINES: usize = 8;
const MAX_STDERR_SUMMARY_BYTES: usize = 1024;
const REDACTED: &str = "<redacted>";

/// Upstream refused the credential on a clone/fetch (an auth diagnostic, or a
/// 401/403/404). Carried out of the fetch so a caller in `client`
/// upstream-credential mode can answer the client a 401, not a 502: the
/// credential is the client's own.
#[derive(Debug)]
pub struct UpstreamAuthFailed;

impl std::fmt::Display for UpstreamAuthFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("upstream rejected the credential")
    }
}

impl std::error::Error for UpstreamAuthFailed {}

/// Whether `err` carries an [`UpstreamAuthFailed`] anywhere in its chain.
pub fn is_auth_failure(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| cause.is::<UpstreamAuthFailed>())
}

/// Whether `err` is an upstream git failure already logged where the operation
/// failed, so a caller need not log it again.
pub fn upstream_failure_logged(err: &anyhow::Error) -> bool {
    failure_kind(err).is_some()
}

/// The `error_kind` of an upstream failure; `None` for a local error (spawn,
/// staging cleanup), which is left to the caller to log.
pub(super) fn failure_kind(err: &anyhow::Error) -> Option<FailureKind> {
    err.chain().find_map(|cause| {
        if let Some(failed) = cause.downcast_ref::<UpstreamFailed>() {
            return Some(failed.kind);
        }
        cause
            .downcast_ref::<UpstreamStopped>()
            .map(|stopped| match stopped {
                UpstreamStopped::Cancelled => FailureKind::Cancelled,
                UpstreamStopped::TimedOut => FailureKind::Timeout,
            })
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FailureKind {
    Auth,
    TransientHttp,
    Tls,
    Connection,
    Dns,
    Unknown,
    Timeout,
    Cancelled,
}

impl FailureKind {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Auth => "auth",
            Self::TransientHttp => "transient_http",
            Self::Tls => "tls",
            Self::Connection => "connection",
            Self::Dns => "dns",
            Self::Unknown => "unknown",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
        }
    }

    pub(super) fn reason(self) -> &'static str {
        match self {
            Self::Auth => "upstream rejected the credential",
            Self::TransientHttp => "transient HTTP status",
            Self::Tls => "TLS connection interrupted",
            Self::Connection => "connection interrupted or timed out",
            Self::Dns => "DNS resolution failed",
            Self::Unknown => "permanent or unrecognized error",
            Self::Timeout => "total budget exhausted",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum UpstreamStopped {
    Cancelled,
    TimedOut,
}

impl std::fmt::Display for UpstreamStopped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Cancelled => "upstream operation cancelled",
            Self::TimedOut => "upstream operation exhausted its total budget",
        })
    }
}

impl std::error::Error for UpstreamStopped {}

/// A git process that exited unsuccessfully. The stderr tail is kept for the
/// failure log only: it is upstream-controlled text, so it stays out of
/// `Display` and `Debug` and is redacted before it is logged.
pub(super) struct UpstreamFailed {
    pub(super) operation: String,
    pub(super) repo: String,
    pub(super) attempts: usize,
    pub(super) status: ExitStatus,
    pub(super) kind: FailureKind,
    pub(super) stderr: Vec<u8>,
}

impl std::fmt::Debug for UpstreamFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpstreamFailed")
            .field("operation", &self.operation)
            .field("repo", &self.repo)
            .field("attempts", &self.attempts)
            .field("status", &self.status)
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for UpstreamFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.kind == FailureKind::Auth {
            // The cause, `UpstreamAuthFailed`, follows in the error chain.
            return write!(
                f,
                "git {} failed for {} (status {})",
                self.operation, self.repo, self.status
            );
        }
        write!(
            f,
            "git {} failed for {} after {} attempt(s) ({}, status {})",
            self.operation,
            self.repo,
            self.attempts,
            self.kind.reason(),
            self.status
        )
    }
}

impl std::error::Error for UpstreamFailed {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        // Keeps `is_auth_failure` a plain chain walk.
        static AUTH: UpstreamAuthFailed = UpstreamAuthFailed;
        (self.kind == FailureKind::Auth).then_some(&AUTH as _)
    }
}

/// Whether a non-retryable failure is upstream refusing the credential, as
/// opposed to a proxy-side permanent error (disk, lock, local cert). GitHub and
/// GitLab return 404 for a repo a credential cannot see, so a not-found reads as
/// an auth failure here. Checked only after `transient_upstream_error` has ruled
/// the error permanent.
pub(super) fn upstream_auth_diagnostic(stderr: &[u8]) -> bool {
    let text = String::from_utf8_lossy(stderr).to_ascii_lowercase();
    [
        "authentication failed",
        "could not read username",
        "repository not found",
        "returned error: 401",
        "returned error: 403",
        "returned error: 404",
    ]
    .iter()
    .any(|message| text.contains(message))
}

pub(super) fn transient_upstream_error(stderr: &[u8]) -> Option<FailureKind> {
    let text = String::from_utf8_lossy(stderr).to_ascii_lowercase();
    // Permanent errors take precedence if Git reports more than one diagnostic.
    if [
        "authentication failed",
        "repository not found",
        "could not read username",
        "certificate problem",
        "certificate verify failed",
        "certificate verification failed",
        "server certificate verification failed",
        "no space left on device",
        "permission denied",
        "read-only file system",
        ".lock",
        "returned error: 401",
        "returned error: 403",
        "returned error: 404",
    ]
    .iter()
    .any(|message| text.contains(message))
    {
        return None;
    }
    if [408, 429, 500, 502, 503, 504].iter().any(|code| {
        text.lines().any(|line| {
            line.trim_end()
                .ends_with(&format!("returned error: {code}"))
        })
    }) {
        return Some(FailureKind::TransientHttp);
    }
    if [
        "unexpected eof while reading",
        "gnutls_handshake() failed: the tls connection was non-properly terminated",
        "gnutls recv error (-110)",
        "ssl_error_syscall",
    ]
    .iter()
    .any(|message| text.contains(message))
    {
        return Some(FailureKind::Tls);
    }
    if [
        "connection reset",
        "connection timed out",
        "connection refused",
        "failed to connect to",
        "operation timed out",
        "connection timeout",
        "empty reply from server",
        "recv failure:",
        "send failure:",
        "curl 18 ",
        "curl 28 ",
        "curl 52 ",
        "curl 55 ",
        "curl 56 ",
        "curl 92 ",
    ]
    .iter()
    .any(|message| text.contains(message))
    {
        return Some(FailureKind::Connection);
    }
    if [
        "temporary failure in name resolution",
        "could not resolve host:",
        "could not resolve proxy:",
    ]
    .iter()
    .any(|message| text.contains(message))
    {
        return Some(FailureKind::Dns);
    }
    None
}

/// A bounded, single-line summary of upstream git stderr for the failure log.
/// Git already strips credentials from URLs in its own diagnostics, but
/// `remote:` lines are upstream-controlled text, so the same rules are applied
/// again and the credential in use is removed wherever it appears verbatim.
pub(super) fn redact_stderr(stderr: &[u8], secret: Option<&str>) -> String {
    let text = String::from_utf8_lossy(stderr);
    let lines: Vec<&str> = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();
    let recent = &lines[lines.len().saturating_sub(MAX_STDERR_SUMMARY_LINES)..];
    let mut summary = recent
        .iter()
        .map(|line| redact_line(line, secret))
        .collect::<Vec<_>>()
        .join(" | ");
    if summary.len() > MAX_STDERR_SUMMARY_BYTES {
        let mut cut = summary.len() - MAX_STDERR_SUMMARY_BYTES;
        while !summary.is_char_boundary(cut) {
            cut += 1;
        }
        summary.replace_range(..cut, "...");
    }
    summary
}

fn redact_line(line: &str, secret: Option<&str>) -> String {
    let replaced: String = line
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    let mut line = replaced.trim().to_owned();
    if let Some(secret) = secret {
        // The whole header first, then its credential alone. A short value is
        // skipped: replacing it would mangle unrelated text.
        let credential = secret.rsplit(char::is_whitespace).next().unwrap_or("");
        for value in [secret.trim(), credential] {
            if value.len() >= 8 {
                line = line.replace(value, REDACTED);
            }
        }
    }
    redact_userinfo(&redact_header(&line))
}

/// Keep an `Authorization` header's scheme and drop the rest of the line, as
/// git's curl trace does. Git keeps cookie names and redacts their values;
/// this drops everything after `Cookie:`, which is stricter.
fn redact_header(line: &str) -> String {
    let lower = line.to_ascii_lowercase();
    let auth = lower.find("authorization:").map(|at| {
        let value = &line[at + "authorization:".len()..];
        let scheme = value.trim_start();
        let scheme_len = scheme.find(char::is_whitespace).unwrap_or(scheme.len());
        line.len() - scheme.len() + scheme_len
    });
    let cookie = lower.find("cookie:").map(|at| at + "cookie:".len());
    match auth.into_iter().chain(cookie).min() {
        Some(end) => format!("{} {REDACTED}", &line[..end]),
        None => line.to_owned(),
    }
}

/// Replace the userinfo of every `scheme://user:pass@host` URL in `line`.
fn redact_userinfo(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut rest = line;
    while let Some(at) = rest.find("://") {
        let (head, tail) = rest.split_at(at + 3);
        out.push_str(head);
        let authority_end = tail
            .find(|c: char| c == '/' || c.is_whitespace() || matches!(c, '\'' | '"' | '<' | '>'))
            .unwrap_or(tail.len());
        match tail[..authority_end].rfind('@') {
            Some(userinfo_end) => {
                out.push_str(REDACTED);
                rest = &tail[userinfo_end..];
            }
            None => rest = tail,
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn upstream_retry_classifies_transient_diagnostics() {
        for message in [
            "TLS connect error: error:0A000126:SSL routines::unexpected eof while reading",
            "gnutls_handshake() failed: The TLS connection was non-properly terminated.",
            "GnuTLS recv error (-110): The TLS connection was non-properly terminated.",
            "OpenSSL SSL_connect: SSL_ERROR_SYSCALL in connection to github.com:443",
            "Recv failure: Connection reset by peer",
            "Failed to connect to github.com port 443: Connection refused",
            "Operation timed out after 30000 milliseconds",
            "Empty reply from server",
            "Send failure: Broken pipe",
            "Temporary failure in name resolution",
            "Could not resolve host: github.com",
            "Could not resolve proxy: proxy.example",
            "error: RPC failed; curl 18 transfer closed with outstanding read data remaining",
            "error: RPC failed; curl 92 HTTP/2 stream was not closed cleanly: CANCEL",
        ] {
            assert!(
                transient_upstream_error(message.as_bytes()).is_some(),
                "{message}"
            );
        }
        for code in [408, 429, 500, 502, 503, 504] {
            let message = format!(
                "fatal: unable to access 'https://github.com/a/b/': The requested URL returned error: {code}\n"
            );
            assert_eq!(
                Some(FailureKind::TransientHttp),
                transient_upstream_error(message.as_bytes())
            );
        }
    }

    #[test]
    fn upstream_credential_rejections_are_auth_diagnostics() {
        for stderr in [
            "fatal: Authentication failed for 'https://host/r.git/'",
            "fatal: could not read Username for 'https://host': terminal prompts disabled",
            "remote: Repository not found.",
            "The requested URL returned error: 401",
            "The requested URL returned error: 403",
            "The requested URL returned error: 404",
        ] {
            assert!(upstream_auth_diagnostic(stderr.as_bytes()), "{stderr}");
            // All are permanent, so the typed auth error, not a retry, is returned.
            assert_eq!(
                transient_upstream_error(stderr.as_bytes()),
                None,
                "{stderr}"
            );
        }
        // A proxy-side or transient failure is not a client auth failure.
        for stderr in [
            "fatal: write error: No space left on device",
            "The requested URL returned error: 503",
            "fatal: unable to access: Could not resolve host: host",
            "error: cannot lock ref refs/heads/main",
        ] {
            assert!(!upstream_auth_diagnostic(stderr.as_bytes()), "{stderr}");
        }
    }

    #[test]
    fn redact_stderr_removes_credentials() {
        let secret = "Authorization: Bearer s3cret-token-value";
        let stderr = "\
remote: token s3cret-token-value is not allowed
fatal: unable to access 'https://user:hunter2@git.example/a/b.git/': error
> Authorization: Basic dXNlcjpwYXNz
> proxy-authorization: Bearer abc.def
> Cookie: session=abc; other=def
plain https://git.example/a/b.git and ssh://git@git.example/a/b.git
";
        let summary = redact_stderr(stderr.as_bytes(), Some(secret));
        for leaked in [
            "s3cret-token-value",
            "hunter2",
            "user:",
            "dXNlcjpwYXNz",
            "abc.def",
            "session=",
        ] {
            assert!(!summary.contains(leaked), "{leaked}: {summary}");
        }
        for kept in [
            "remote: token <redacted> is not allowed",
            "https://<redacted>@git.example/a/b.git/",
            "Authorization: Basic <redacted>",
            "proxy-authorization: Bearer <redacted>",
            "Cookie: <redacted>",
            "plain https://git.example/a/b.git and ssh://<redacted>@git.example/a/b.git",
        ] {
            assert!(summary.contains(kept), "{kept}: {summary}");
        }
        // A short credential is not replaced; it would mangle unrelated text.
        assert_eq!(redact_stderr(b"a b c", Some("Bearer b")), "a b c");
    }

    #[test]
    fn redact_stderr_is_bounded_and_single_line() {
        let stderr: String = (0..20).map(|n| format!("line {n}\x07\r\x1b\n\n")).collect();
        let summary = redact_stderr(stderr.as_bytes(), None);
        assert!(!summary.contains("line 11 "), "{summary}");
        assert!(summary.starts_with("line 12"), "{summary}");
        assert!(summary.ends_with("line 19"), "{summary}");
        assert!(!summary.chars().any(char::is_control), "{summary:?}");

        // Odd length, so the cut first lands inside a two-byte character.
        let long = format!("{}z", "\u{e9}".repeat(MAX_STDERR_SUMMARY_BYTES));
        let summary = redact_stderr(long.as_bytes(), None);
        assert!(summary.starts_with("..."));
        assert!(summary.len() <= MAX_STDERR_SUMMARY_BYTES + "...".len());
    }

    #[test]
    fn failure_kinds_have_distinct_labels_and_reasons() {
        let kinds = [
            FailureKind::Auth,
            FailureKind::TransientHttp,
            FailureKind::Tls,
            FailureKind::Connection,
            FailureKind::Dns,
            FailureKind::Unknown,
            FailureKind::Timeout,
            FailureKind::Cancelled,
        ];
        let labels: HashSet<_> = kinds.iter().map(|kind| kind.as_str()).collect();
        let reasons: HashSet<_> = kinds.iter().map(|kind| kind.reason()).collect();
        assert_eq!(labels.len(), kinds.len());
        assert_eq!(reasons.len(), kinds.len());
        assert_eq!(
            UpstreamStopped::Cancelled.to_string(),
            "upstream operation cancelled"
        );
    }

    #[test]
    fn is_auth_failure_sees_the_typed_error_under_context() {
        let typed = anyhow::Error::new(UpstreamAuthFailed).context("git clone failed for r");
        assert!(is_auth_failure(&typed));
        assert!(!is_auth_failure(&anyhow::anyhow!("upstream unavailable")));
        // The error renders without leaking a URL or credential.
        assert_eq!(
            UpstreamAuthFailed.to_string(),
            "upstream rejected the credential"
        );
    }
}
