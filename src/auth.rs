// SPDX-License-Identifier: Apache-2.0
//! Client authentication for the serving surface.

use std::fmt;

use axum::http::header::{self, HeaderMap};
use subtle::ConstantTimeEq;

/// The outcome of authenticating one client request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Serve: no auth configured, or the credential was accepted.
    Allow,
    /// Refuse with 401: the credential is missing or rejected.
    Deny,
}

pub struct Auth {
    mode: Mode,
}

enum Mode {
    Anonymous,
    Shared { token: String },
}

impl fmt::Debug for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Only the mode label: never the shared secret.
        f.debug_struct("Auth")
            .field("mode", &self.describe())
            .finish()
    }
}

impl Auth {
    /// Serve anonymously.
    pub fn anonymous() -> Self {
        Self {
            mode: Mode::Anonymous,
        }
    }

    /// Require one bearer secret, compared in constant time.
    pub fn shared(token: &str) -> Self {
        Self {
            mode: Mode::Shared {
                token: token.to_string(),
            },
        }
    }

    /// Authenticate one client request.
    pub fn check(&self, headers: &HeaderMap) -> Verdict {
        match &self.mode {
            Mode::Anonymous => Verdict::Allow,
            Mode::Shared { token } => match bearer_token(headers) {
                Some(provided) if token_matches(provided, token) => Verdict::Allow,
                _ => Verdict::Deny,
            },
        }
    }

    /// A short label for startup logging.
    pub fn describe(&self) -> &'static str {
        match &self.mode {
            Mode::Anonymous => "anonymous",
            Mode::Shared { .. } => "shared token",
        }
    }
}

/// The credential of `Authorization: Bearer <token>`. No trimming: the token
/// begins right after the scheme's single space.
fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .filter(|t| !t.is_empty())
}

/// Compare a client-supplied bearer token against the expected one in constant
/// time, so response latency does not leak how many leading bytes matched. Only
/// the length can differ observably, which is not sensitive for a shared secret.
fn token_matches(provided: &str, expected: &str) -> bool {
    provided.as_bytes().ct_eq(expected.as_bytes()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::http::HeaderValue;

    #[test]
    fn token_matches_only_the_exact_token() {
        assert!(token_matches("s3cret", "s3cret"));
        assert!(!token_matches("s3creT", "s3cret")); // last byte differs
        assert!(!token_matches("s3cre", "s3cret")); // prefix, shorter
        assert!(!token_matches("s3cret-extra", "s3cret")); // longer
        assert!(!token_matches("", "s3cret"));
        assert!(token_matches("", "")); // degenerate empty token
    }

    #[test]
    fn shared_mode_takes_bearer_only() {
        let auth = Auth::shared("tok");
        assert_eq!(auth.check(&headers("Bearer tok")), Verdict::Allow);
        assert_eq!(auth.check(&headers("Basic dXNlcjp0b2s=")), Verdict::Deny);
        assert_eq!(auth.check(&headers("Bearer ")), Verdict::Deny);
        // The token starts right after the scheme's single space.
        assert_eq!(auth.check(&headers("Bearer  tok")), Verdict::Deny);
        assert_eq!(auth.check(&HeaderMap::new()), Verdict::Deny);
        assert_eq!(Auth::anonymous().check(&HeaderMap::new()), Verdict::Allow);
    }

    fn headers(value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(header::AUTHORIZATION, HeaderValue::from_str(value).unwrap());
        h
    }
}
