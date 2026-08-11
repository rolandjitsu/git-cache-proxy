# AGENTS.md

Guidance for AI coding agents in this repo. Human contributors: see [CONTRIBUTING.md](./CONTRIBUTING.md).

## Workflow

- Clarify the design before implementing. For anything non-trivial, agree on the approach first;
  prefer a short design note over jumping to code.
- One unit of change per commit. Never mix unrelated changes. Present the change for review
  before committing.
- Every change ships with tests. Run local CI before calling it done, and do not claim it passes
  without running it.
- Verify against the code and the tools: read before you answer, run before you assert.

Local CI:

```shell
cargo fmt --all --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-features
```

## Writing: code, comments, docs, commits

- Concise and to the point. No fluff. Explain the non-obvious; do not narrate the obvious.
- ASCII only. No em-dash and no `--`; write `-`. Do not use any non-ASCII glyph: write `->` for
  the right arrow, `<->` for the left-right arrow, `!=` for not-equal, straight quotes for curly
  ones, and the same for every other Unicode symbol. Applies everywhere, including this file.
- Comments justify *why*, not *what*. Delete any comment that restates the code.
- Do not use the word "seam"; say boundary, interface, or extension point.
- Do not use "bespoke"; say "custom".

## Commits

- Conventional Commits (see CONTRIBUTING.md). Write the subject in the present tense, imperative
  voice: `feat: add fetch TTL`, not `added` or `adds`.
- Keep the body minimal. The subject alone is often enough; add body lines only for the
  non-obvious *why*. Do not restate the diff or enumerate every file changed.
- Disclose AI with an `Assisted-by: Claude:claude-opus-4-8` trailer. Never `Co-Authored-By`, and
  never add a human's `Signed-off-by`.

## Tests

- Unit tests inline (`#[cfg(test)] mod tests`); public-surface tests in `tests/`.
- Put helpers *after* the tests that use them.
- Prefer deterministic time: drive tokio's paused clock, not sleeps or `yield_now` loops.

## Terminology

- **upstream**: the origin git server the proxy fetches from. The proxy holds a single
  read-only credential for it and never writes to it.
- **mirror**: the local bare repository (`clone --mirror`) the proxy serves clients from and
  keeps fresh with incremental fetches.
- **serve**: responding to a client's `info/refs` / `git-upload-pack` from the mirror.
- git-cache-proxy is strictly read-only and pull-only. Any change that could push or
  proactively replicate to upstream is out of scope.

## Code conventions

- All git protocol work is delegated to the system `git` binary. Do not reimplement pack
  negotiation; shell out to `git` so protocol correctness (v2, shallow, partial clones) is free.
- Import a type by one path and use it consistently (e.g. a single `use std::sync::Arc`, not
  mixed inline `std::sync::Arc` paths).
- Prefer `tokio::fs` in async paths unless `std::fs` is clearly fine (small, at startup, no
  blocking concern).
- Document public items with rustdoc; keep it accurate and free of drift.

## CI workflows

- GitHub Actions live in `.github/workflows`. Write the workflow `name:`, every job name, and
  every named step in Sentence case, matching `ci.yml` (e.g. `name: CI`, `Check formatting`).
- Keep workflows minimal and scoped to one purpose; prefer the built-in `GITHUB_TOKEN` over a
  personal access token.
