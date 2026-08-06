# Contributing

Thanks for contributing to git-cache-proxy. These guidelines keep history clean and review
fast.

## Ground rules

1. Commits follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).
2. Keep commits small and self-contained so each can be reviewed on its own. Split large
   changes into incremental commits/PRs; never mix unrelated changes in one commit.
3. Every change ships with tests (unit, plus integration where it fits). Coverage should not drop.
4. CI must be green before review: formatting, clippy, and tests.
5. Address review feedback by amending the relevant commit and rebasing, not with follow-up
   `fix: typo` commits. Keep history linear.

## Commit messages

A Conventional Commits subject in the present tense, imperative voice (`feat: add fetch TTL`,
not `feat: added fetch TTL` or `feat: adds fetch TTL`), then an imperative body that explains
*why* when it is not obvious, wrapped at ~72 columns, ASCII only (no em-dash, no `--`).

AI-assisted commits disclose the assistant with an `Assisted-by:` trailer, following the kernel
[coding-assistants guidance](https://docs.kernel.org/process/coding-assistants.html). Do not use
`Co-Authored-By`.

```
feat(git): coalesce concurrent fetches per repo

A burst of clients for the same repo previously each triggered an upstream
fetch. Serialize them behind a per-repo lock so a burst does one fetch.

Assisted-by: Claude:claude-opus-4-8
```

Agents have extra rules in [AGENTS.md](./AGENTS.md).

## Development

```shell
cargo build
cargo test --all-features
cargo fmt --all
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo llvm-cov --all-features --locked --summary-only   # coverage, matches CI
```

Run the proxy locally:

```shell
cargo run -- --upstream https://git.example.com --cache-root /tmp/git-cache
```

Install [prek](https://github.com/j178/prek) and enable the git hooks once. They run typos
and rustfmt on commit, a Conventional Commits check on the message, and clippy on push:

```shell
brew install prek   # or: cargo install --locked prek
prek install
```

## Rust style

Follow the [Rust Style Guide](https://doc.rust-lang.org/nightly/style-guide/); `rustfmt` and
`clippy` are enforced in CI. Prefer idiomatic Rust. Keep comments concise and reserved for the
non-obvious; do not narrate the code. Optional: [typos](https://github.com/crate-ci/typos) for
spell checking.

## Design

The design and rationale live in the [README](./README.md). All git protocol work is delegated
to the system `git` binary; keep it that way so protocol correctness (v2, shallow, partial
clones) comes for free. The proxy is strictly read-only and pull-only - it must never push or
proactively replicate to upstream.
