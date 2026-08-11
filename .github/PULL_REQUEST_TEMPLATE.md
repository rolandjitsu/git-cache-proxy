**What kind of change does this PR introduce?**
- [ ] fix
- [ ] feat
- [ ] refactor
- [ ] perf
- [ ] docs
- [ ] test
- [ ] build / ci
- [ ] chore

**Summary**
Explain the motivation. What problem does this solve? Link any related issue.

**Tests**
- [ ] Added / updated tests (unit, plus integration where it fits)
- [ ] Not relevant, because: ...

**Checklist**
- [ ] CI is green locally: `cargo fmt --all --check`, `cargo clippy --all-targets --all-features --locked -- -D warnings`, `cargo test --all-features`
- [ ] Commits follow Conventional Commits; AI-assisted commits carry an `Assisted-by:` trailer (see CONTRIBUTING.md / AGENTS.md)
- [ ] Preserves the read-only, pull-only invariant (no push or proactive replication to upstream)
- [ ] Docs / README updated if behavior or flags changed

**Breaking change?**
If yes, describe the impact and the migration path (flags / env, on-disk cache layout).
