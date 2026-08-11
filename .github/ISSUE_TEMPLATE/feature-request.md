---
name: Feature Request
about: Suggest an improvement
title: "[FEATURE] "
labels: enhancement
assignees: ''

---

**Is your feature request related to a problem?**
A clear and concise description of the problem. Ex. "When [...], I have to [...]."

**Describe the solution you'd like**
What you want to happen. If it touches configuration (flags / env) or the serve
path, a short sketch helps.

**Describe alternatives you've considered**
Other approaches or workarounds you tried.

**Scope check**
- [ ] Keeps the proxy strictly read-only and pull-only (it must never push or
      proactively replicate to upstream).
- [ ] Not already tracked in the README "Status / scope" roadmap (LRU eviction,
      per-repo histograms, scheduled refresh, in-process git).

**Additional context**
References, prior art, or links.
