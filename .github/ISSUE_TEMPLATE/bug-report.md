---
name: Bug Report
about: Report something that does not work as documented
title: "[BUG] "
labels: bug
assignees: ''

---

**Describe the bug**
A clear and concise description of what goes wrong.

**To reproduce**
The proxy invocation and the client command, with secrets redacted:

```sh
# proxy
git-cache-proxy --upstream https://git.example.com --cache-root /var/cache/git-cache-proxy

# client
git -c url."http://proxy:8080/".insteadOf="https://git.example.com/" \
  clone https://git.example.com/group/repo.git
```

1. Start the proxy with '...'
2. Clone / fetch '...'
3. See the error / wrong output

**Expected behavior**
What you expected to happen instead.

**Environment**
- git-cache-proxy version (`git-cache-proxy --version`):
- `git --version` (the proxy shells out to it):
- OS / arch:
- How you run it (`cargo install`, prebuilt binary, or `ghcr.io` image + tag):
- Upstream server (GitHub, GitLab, Gitea, other smart-HTTP):
- Relevant flags / env:

**Logs / output**
Relevant logs (rerun with `--log debug` if helpful).

> [!IMPORTANT]
> Do not paste secrets. Redact `--upstream-auth-header`, `--serve-token`,
> `Authorization` headers, tokens, and private repo URLs before posting.

**Additional context**
Anything else that helps: `/metrics` output, cache-volume state, network topology.
