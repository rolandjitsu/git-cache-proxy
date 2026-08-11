# Security Policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately, not as a public issue. Use
GitHub's private vulnerability reporting for this repository (the "Security" tab
-> "Report a vulnerability"). Include a description, the affected version, and
reproduction steps. We aim to acknowledge within a few days and will keep you
posted on remediation.

## Scope

git-cache-proxy is a shared, credentialed reader. Please read the "Security
model" section of the README first: it documents the intended trust boundary
(reachability), the single upstream credential, plain-HTTP serving, and the DoS
knobs. Reports that contradict or extend that model are in scope. Known and
documented behaviors - open defaults and an unbounded on-disk cache - are not
vulnerabilities on their own.

## Supported versions

Pre-1.0: fixes land on the latest release published to crates.io.
