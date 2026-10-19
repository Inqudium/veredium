# Security Policy

## Supported Versions

Only the latest released version of Veredium receives security fixes.

## Reporting a Vulnerability

Please **do not** report security vulnerabilities through public GitHub issues.

Instead, report them privately via
[GitHub Security Advisories](https://github.com/Inqudium/veredium/security/advisories/new)
or by email to **veredium@inqudium.eu**.

Please include:

- A description of the vulnerability and its impact
- The affected module and version
- Steps to reproduce, or a proof of concept if available

You will receive an acknowledgement within a few days. Please allow a reasonable
time for a fix to be released before any public disclosure.

## Scope notes

Veredium is a web client: it carries what a service sends to a foreign party and brings
the answer back. Reports about the client **altering a request or a response** beyond its
documented behaviour, **leaking sensitive data** into logs or error messages, or
**mishandling the peer's answer** (unbounded buffering, unchecked redirects, lax TLS) are
explicitly in scope and very welcome.

Measures already in place, so you know what is expected behaviour:

- **Dependencies are scanned continuously.** CI builds a CycloneDX SBOM of
  the resolved graph and fails on any advisory known to OSV — on every change
  and weekly, so newly published advisories surface without a commit.
  Dependabot proposes the version bumps.
- **The code itself is statically analysed.** A CodeQL workflow analyses the
  library sources and the CI workflow definitions on every change and weekly;
  results appear under Security → Code scanning.
- **Boundary parsers are fuzzed.** Components that handle peer-controlled or
  caller-controlled data are Jazzer `@FuzzTest` targets with their invariants
  asserted in the test body: explored nightly by the Fuzz workflow, and
  replayed against the checked-in findings in every build (none yet - the
  scaffold carries no parser; the first one brings its target).
- **Release assets carry SLSA build provenance.** The Release workflow
  rebuilds the jars and the SBOM from the release tag, uploads them to the
  GitHub release, and attaches Sigstore-signed SLSA provenance
  (`*.intoto.jsonl`) — verifiable with
  [slsa-verifier](https://github.com/slsa-framework/slsa-verifier). Maven
  Central artifacts are additionally GPG-signed by the release profile, and
  the build is reproducible (`project.build.outputTimestamp`, see the
  README's build section): the jars on Maven Central are byte-identical to
  the attested GitHub release assets, so the provenance verifies them too.
- **The repository's supply-chain posture is scored publicly.** The OpenSSF
  Scorecard badge in the README links to the current per-check breakdown.
  Read it as a posture indicator, not as a grade: several checks assume a
  multi-maintainer, pull-request-based project and score low by construction
  for a single-maintainer one. Where a deduction is a deliberate trade-off,
  the reason sits next to the decision — see the `repo_token` note in
  `.github/workflows/scorecard.yml`. The **Fuzzing** check in particular can
  read 0 despite nightly Jazzer fuzzing: Scorecard only scans for Jazzer
  targets when Java holds a "prominent" share of the repository's bytes, and
  a Kotlin-dominated codebase sits below that threshold — so that score
  tracks the language ratio, not the actual fuzzing coverage. The Fuzz
  workflow's run history is the authoritative signal.

Things outside this library's control, which the consuming application owns:
the peers it chooses to call, the credentials it hands to the client, and
where its log output is shipped and stored.
