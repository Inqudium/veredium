# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `veredium-core`: the stack-neutral part both client modules share - the mesh timeout ladder
  (`app.mesh`, `eu.inqudium.veredium.core.mesh`), the breaker verdicts (benign 4xx is a success, a
  client-side failure is ignored) and the Resilience4j wiring that installs them (one
  `*ConfigCustomizer` per mesh alias, the fail-fast startup guard, the ladder logger). Each client
  contributes only its exception vocabulary (`RestClientVerdicts`, `WebClientVerdicts`) and imports
  the core's `MeshTimeoutConfig` through its `*VerdictsConfig`. ADR-0001; resolves finding 1 of the
  origin module's architecture review (tool-box, 2026-10-19).
- `veredium-restclient`: the blocking client - Spring `RestClient` over Apache HttpClient 5, one
  resilience-wrapped client per mesh endpoint, the timeout ladder derived from one measured anchor,
  the Resilience4j verdicts and rungs installed through the library's customizers with a fail-fast
  startup guard, a bounded body read and legatium's exchange event. Ported from dhaase/tool-box.
- `veredium-webclient`: the reactive sibling - Spring `WebClient` over Reactor Netty with the same
  mesh ladder, the same Resilience4j customizer wiring and legatium's exchange event. Ported from
  dhaase/tool-box.
- Project scaffold: the `veredium-core` module, the build (Maven, Kotlin, Spring Boot parent,
  JaCoCo, ktlint, Dokka, CycloneDX, reproducible jars), the CI, CodeQL, Scorecard, Docs, Fuzz
  and Release workflows, and the documentation site - set up from the sibling project
  [Legatium](https://github.com/Inqudium/legatium).

### Changed

- `veredium-webclient` installs its breaker verdicts and the mesh-derived rungs through the shared
  customizer wiring of `veredium-core`; the amendment framework (`RegistryAmendments`,
  `ResilienceAmendmentApplier`, `AmendmentLogFields`, the three registrars) and its ERROR events are
  gone. An alias missing from `resilience4j.<layer>.instances` is a failed start naming the key, on
  both stacks; the reactive reference configuration now declares every derived alias under
  `resilience4j.timelimiter.instances`.
- The mesh margin of the transport's response timeout is `app.mesh.defaults.margins.response-timeout-margin-ms`
  on both stacks (was `rest-client-margin-ms` / `web-client-margin-ms`), the rung is
  `ResolvedTimeouts.responseTimeout` and `ResilientWebClient.responseTimeout` (was
  `webClientResponseTimeout`). The per-endpoint `base-url` and `headers` of the shared tree are
  honoured by the blocking client as well.

[Unreleased]: https://github.com/Inqudium/veredium/commits/main
