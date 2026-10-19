# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `veredium-restclient`: the blocking client - Spring `RestClient` over Apache HttpClient 5, one
  resilience-wrapped client per mesh endpoint, the timeout ladder derived from one measured anchor,
  the Resilience4j verdicts and rungs installed through the library's customizers with a fail-fast
  startup guard, a bounded body read and legatium's exchange event. Ported from dhaase/tool-box.
- `veredium-webclient`: the reactive sibling - Spring `WebClient` over Reactor Netty with the same
  mesh ladder, the Resilience4j amendments applied at startup and legatium's exchange event. Ported
  from dhaase/tool-box.
- Project scaffold: the `veredium-core` module, the build (Maven, Kotlin, Spring Boot parent,
  JaCoCo, ktlint, Dokka, CycloneDX, reproducible jars), the CI, CodeQL, Scorecard, Docs, Fuzz
  and Release workflows, and the documentation site - set up from the sibling project
  [Legatium](https://github.com/Inqudium/legatium).

[Unreleased]: https://github.com/Inqudium/veredium/commits/main
