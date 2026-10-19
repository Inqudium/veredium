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

- `BlockingResilienceDecorator` moves from `veredium-restclient` to `veredium-core` as a component
  usable without a client: no Spring annotation (the blocking client declares its bean in
  `RestClientConfiguration`), the worker-thread name prefix is a constructor parameter
  (`veredium-call-` by default, `restclient-call-` for the blocking client), a second `decorate`
  overload takes the three Resilience4j instances directly instead of a registry name, and
  `SELF_INFLICTED_FAILURES` names the failures a breaker used with it must ignore, and
  `decorateAsync` runs the same pipeline without blocking the caller - a `CompletableFuture` whose
  `cancel` interrupts the worker and frees its bulkhead permit. ADR-0002.
- `veredium-core`: response validation (`ResponseValidationProperties`, the `validation.*` block both
  clients bind, and `ResponseValidation`, the English-pinned validator derived from the host's factory)
  and the HTTP observation report (`HttpObservationReport`, subclassed by `RestClientObservationReport`
  and the new `WebClientObservationReport`) - both written once instead of as a class in the blocking
  client and inline in the reactive factory. `ResilientWebClientFactory` loses its `environment` and
  `observationRegistry` constructor parameters; the report reads them instead.
- `ClientCallLog` and `ResponseBodyMetrics` in `eu.inqudium.veredium.core.client`: the transport lines a client
  writes, the verdict-driven severity of its error line, the size sample, the Bean Validation of the decoded
  response and the UTF-8 size counter - identical private code in both clients until now, held once per
  client as one object. The log lines and their fields are unchanged.
- `BaseAdapter` moves to `eu.inqudium.veredium.core`: the two budgets an adapter publishes are the same two
  numbers on both stacks, so a host composes them from one type. The reactive copy's KDoc had the order of
  the two timeouts inverted; the shared one states the ladder's.
- One configuration class per client module: `RestClientConfiguration` carries the `app.restclient`
  binding, the import of the core's `MeshTimeoutConfig`, the `BreakerVerdicts` bean and the
  `BlockingResilienceDecorator` bean; `WebClientConfiguration` the reactive counterpart without the
  decorator. The interim `RestClientVerdictsConfig`, `RestClientResilienceConfig` and
  `WebClientVerdictsConfig` are gone; a host that does not scan imports the one class.
- `ResilientRestClient.getAsync` / `postAsync`: the blocking client's calls without the wait, over the
  decorator's asynchronous form - a `CompletableFuture<T>` that carries every outcome unwrapped (a
  serialisation failure included) and whose `cancel` abandons the exchange.

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
