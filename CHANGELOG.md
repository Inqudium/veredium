# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Selectable HTTP engine per client stack, with a per-backend override (`engine.endpoints.<mesh-alias>`):
  - `veredium-restclient`: `app.restclient.engine.default` takes `apache` (default), `jdk`, `jetty` or
    `reactor-netty`.
  - `veredium-webclient`: `app.webclient.engine.default` takes `reactor-netty` (default), `jdk`, `jetty` or
    `apache`.

  The setting applies to the module's own clients only. Boot's global `spring.http.clients.*` switches are
  neither read nor written, and no transport bean is registered. Boot's `RestClient.Builder` / `WebClient.Builder`
  bean is left untouched, because the engine is set on the module's clone of it. No process-global resource or
  system property is used. Isolation tests pin all of this against Boot's real HTTP-client auto-configuration.

  Each engine is created lazily, shared by the backends that select it, and released on shutdown. The mesh
  ladder is mapped onto every engine as far as it allows. What an engine cannot apply, for example pool size
  and pending-acquire on the JDK client, is stated in a startup line and in the module guides, not silently
  ignored.

  The non-default engine libraries are optional dependencies: `jetty-client` and `reactor-netty-http` for the
  blocking client; `jetty-client`/`jetty-reactive-httpclient` and `httpclient5`/`httpcore5-reactive` for the
  reactive one.

  The start fails, naming the key:
  - for an override key that is not a mesh alias,
  - for an engine whose library is missing (with its Maven coordinates),
  - for `jdk` while the JDK would replace the backend's `Host` header. The host enables that header itself
    with `-Djdk.httpclient.allowRestrictedHeaders=host`.

  Cancellation (TimeLimiter expiry, interrupted caller, cancelled future or subscription) abandons the
  exchange on every engine. For Jetty and Reactor Netty on the blocking side, and for Apache on the reactive
  side, this closes gaps in Spring Framework 7.0.9's adapters.

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

### Fixed

- `veredium-restclient`: on the Reactor Netty engine, a call still waiting for its response head when a shutdown
  past its deadline disposed the event loops could hang until its TimeLimiter budget. Spring's wait for the head
  is a `block()` without a time bound, and the response timeout is a timer on those loops. Seen once on CI; not
  reproduced locally. The wait now runs on the engine's hand-off executor, like each body read already did, and
  the shutdown's `shutdownNow` of that executor ends it at once with an `IOException`. A unit test pins the
  hand-off.

Findings of `docs/assessment/DEFECT_ANALYSIS-2026-10-23T05-20-01.md`:

- `veredium-webclient`: the Apache engine's release no longer fails a call that is still waiting for a
  connection. It shut the I/O reactor down at once, and a reactor in `SHUTTING_DOWN` opens no connection, so a
  call queued behind `max-connections` failed with an `IOReactorShutdownException` as soon as the connection it
  waited for came free, although the drain deadline left it all the time it needed. The release now waits for
  the engine's exchanges in flight first, as Jetty's does, and shuts the reactor down after. A per-engine test
  of a call waiting for a connection pins it; on Apache it failed after 0.6 s of a 5 s deadline before the fix.
  The "in-flight exchange finishes" case, before on Jetty only, now runs on every reactive engine.
- `veredium-webclient`: the Jetty engine aborts an exchange whose response body is cancelled before its end - a
  body the codec refuses at `max-in-memory-size`, a TimeLimiter abort while the body arrives. Spring's connector
  only stopped the delivery, and Jetty kept receiving the rest on the connection, holding it and its slot of
  `max-connections` for as long as the peer sent. Pinned per engine with a backend that keeps dripping.
- `veredium-webclient`: the JDK engine's startup line and the engine page now say that a body cancelled after
  the head arrived is read to its end - the JDK client has no per-exchange abort; cancelling its body
  subscription only unregisters the subscriber. A case pins that, so a JDK that changes it turns it red.
- `veredium-restclient`: on the Reactor Netty engine, a peer cutting a body off mid-stream fails the call with
  Spring's `ResourceAccessException` over an `IOException`, as on every other engine, instead of Reactor's
  unchecked `ReactiveException` around a `PrematureCloseException` that passed Spring's transport-fault handling.
- `veredium-restclient`: an error body of exactly the declared length at `max-in-memory-size` is read to its end,
  so the engines keep its connection instead of discarding one that has nothing left on it.
- `veredium-webclient`: the JDK and Apache engines' release WARN names an interrupted shutdown thread as the
  reason, as Jetty's and the blocking module's do.
- `veredium-webclient`: the Apache engine's abort link (`AbortableExchange`) is one atomic state instead of a
  future and a flag whose correctness depended on the order of their writes.
- Tests: the reactive pool settings (`max-idle-time`, `max-life-time`, `pending-acquire-max-count`) are pinned on
  the wire per engine; the blocking response and connect timeouts are bounded from below - the blocking connect
  case had run into a refused connect on three engines (`localhost` resolved to `::1`) and is now aimed at the
  address; the max-connections case demands each engine's own wait bound; sharing one engine between backends is
  seen on the wire; the Reactor Netty gauges are read for their values, its disposal expiry and the reactive
  shared deadline are reached; the interrupted release runs on every engine that waits; WireMock request
  listeners no longer pile up across tests; the reactive response-timeout windows are as wide as telling the
  timeouts apart allows.
- `veredium-webclient`: the Reactor Netty engine's release no longer fails a call that is still waiting for a
  connection either. Its pool was disposed without a `disposeTimeout`, which is no graceful shutdown: pending
  acquires failed at once with a `PoolShutdownException`. The pool is now built with the factory's drain bound as
  its `disposeTimeout`, so its disposal serves the waiting calls and completes once every connection came back;
  the release now waits for in-flight calls on every reactive engine alike.

Findings of `docs/assessment/DEFECT_ANALYSIS-2026-10-22T14-01-49.md`:

- `veredium-restclient`: a body refused at the buffer limit is no longer read to its end on the Apache engine
  (the default) or the JDK engine. Spring's responses of both drain the rest of the body in `close`; for a large
  or slow body that held the worker, its permit and its connection, and past the TimeLimiter budget the
  client-side refusal ended as a `TimeoutException` the breaker records. Both engines now abort the exchange
  instead (Apache: the request is cancelled; JDK: the body stream is closed), as Jetty and Reactor Netty already
  did, through one shared decorator. A per-engine test with a backend that keeps dripping pins it; on Apache it
  took 30 s and ended at the budget before the fix.
- `veredium-restclient`: the factory's shutdown releases every engine even when one of them throws, reporting
  the failure in a WARN line; a Reactor Netty disposal past its wait is reported instead of thrown out of the
  destroy callback; a Jetty client stopped from an interrupted shutdown thread no longer blocks the shutdown.
  Engines created before one that fails to construct are shut down before the failure is rethrown.
- `veredium-restclient`: `create()` after `shutdown()` is refused with an `IllegalStateException` instead of
  building a client on a closed engine.
- `veredium-restclient`: the Apache pool's gauges (`restclient.pool.*`) leave the host's registry on shutdown,
  as Reactor Netty's do.
- `veredium-core`: the startup guard checks the bulkheads against each connection pool the client factories
  report (`MeshConnectionPools`, one pool per engine; the JDK client has no bound and is not checked), instead
  of against one pool over all mesh aliases.
- Tests: per-engine pins for `max-connections`, the connect timeout, per-backend response timeouts on a shared
  engine, the drain / deadline / interrupt branches of shutdown, and the isolation of the module's engine read
  off the wire and the engine's own pool instead of the factory's record.
- `veredium-webclient`: `shutdown()` releases all engines against one shared deadline instead of one bound per
  engine, and waits for Apache's idle-connection evictor to end. After `shutdown()`, `create()` for a client
  not yet built fails with an `IllegalStateException` instead of creating engine resources nothing releases.
- `veredium-webclient`: shutting down the reactive Apache engine cancels the exchanges still in flight, with a
  WARN giving their number. Their callers no longer wait out their budget, and Apache no longer logs a
  `ClosedSelectorException`.
- `veredium-webclient`: releasing the Jetty engine waits for its in-flight exchanges until the shared shutdown
  deadline, as every other engine (and Jetty on the blocking side) does, instead of stopping the client at once
  and aborting them. What is still running at the deadline is aborted with one WARN; an interrupted shutdown
  thread stops waiting at once and leaves the stop to a thread of its own.
- `veredium-webclient`: Jetty's request queue is sized at `max-connections` plus `pool.pending-acquire-max-count`,
  because Jetty queues every request, including the ones about to get a connection. Before, a burst on a cold
  pool was rejected at half the concurrency Reactor Netty accepts.
- `veredium-webclient`: only errors dropped after the exchange was cancelled are downgraded to DEBUG. Any other
  dropped error goes on to the caller's own hook or to Reactor's global handling.
- `veredium-webclient`: the guide and the startup line say that reactor-netty's `max-connections` applies per
  backend: Reactor Netty keeps a sub-pool per remote address and client configuration, and every backend's
  client has its own. The startup guard sizes the bulkheads accordingly.

Findings of `docs/assessment/DEFECT_ANALYSIS-2026-10-21T20-14-02.md`:

- An empty 2xx body (204, empty 200) now fails the call with `EmptyResponseBodyException` on both stacks,
  after the Resilience4j decoration, so the breaker records a success. Before, the blocking client threw a
  Jackson parse error, and the reactive client completed its `Mono` empty, with no error line.
- The circuit-breaker customizer no longer lets an operator's `record-exceptions` class list record a benign
  4xx: Resilience4j OR-ed the class list back in at `build()`. The startup guard now tells "customizer
  overridden" apart from "customizer not reached".
- The startup guard WARNs when the sum of the mesh aliases' bulkhead `max-concurrent-calls` exceeds
  `app.mesh.defaults.transport.max-connections`. `ResilienceConfigContractTest` (both stacks) pins the
  invariant for the reference configuration.
- The reactive client's transport lines (error, cancel, final signal, report-only validation) carry the
  caller's MDC, taken at subscription. Before, they carried it only when the host had enabled Reactor's
  automatic context propagation.
- `ResolvedTimeouts` rejects a negative `global-margin-ms`, and `MeshProperties.Transport` bounds the
  timeouts to `Int.MAX_VALUE`. Every guard message names the key, the endpoint and the value.
- The blocking async API completes the returned future on every `CancellationException` that is not the
  caller's own cancel. Before, the future could hang.
- `@Throws` on the blocking `decorate`, `get` and `post`, so Java callers can catch `TimeoutException` and
  `InterruptedException`.
- The buffer-limit line carries `adapter_name` and `adapter_url_template` like every other transport line.
- The blocking client's bounded read no longer copies the body several times: the buffer is presized from
  `Content-Length`, and a UTF-8 body goes to Jackson as bytes.
- legatium 1.2.0 → 1.2.1: Apache's pool-lease timeout (`ConnectionRequestTimeoutException`) now shows
  in the exchange event as `adapter_outcome=timeout` at WARN instead of `failure` at ERROR. A pool-lease
  case in the restclient's `LegatiumExchangeLoggingIntegrationTest` pins it. A cancelled exchange
  (TimeLimiter expiry, interrupted caller: `Closed by interrupt`) is still logged as ERROR `failure`.
- Coverage: the build-only module `veredium-coverage` merges every module's JaCoCo data into one report
  (`jacoco:report-aggregate`). A class now counts as covered when any module's tests run it; before, the
  per-module reports under-counted `veredium-core`. The badge, the CI job summary and the published
  `/coverage/` page read this report. The module is never deployed, bundled for Central, or listed in the SBOM.
- Jackson 3 is pinned to 3.1.7, above the Spring Boot 4.1.1 BOM's 3.1.5, to clear seven OSV advisories
  against `jackson-core`/`jackson-databind` 3.1.5 (five High, two Medium). Drop the pin once Boot manages
  3.1.7 or later itself.
- `ResponseValidationTest` builds its German host validator after switching the default locale. Hibernate
  Validator fixes the locale when the factory is built, so the test only passed on a de_DE machine, and CI and
  Docs on `main` had been red since 2026-10-19.
- Jazzer fuzz targets for `ResponseBodyMetrics.utf8Size` and `BoundedBodyReader`, both in the nightly Fuzz
  workflow.

### Changed

Findings of `docs/assessment/CODE_STYLE-2026-10-23T10-45-28.md`:

- **Renamed resources (operator-visible):** the module's pools, clients and threads follow one scheme,
  `<module>-<engine>`:
  - the reactive Reactor Netty pool is `webclient-reactor-netty` instead of `lap-webclient` - the `name` tag of
    its `reactor.netty.connection.provider.*` meters;
  - the blocking Apache pool is `restclient-apache` instead of `restclient-pool` - the `pool` tag of its
    `restclient.pool.*` gauges and its evictor thread's name;
  - the reactive Jetty and Apache threads are `webclient-jetty-*` and `webclient-apache-*` instead of
    `veredium-webclient-*`.

  Dashboards and alerts filtering on the old tag values need the new ones. The Resilience4j base configuration of
  both reference configurations is `mesh-backend` instead of `lap-backend`; a host that copied it keeps working
  under its own name.
- Both client stacks select their HTTP engine through one shape in `veredium-core`
  (`eu.inqudium.veredium.core.engine`).
  - `RestClientEngine` and `WebClientEngine` implement `HttpEngine` (`key`, `requiredClasses`, `dependency`);
    `WebClientEngine.propertyValue` and `coordinates` are now `key` and `dependency`, and
    `RestClientEngine.markerClass` is now `requiredClasses`.
  - The checks of an unknown override key, a missing engine library and the JDK client's Host restriction fail
    with an `IllegalArgumentException` and one message form on both stacks. The blocking stack threw an
    `IllegalStateException`.
  - The blocking stack asks the JDK whether it accepts the Host header instead of reading
    `jdk.httpclient.allowRestrictedHeaders` with `System.getProperty`, which a value set after the JDK read it
    would have fooled.
  - Both stacks' engines drain through `EngineDrain` and report an engine that did not drain with one WARN form:
    `HTTP engine '<key>' did not drain <reason> - closing it with <n> exchange(s) still in flight`.

Earlier:

- `pool.max-idle-time` must be positive on both stacks (`app.restclient.pool`, `app.webclient.pool`); zero is
  refused at binding, naming the key. Zero meant opposite things per engine: no keep-alive at all on Reactor
  Netty, no idle timeout on Jetty, every idle connection closed at each sweep on Apache.
- `veredium-webclient`: the reactive Apache engine's startup line lists `pending-acquire-timeout-ms` under
  "does NOT apply": Apache's async pool checks a waiting lease's deadline only on its next lease or release, so
  the TimeLimiter bounds the wait.
- `veredium-webclient`: the Jetty and Apache engines name their I/O threads `veredium-webclient-jetty-*` and
  `veredium-webclient-apache-*`.
- `veredium-core`: `MeshResilienceStartupGuard` takes the connection pools the client factories report
  (`MeshConnectionPools`, implemented by both factories); without one it keeps checking the single pool of
  `app.mesh.defaults.transport.max-connections`.
- `veredium-webclient`: the default Reactor Netty engine now runs on factory-owned event loops and a
  factory-owned resolver, instead of Reactor Netty's global `HttpResources`. Its connection pool is created on
  the first client. The new resolver resolves through `InetAddress`, so a remote (non-localhost) base URL means
  a blocking DNS lookup on the event loop.
- `veredium-webclient`: an exchange error reported after a cancellation is logged at DEBUG instead of as
  Reactor's `onErrorDropped` ERROR.

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
- Both clients send `Veredium.userAgent()` - `veredium/<version>` - as the `User-Agent` of every request,
  replacing the transport's default; `app.restclient.user-agent` / `app.webclient.user-agent` lets a host
  name itself instead (validated at bind time), and an endpoint's static `headers` can override either. The jar manifest
  carries the POM-derived implementation entries so the token names the release (reproducible: no build
  JDK or user enters the manifest).
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
