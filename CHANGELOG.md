# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

#### Modules - 2026-10-19

- `veredium-core`: the stack-neutral part both client modules share - the mesh timeout ladder
  (`app.mesh`, `eu.inqudium.veredium.core.mesh`), the breaker verdicts (benign 4xx is a success, a
  client-side failure is ignored) and the Resilience4j wiring that installs them (one
  `*ConfigCustomizer` per mesh alias, the fail-fast startup guard, the ladder logger). Each client
  contributes only its exception vocabulary (`RestClientVerdicts`, `WebClientVerdicts`), which its
  auto-configuration hands to the core's wiring through its own `MeshResilienceRegistrar` subclass. ADR-0001; resolves finding 1 of the
  origin module's architecture review (tool-box, 2026-10-19).
- `veredium-restclient`: the blocking client - Spring `RestClient` over Apache HttpClient 5, one
  resilience-wrapped client per mesh endpoint, the timeout ladder derived from one measured anchor,
  the Resilience4j verdicts and rungs installed through the library's customizers with a fail-fast
  startup guard, a bounded body read and legatium's exchange event. Ported from dhaase/tool-box.
- `veredium-webclient`: the reactive sibling - Spring `WebClient` over Reactor Netty with the same
  mesh ladder, the same Resilience4j customizer wiring and legatium's exchange event. Ported from
  dhaase/tool-box.

#### Both client stacks in one host - 2026-10-23

- **Both client modules in one host (ADR-0003):** `app.mesh.endpoints[].stack` (`restclient` / `webclient`) assigns an
  endpoint to the stack that serves it. Needed only when a host depends on both modules - with one, every endpoint
  belongs to it. Each stack's customizers, startup guard and factory cover its own endpoints only; a factory refuses
  the other stack's endpoint, and an engine override for it, naming the owner. `MeshOwnershipCheck` fails the start,
  before any registry or pool exists, for an endpoint both stacks or no stack would serve. `veredium-examples` has a
  mixed example with both clients in one context.

#### HTTP engines - 2026-10-21 to 2026-10-23

- Selectable HTTP engine per client stack, with a per-backend override (`engine.endpoints.<mesh-alias>`):
  - `veredium-restclient`: `app.restclient.engine.default` takes `apache` (default), `jdk`, `jetty` or
    `reactor-netty`.
  - `veredium-webclient`: `app.webclient.engine.default` takes `reactor-netty` (default), `jdk`, `jetty` or
    `apache`.

  The setting applies to the module's own clients only. Boot's global `spring.http.clients.*` switches are
  neither read nor written, and no transport bean is registered. Boot's `RestClient.Builder` / `WebClient.Builder`
  bean is left untouched, because the engine is set on the module's clone of it. No process-global resource or
  system property is used. Isolation tests pin all of this against Boot's real HTTP-client auto-configuration.

  Each engine is created with the factory, shared by the backends that select it, and released on shutdown. The mesh
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

#### Examples - 2026-10-24

- `veredium-examples`: the decorator example shows the MDC hand-off. `CorrelatedCustomerLookup` puts a request id in
  the caller's MDC, and the adapter's line on the decorator's worker carries it, for `decorate` and `decorateAsync`
  alike. A fan-out over threads of the host's own hands the caller's MDC on with `CallerMdc.with`, because the
  decorator copies the MDC of the thread that calls it.

#### Build and project setup - 2026-10-19 to 2026-10-23

- A new build-only module, `veredium-examples`, shows a host using both clients: one Spring Boot
  application per calling style - blocking and asynchronous on the RestClient, reactive on the WebClient - each
  with an adapter over two backends and the configuration it needs, and one that bounds a native Apache HttpClient 4
  call with the core's `BlockingResilienceDecorator`. Its tests start each
  application against a WireMock stand-in for the sidecar, so an example that stops working fails the build. Like
  `veredium-coverage` and `veredium-test-support` it is never published; all three have a README that says why
  each is a module of its own.
- A new build-only module, `veredium-test-support`, holds the test helpers both client modules
  share - the raw-socket backends, the thread-safe log capture, `eventually`, the WireMock arrival listener, the
  configuration-file binding, the test payloads and the customized registries - in place of the copies that had
  drifted apart in each module. Like `veredium-coverage` it is never published.
- The `veredium-core` module, the build (Maven, Kotlin, Spring Boot parent,
  JaCoCo, ktlint, Dokka, CycloneDX, reproducible jars), the CI, CodeQL, Scorecard, Docs, Fuzz
  and Release workflows, and the documentation site - set up from the sibling project
  [Legatium](https://github.com/Inqudium/legatium).

### Fixed

Most entries resolve the findings of the defect analyses in `docs/assessment/`
(`DEFECT_ANALYSIS-2026-10-21T20-14-02.md`, `-2026-10-22T14-01-49.md`, `-2026-10-23T05-20-01.md`,
`-2026-10-24T10-08-59.md`, `-2026-10-24T19-38-36.md`).

#### Shutdown and engine release - 2026-10-22 to 2026-10-24

- `veredium-webclient`: on the Reactor Netty engine, a shutdown thread that was interrupted no longer fails the
  engine's release. Its wait for the pool's disposal was a `block()`, which answers an interrupt with an exception:
  the event loops were never scheduled for disposal and outlived the context, calls in flight ran until their
  response timeout, and the log showed "failed to release its resources" with a stack trace. The wait now ends on
  the interrupt with the shared drain line ("because the shutdown thread was interrupted"), keeps the flag, and the
  loops go in any case. The interrupted-shutdown case now runs on every reactive engine.
- `veredium-webclient`: the Reactor Netty engine's drain line names the drain window, as every engine's does,
  instead of the part of the shared deadline that was left when its turn came.
- `veredium-restclient`: on the Reactor Netty engine, a head wait that finished just as the worker was interrupted
  has its response closed, instead of holding its pooled connection.

- `veredium-restclient`: on the Reactor Netty engine, a call still waiting for its response head when a shutdown
  past its deadline disposed the event loops could hang until its TimeLimiter budget. Spring's wait for the head
  is a `block()` without a time bound, and the response timeout is a timer on those loops. Seen once on CI; not
  reproduced locally. The wait now runs on the engine's hand-off executor, like each body read already did, and
  the shutdown's `shutdownNow` of that executor ends it at once with an `IOException`. A unit test pins the
  hand-off.
- `veredium-webclient`: the Apache engine's release no longer fails a call that is still waiting for a
  connection. It shut the I/O reactor down at once, and a reactor in `SHUTTING_DOWN` opens no connection, so a
  call queued behind `max-connections` failed with an `IOReactorShutdownException` as soon as the connection it
  waited for came free, although the drain deadline left it all the time it needed. The release now waits for
  the engine's exchanges in flight first, as Jetty's does, and shuts the reactor down after. A per-engine test
  of a call waiting for a connection pins it; on Apache it failed after 0.6 s of a 5 s deadline before the fix.
  The "in-flight exchange finishes" case, before on Jetty only, now runs on every reactive engine.
- `veredium-webclient`: the Reactor Netty engine's release no longer fails a call that is still waiting for a
  connection either. Its pool was disposed without a `disposeTimeout`, which is no graceful shutdown: pending
  acquires failed at once with a `PoolShutdownException`. The pool is now built with the factory's drain bound as
  its `disposeTimeout`, so its disposal serves the waiting calls and completes once every connection came back;
  the release now waits for in-flight calls on every reactive engine alike.
- `veredium-webclient`: the JDK and Apache engines' release WARN names an interrupted shutdown thread as the
  reason, as Jetty's and the blocking module's do.
- `veredium-restclient`: the factory's shutdown releases every engine even when one of them throws, reporting
  the failure in a WARN line; a Reactor Netty disposal past its wait is reported instead of thrown out of the
  destroy callback; a Jetty client stopped from an interrupted shutdown thread no longer blocks the shutdown.
  Engines created before one that fails to construct are shut down before the failure is rethrown.
- `veredium-restclient`: `create()` after `shutdown()` is refused with an `IllegalStateException` instead of
  building a client on a closed engine.
- `veredium-restclient`: the Apache pool's gauges (`restclient.pool.*`) leave the host's registry on shutdown,
  as Reactor Netty's do.
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

#### Cancellation and response bodies - 2026-10-21 to 2026-10-24

- `veredium-restclient`: on the Reactor Netty engine, a response timeout during the BODY phase fails the call with
  Spring's `ResourceAccessException` over an `IOException`, as on every other engine and as during the head wait,
  instead of Netty's bare `ReadTimeoutException`.

- `veredium-restclient`, Jetty engine: a call abandoned while its body was being read left the worker's interrupt
  flag cleared - Jetty's body stream answers the interrupt with a bare `InterruptedIOException` and swallows it -
  so the exchange log had no sign of the cancellation and wrote ERROR `failure`. The transport now sets the flag
  again before it rethrows that exception, and the call is logged as `cancelled`.
- `veredium-webclient`: the Jetty engine aborts an exchange whose response body is cancelled before its end - a
  body the codec refuses at `max-in-memory-size`, a TimeLimiter abort while the body arrives. Spring's connector
  only stopped the delivery, and Jetty kept receiving the rest on the connection, holding it and its slot of
  `max-connections` for as long as the peer sent. Pinned per engine with a backend that keeps dripping.
- `veredium-webclient`: the JDK engine's startup line and the engine page now say that a body cancelled after
  the head arrived is read to its end - the JDK client has no per-exchange abort; cancelling its body
  subscription only unregisters the subscriber. A case pins that, so a JDK that changes it turns it red.
- `veredium-webclient`: the Apache engine's abort link (`AbortableExchange`) is one atomic state instead of a
  future and a flag whose correctness depended on the order of their writes.
- `veredium-restclient`: on the Reactor Netty engine, a peer cutting a body off mid-stream fails the call with
  Spring's `ResourceAccessException` over an `IOException`, as on every other engine, instead of Reactor's
  unchecked `ReactiveException` around a `PrematureCloseException` that passed Spring's transport-fault handling.
- `veredium-restclient`: an error body of exactly the declared length at `max-in-memory-size` is read to its end,
  so the engines keep its connection instead of discarding one that has nothing left on it.
- `veredium-restclient`: a body refused at the buffer limit is no longer read to its end on the Apache engine
  (the default) or the JDK engine. Spring's responses of both drain the rest of the body in `close`; for a large
  or slow body that held the worker, its permit and its connection, and past the TimeLimiter budget the
  client-side refusal ended as a `TimeoutException` the breaker records. Both engines now abort the exchange
  instead (Apache: the request is cancelled; JDK: the body stream is closed), as Jetty and Reactor Netty already
  did, through one shared decorator. A per-engine test with a backend that keeps dripping pins it; on Apache it
  took 30 s and ended at the budget before the fix.
- The blocking client's bounded read no longer copies the body several times: the buffer is presized from
  `Content-Length`, and a UTF-8 body goes to Jackson as bytes.

#### Connection pools - 2026-10-22

- `veredium-webclient`: Jetty's request queue is sized at `max-connections` plus `pool.pending-acquire-max-count`,
  because Jetty queues every request, including the ones about to get a connection. Before, a burst on a cold
  pool was rejected at half the concurrency Reactor Netty accepts.
- `veredium-webclient`: the guide and the startup line say that reactor-netty's `max-connections` applies per
  backend: Reactor Netty keeps a sub-pool per remote address and client configuration, and every backend's
  client has its own. The startup guard sizes the bulkheads accordingly.

#### Resilience and the mesh ladder - 2026-10-21 to 2026-10-24

- `veredium-core`: `ResolvedTimeouts` no longer derives a wrapped ladder from extreme values. `1 + max-retries` was
  an `Int` and the rungs plain `Long` arithmetic, computed before the guards ran: `max-retries: 2147483647` or an
  anchor near `Long.MAX_VALUE` (a unit mix-up) gave a negative or tiny `global`, response timeout, slow-call and
  TimeLimiter budget. The guards now run first, the top rung is computed with exact arithmetic, and the start fails
  when it exceeds `Int.MAX_VALUE` ms (about 24.8 days, the bound of the transport timeouts), naming the endpoint,
  the keys and the value. `margins.slow-call-backoff-ms` has the same bound.

- `veredium-core`: the startup guard checks the bulkheads against each connection pool the client factories
  report (`MeshConnectionPools`, one pool per engine; the JDK client has no bound and is not checked), instead
  of against one pool over all mesh aliases.
- The circuit-breaker customizer no longer lets an operator's `record-exceptions` class list record a benign
  4xx: Resilience4j OR-ed the class list back in at `build()`. The startup guard now tells "customizer
  overridden" apart from "customizer not reached".
- The startup guard WARNs when the sum of the mesh aliases' bulkhead `max-concurrent-calls` exceeds
  `app.mesh.defaults.transport.max-connections`. `ResilienceConfigContractTest` (both stacks) pins the
  invariant for the reference configuration.
- `ResolvedTimeouts` rejects a negative `global-margin-ms`, and `MeshProperties.Transport` bounds the
  timeouts to `Int.MAX_VALUE`. Every guard message names the key, the endpoint and the value.

#### Client API - 2026-10-21

- An empty 2xx body (204, empty 200) now fails the call with `EmptyResponseBodyException` on both stacks,
  after the Resilience4j decoration, so the breaker records a success. Before, the blocking client threw a
  Jackson parse error, and the reactive client completed its `Mono` empty, with no error line.
- The blocking async API completes the returned future on every `CancellationException` that is not the
  caller's own cancel. Before, the future could hang.
- `@Throws` on the blocking `decorate`, `get` and `post`, so Java callers can catch `TimeoutException` and
  `InterruptedException`.

#### Logging - 2026-10-21 to 2026-10-22

- `veredium-webclient`: only errors dropped after the exchange was cancelled are downgraded to DEBUG. Any other
  dropped error goes on to the caller's own hook or to Reactor's global handling.
- The reactive client's transport lines (error, cancel, final signal, report-only validation) carry the
  caller's MDC, taken at subscription. Before, they carried it only when the host had enabled Reactor's
  automatic context propagation.
- The buffer-limit line carries `adapter_name` and `adapter_url_template` like every other transport line.
- legatium 1.2.0 → 1.2.1: Apache's pool-lease timeout (`ConnectionRequestTimeoutException`) now shows
  in the exchange event as `adapter_outcome=timeout` at WARN instead of `failure` at ERROR. A pool-lease
  case in the restclient's `LegatiumExchangeLoggingIntegrationTest` pins it. A cancelled exchange
  (TimeLimiter expiry, interrupted caller: `Closed by interrupt`) is still logged as ERROR `failure`.

#### Build and dependencies - 2026-10-21

- Coverage: the build-only module `veredium-coverage` merges every module's JaCoCo data into one report
  (`jacoco:report-aggregate`). A class now counts as covered when any module's tests run it; before, the
  per-module reports under-counted `veredium-core`. The badge, the CI job summary and the published
  `/coverage/` page read this report. The module is never deployed, bundled for Central, or listed in the SBOM.
- Jackson 3 is pinned to 3.1.7, above the Spring Boot 4.1.1 BOM's 3.1.5, to clear seven OSV advisories
  against `jackson-core`/`jackson-databind` 3.1.5 (five High, two Medium). Drop the pin once Boot manages
  3.1.7 or later itself.

#### Examples - 2026-10-24

- `veredium-examples`: the async example's `overview` fails as soon as one of its two calls fails, with that call's
  error, and cancels the other. `thenCombine` waits for both inputs, so a failed order search left the future open
  until the customers call ended on its TimeLimiter budget, failed it with that timeout instead of the search's
  error, and cancelled the customers call only after it had ended. Tests for both directions; the cancellation
  test's comment no longer claims what nothing tested.

#### Tests - 2026-10-21 to 2026-10-24

- Shutdown cases check how the call still in flight ends, not only that the engine closed; the blocking Reactor
  Netty engine's head hand-off and its executor's `shutdownNow` are pinned at engine level. A per-engine case pins
  the response timeout in the body phase. Each stack has a context that calls through the factory its
  auto-configuration builds. The reactive deserialisation and response validation are pinned against the breaker,
  as on the blocking stack. `EngineDrain`, `MeshClientPlan` and `EngineSet`'s lock have core tests of their own.
  The raw-socket backends of `veredium-test-support` report a client that leaves before its head and no longer
  report their own close as the client's; they have self-tests. Assertions that promised more than they checked
  ("exactly one event", the call's own clock, the example budgets, the async example's cancel) check it now.

- The reactive pool settings (`max-idle-time`, `max-life-time`, `pending-acquire-max-count`) are pinned on
  the wire per engine; the blocking response and connect timeouts are bounded from below - the blocking connect
  case had run into a refused connect on three engines (`localhost` resolved to `::1`) and is now aimed at the
  address; the max-connections case demands each engine's own wait bound; sharing one engine between backends is
  seen on the wire; the Reactor Netty gauges are read for their values, its disposal expiry and the reactive
  shared deadline are reached; the interrupted release runs on every engine that waits; WireMock request
  listeners no longer pile up across tests; the reactive response-timeout windows are as wide as telling the
  timeouts apart allows.
- Per-engine pins for `max-connections`, the connect timeout, per-backend response timeouts on a shared
  engine, the drain / deadline / interrupt branches of shutdown, and the isolation of the module's engine read
  off the wire and the engine's own pool instead of the factory's record.
- `ResponseValidationTest` builds its German host validator after switching the default locale. Hibernate
  Validator fixes the locale when the factory is built, so the test only passed on a de_DE machine, and CI and
  Docs on `main` had been red since 2026-10-19.
- Jazzer fuzz targets for `ResponseBodyMetrics.utf8Size` and `BoundedBodyReader`, both in the nightly Fuzz
  workflow.

### Changed

Part of these entries resolve the findings of `docs/assessment/CODE_STYLE-2026-10-23T10-45-28.md`.

#### Wiring and configuration classes - 2026-10-19 to 2026-10-23

- **Mesh bean names (breaking for lookups by name):** the customizer and guard beans carry the stack -
  `meshCircuitBreakerCustomizer.<stack>.<alias>`, `meshTimeLimiterCustomizer.<stack>.<alias>`,
  `meshResilienceStartupGuard.<stack>` - and `MeshResilienceRegistrar` takes the stack's name next to its verdicts.
- **Wiring (breaking for a host that scanned the packages):** both client modules register a Spring Boot
  auto-configuration (`VerediumRestClientAutoConfiguration`, `VerediumWebClientAutoConfiguration`) instead of
  relying on a component scan of `eu.inqudium.veredium.restclient` / `.webclient`. The dependency alone wires the
  factory, the decorator, the observation report and the mesh wiring; each bean backs off for a host's own. A host
  with both modules on its classpath runs both stacks in one context, each endpoint naming its stack (ADR-0003).
  `RestClientConfiguration` and
  `WebClientConfiguration` are gone; `ReactiveResilienceDecorator` is a bean of the auto-configuration like the
  blocking one.
- `veredium-core`: a stack hands its `BreakerVerdicts` to the shared mesh wiring as an object instead of
  declaring it as a bean. The new abstract `MeshResilienceRegistrar(verdicts)` registers the per-alias
  customizers and the startup guard; each stack subclasses it (`RestClientMeshRegistrar`,
  `WebClientMeshRegistrar`) and imports it next to `MeshResilienceConfiguration`, which keeps only the `app.mesh`
  binding and the ladder logger. `MeshResilienceCustomizerRegistrar` and the `breakerVerdicts` bean are gone. A
  host's own `BreakerVerdicts` bean - or bean overriding - can no longer replace a stack's verdicts, which would
  have made the breakers record a benign 4xx unnoticed. A host that uses the core without a client subclasses the
  registrar instead of declaring the bean.
- **Renamed configuration classes:** `MeshTimeoutConfig` is `MeshResilienceConfiguration` - it wires the ladder
  logger, the startup guard and the customizers, not only timeouts - and the reactive
  `WebClientReactorContextPropagationConfig` is `WebClientReactorContextPropagationConfiguration`, now in
  `WebClientConfiguration.kt`. A host that `@Import`s either by name updates the import.
- One configuration class per client module: `RestClientConfiguration` carries the `app.restclient`
  binding, the import of the core's `MeshTimeoutConfig`, the `BreakerVerdicts` bean and the
  `BlockingResilienceDecorator` bean; `WebClientConfiguration` the reactive counterpart without the
  decorator. The interim `RestClientVerdictsConfig`, `RestClientResilienceConfig` and
  `WebClientVerdictsConfig` are gone; a host that does not scan imports the one class.
- `veredium-webclient` installs its breaker verdicts and the mesh-derived rungs through the shared
  customizer wiring of `veredium-core`; the amendment framework (`RegistryAmendments`,
  `ResilienceAmendmentApplier`, `AmendmentLogFields`, the three registrars) and its ERROR events are
  gone. An alias missing from `resilience4j.<layer>.instances` is a failed start naming the key, on
  both stacks; the reactive reference configuration now declares every derived alias under
  `resilience4j.timelimiter.instances`.

#### Configuration keys - 2026-10-19 to 2026-10-23

- **Mesh (breaking):** the margins are configured under `app.mesh.defaults.margins` only; the per-endpoint
  `app.mesh.endpoints[].margins` override (`MeshProperties.MarginsOverride`) is removed. An endpoint is tuned
  through its anchor and its `max-retries`.
- `pool.max-idle-time` must be positive on both stacks (`app.restclient.pool`, `app.webclient.pool`); zero is
  refused at binding, naming the key. Zero meant opposite things per engine: no keep-alive at all on Reactor
  Netty, no idle timeout on Jetty, every idle connection closed at each sweep on Apache.
- The mesh margin of the transport's response timeout is `app.mesh.defaults.margins.response-timeout-margin-ms`
  on both stacks (was `rest-client-margin-ms` / `web-client-margin-ms`), the rung is
  `ResolvedTimeouts.responseTimeout` and `ResilientWebClient.responseTimeout` (was
  `webClientResponseTimeout`). The per-endpoint `base-url` and `headers` of the shared tree are
  honoured by the blocking client as well.

#### Shared code in `veredium-core` - 2026-10-19 to 2026-10-23

- The stack-neutral part of the two client factories - endpoints, drain window, default headers, response-size
  meter, validation - is `MeshClientPlan` in `veredium-core`; the caller's MDC is carried onto another thread by one
  helper, `CallerMdc`, on both stacks; `WebClientMetrics.tagsOf` and the list form of `uriTag` are removed, the
  per-endpoint `uriTagger` remains.
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
- `veredium-core`: `MeshResilienceStartupGuard` takes the connection pools the client factories report
  (`MeshConnectionPools`, implemented by both factories); without one it keeps checking the single pool of
  `app.mesh.defaults.transport.max-connections`.

#### HTTP engines - 2026-10-21 to 2026-10-23

- Both stacks give their HTTP engines one lifecycle, `EngineSet` in `veredium-core`: every engine in use is created
  with the factory (the reactive stack created its engines at the first client), a client built after the
  shutdown is refused naming its backend, and an engine that did not drain is reported `within <window>` on both
  stacks.
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
- `veredium-webclient`: `ResilientWebClientFactory` takes an optional `nanoTime` clock for its shutdown deadline,
  as the blocking twin does.
- `veredium-webclient`: the default Reactor Netty engine now runs on factory-owned event loops and a
  factory-owned resolver, instead of Reactor Netty's global `HttpResources`. Its connection pool is created on
  the first client. The new resolver resolves through `InetAddress`, so a remote (non-localhost) base URL means
  a blocking DNS lookup on the event loop.
- `veredium-webclient`: the reactive Apache engine's startup line lists `pending-acquire-timeout-ms` under
  "does NOT apply": Apache's async pool checks a waiting lease's deadline only on its next lease or release, so
  the TimeLimiter bounds the wait.

#### Logging, metrics and resource names - 2026-10-21 to 2026-10-23

- `veredium-restclient`, `veredium-webclient`: legatium 1.3.0 (from 1.2.1). **Behaviour change of the blocking
  stack's exchange log:** a call its caller abandoned - a TimeLimiter that cancels the worker, an interrupted
  caller, a cancelled `getAsync` future - is now logged as WARN `adapter_outcome=cancelled`, as on the reactive
  stack, instead of ERROR `failure`, on every engine, before the response head and during the body. An alert on
  `failure` for the blocking stack sees fewer events; one on `cancelled` now fires for it too.
- `veredium-webclient`: a Reactor Netty pool that does not drain by the shutdown deadline is reported with the
  drain line every other engine writes - `HTTP engine 'reactor-netty' did not drain within … - closing it with the
  exchanges still in flight` - instead of `Connection pool 'webclient-reactor-netty' did not drain …`. A log query
  on the old text needs the new one.
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

- `veredium-webclient`: the Jetty and Apache engines name their I/O threads `veredium-webclient-jetty-*` and
  `veredium-webclient-apache-*`.
- `veredium-webclient`: an exchange error reported after a cancellation is logged at DEBUG instead of as
  Reactor's `onErrorDropped` ERROR.

#### Client API - 2026-10-19 to 2026-10-24

- **Status contract (both stacks):** only a 2xx is a body to decode. A 1xx or 3xx - no engine follows a redirect -
  fails as the stack's response exception with its status (`RestClientResponseException`,
  `WebClientResponseException`), and the breaker records it as a failure. Before, it was decoded as a body, or
  failed as `EmptyResponseBodyException` while the breaker recorded a success.

- `ResilientRestClient.getAsync` / `postAsync`: the blocking client's calls without the wait, over the
  decorator's asynchronous form - a `CompletableFuture<T>` that carries every outcome unwrapped (a
  serialisation failure included) and whose `cancel` abandons the exchange.
- Both clients send `Veredium.userAgent()` - `veredium/<version>` - as the `User-Agent` of every request,
  replacing the transport's default; `app.restclient.user-agent` / `app.webclient.user-agent` lets a host
  name itself instead (validated at bind time), and an endpoint's static `headers` can override either. The jar manifest
  carries the POM-derived implementation entries so the token names the release (reproducible: no build
  JDK or user enters the manifest).

#### Documentation and code style - 2026-10-23 to 2026-10-24

- `ADR-0004` records the selectable HTTP engines as wanted functionality.
- Docs: the guides of `veredium-core` and `veredium-restclient` follow the code again - an abandoned blocking call
  is logged as `cancelled` (legatium 1.3.0), each stack imports its own `MeshResilienceRegistrar` subclass, the
  blocking decorator's test lives in the core - and no longer use the origin project's module names, adapters and
  review citations; the core's API reference describes all seven packages.
- Code style analysis of 2026-10-23 (afternoon): KDoc, comments, test rationales and the webclient guides follow
  the code again after the legatium 1.3.0 and verdicts-as-object changes; the remaining references to the project
  the modules were ported from are gone; the engine transports of both stacks share one vocabulary
  (`*EngineTransport`, `shutdown`, `isClosed`, `exchangesInFlight`); the test suites share the log-capture scope
  and the reference-configuration loader of `veredium-test-support`; the example tests use constructor injection
  and one shared context annotation. **CI:** two new checks reject words of the origin project in `veredium-*/src`
  and a veredium class named in production KDoc by its full name in backticks instead of a link.
- Code style analysis of 2026-10-24 (`docs/assessment/CODE_STYLE-2026-10-24T14-14-45.md`, 63 findings):
  - **Startup line:** the breaker's policy line now says what it installs - benign 4xx are counted as a success,
    not ignored; its test pins the wording.
  - **Prose follows the code:** the per-engine pools (Reactor Netty: one sub-pool per backend) in
    `WebClientProperties.pool`, the webclient reference YAML and the core's `mesh-timeout.md`; ADR-0003 (both stacks
    in one host) in the startup guard's KDoc and `restclient-vs-webclient.md`; ADR-0004 in the README, the site
    index and `BaseAdapter`; the 1xx/3xx contract in `ResilientRestClient`'s KDoc; the response-timeout exception per
    engine and phase in the blocking timeout guide; the drain KDoc after the interrupt-safe wait.
  - **One name per concept:** the drain window is `drainWindow`/`window` on both stacks; the internal engine-name
    constants of the blocking transports are `ENGINE_NAME`.
  - **Logging guide:** the webclient's `logging-guide.md` keeps only what is current; the superseded diary design
    moved to `logging-history.md`, which the site does not publish.
  - **Test helpers:** the shared log-field, bulkhead and verdict helpers live once in `veredium-test-support`;
    helpers only one module uses (`SilentBackend`, `leapingClock`) moved to it.
  - **Idioms:** typed `const val`, `require` instead of a thrown `IllegalArgumentException`, `if` instead of
    `takeIf`/`?.let` chains, `@Nested` groups, links instead of backticks, and a bounded cause walk shared from the
    core (`ClientSideFailurePolicy.causeChain`).
  - `MixedCustomerAdapter` implements `BaseAdapter`; `CHANGELOG.md` has its `## [Unreleased]` heading.
- Comment audit of 2026-10-24 (`docs/assessment/COMMENT_AUDIT-2026-10-24T20-44-11.md`, 71 findings):
  - **Public KDoc that contradicted the code:**
    - `requestBudget` (both decorators) is the configured TimeLimiter budget, published and logged. Nothing
      derives a transport timeout from it.
    - `RestClientProperties.Pool` states the pending-queue bound per engine. Only Apache bounds the wait in time
      alone.
    - `ResponseBodyLimitExceededException.contentLength` is the declared length or `-1`, never the compressed wire
      size.
    - `RestClientMetrics` lists the pool gauges per engine, the Reactor Netty family included.
    - `Veredium.NAME` no longer claims to be the root of the logger names; the loggers live under
      `eu.inqudium.veredium`.
    - `MeshProperties` states each value range once, on the property that binds it, including the bounds that
      were missing. The transport bound is attributed to the Reactor Netty engine on either stack.
    - The Apache engine's cancellation names its real precondition, a virtual worker thread (JEP 444). The
      engine's startup line, `BlockingResilienceDecorator` and the blocking timeout guide say so too.
  - **One vocabulary for the breaker:** three outcomes (ignored / success / failure) and two verdicts (the
    policies), defined on `BreakerVerdicts`. The tests no longer call a benign 4xx "ignored" - it is a success.
  - **Prefixes:** CONTRIBUTING's prefix rule holds for KDoc too, and it tells `Workaround:` (goes when the
    dependency is fixed) apart from `Assumption:` (to recheck on a bump). About 120 statements about Spring,
    Resilience4j, Apache, Jetty, Reactor Netty, the JDK, legatium and the test libraries now carry the prefix and,
    where version-bound, the checked version. `grep -rn "Assumption:\|Workaround:"` is the checklist of a
    dependency bump again.
  - **One place per concept:**
    - The webclient decorator links the core for the aspect order and the breaker outcomes instead of copying
      them.
    - `WebClientEngine` is the one place for why Boot's reactive connector setting is not read (ADR-0004).
    - The engine transports keep only the reasons a knob has no equivalent. What each engine applies is the
      startup line's job.
    - The URI-template stamp of the blocking client is one helper.
    - Range and failure mode of `maxInMemorySize` are in the KDoc, not in a code comment Dokka does not render.
  - **References:**
    - The reference YAMLs point at `veredium-core/docs/mesh-timeout.md` and qualify legatium's ADRs.
    - Prose names the guide file it means.
    - Production KDoc no longer names test classes or enumerates the client stacks that use a core type.
    - The last words of the project the modules were ported from are gone.
  - **Test rationales follow their assertions:** nine success criteria and "what is tested" lines now say what
    the test checks. Each of about 45 "the previous / next / above test" references names its test, because the
    test-evidence page sorts tests alphabetically.
  - **New tests behind statements that were prose:**
    - A 5xx with an oversized error body is a breaker failure through the real reactive pipeline.
    - The decorator's nesting matches Resilience4j's aspect-order defaults.
    - The worked ladder example of `ResolvedTimeouts` is what the code derives.
    - Every blocking engine counts a gzip body after decompression.
    - An Apache body read on the virtual worker ends with the socket the interrupt closes.
    - The decorator example's shipped `application.yml` keeps its pool at least the bulkhead and its socket
      timeout below the TimeLimiter budget.
    - `DrippingBackend` publishes its shape as named constants.
  - `veredium-restclient`: the JDK engine switches Spring's response decompression on explicitly
    (`enableCompression(true)`, Spring's default until now), so the body limit counts decoded bytes by construction.

#### Build and CI - 2026-10-23

- The comment checks (no `[Symbol]` in `//`, no guide section numbers, no report citations) cover test code
  too; two new steps fail a pull request on an unused import and on a test rationale block that breaks its
  documented shape (`generate-test-catalog.py --check`).

#### Tests - 2026-10-19 to 2026-10-23

- The suites follow their documented conventions throughout - `should …` names, the three-label
  rationale block with its separator, `@Nested` groups with sentence names instead of banner comments, comments that
  state the constraint instead of history - and no longer describe removed code (the exchange diary, a shipped
  `application.yml`, the origin project's tickets). The orphaned `traceparent/conformance.txt` fixture is gone.

[Unreleased]: https://github.com/Inqudium/veredium/commits/main
