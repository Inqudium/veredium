# web-client metrics — reactive WebClient adapter layer

> **Ownership & baseline.** Maintained by **Dirk Haase**. Verified against the code at commit `589d3890` (2026-07-30)
> Where this text and the code disagree, the code
> wins.

> **Dependency facts** (`mvn dependency:list`): `resilience4j-spring-boot4:2.4.0` pulls in
> `resilience4j-micrometer:2.4.0` transitively and `micrometer-core:1.16.5` is present (via
> `spring-boot-starter-actuator` and `resilience4j-micrometer`), so the Resilience4j instances used here
> are actually bound to the registry. **The artifact is the Boot-4 one**: this module runs on Spring
> Boot 4, and carrying `resilience4j-spring-boot3` alongside it put two auto-configurations on the
> classpath and broke the context with a duplicate `BulkheadConfigurationProperties` bean.

The metrics emitted by the reactive WebClient adapter layer in `eu.inqudium.veredium.webclient` — the
resilient, pooled HTTP client (`ResilientWebClientFactory` / `ResilientWebClient`) that every outbound call to
the local sidecar flows through. All meters are Micrometer meters exposed through Spring Boot Actuator.

The package **defines no application meter of its own**; everything here is a framework integration that the
package either **enables** (Reactor Netty transport + pool metrics) or **drives** (the Resilience4j
instances, whose meters the framework auto-binds). The tiers are kept separate below so it is clear which
name belongs to which framework — and §1 records the one meter that was removed, because a dashboard or alert
may still be pointing at it.

## 1. Removed: `lap.webclient.logger.dropped`

This module used to register exactly one meter of its own, a gauge over the drop counter of a hand-rolled
async log dispatcher. **The dispatcher is gone, and the gauge with it** — the host's Logback appender is
asynchronous (`SequencingAsyncAppender` in the `logback-kafka-appender` module), so the second queue in front
of it was buffering what was already buffered one layer down. Log emission is now synchronous on the thread
that completes the exchange, and the appender owns the queueing.

**What replaces it for the question it answered** ("are log events being lost under load?"): the appender's
own discard behaviour, which is visible in the log stream rather than in a meter. `SequencingAsyncAppender`
stamps every event with a strictly monotonic sequence number *before* the queue's discard check, so events
dropped by a full queue leave a numbered gap that the log backend can detect — a stronger signal than the old
gauge, because it also covers the losses that happened past this application's own queue. See the class KDoc
there for the two Kibana queries that quantify it.

**If a dashboard or alert still references `lap_webclient_logger_dropped`**, it is now permanently absent, not
zero. Point it at the sequence-gap query instead.

## 2. Enabled here — Reactor Netty transport & pool

The package switches on Reactor Netty's built-in Micrometer integration; the meters are emitted by Reactor
Netty, not by this module, and stay in the raw `reactor.netty.*` namespace (no rename).

> **Only on the `reactor-netty` engine.** Both families below exist only for backends whose HTTP engine
> (`app.webclient.engine`, [configuration guide](webclient-config-guide.md) §1.6) is `reactor-netty`, the
> default. A backend on `jdk`, `jetty` or `apache` has **no transport meter** at all - neither per-request
> timers nor pool gauges; the module wires none for those engines. What every engine has is the
> `http.client.requests` observation (when `app.webclient.observability.http-observation-enabled` is on), the
> Resilience4j meters of §3 and `webclient.response.body.size`. The startup line of each engine in use names its
> transport meters (`transport meters ...`). A dashboard built on `reactor.netty.*` therefore goes silent for a
> backend moved to another engine - it does not drop to zero.

| Metric family | Type | Meaning | Suggested alert |
|---|---|---|---|
| `reactor.netty.http.client.*` (e.g. `response.time`, `connect.time`, `tls.handshake.time`, `data.received` / `data.sent`, `address.resolver`) | timers / distribution summaries | Per-request transport metrics, enabled in `ResilientWebClientFactory` via `.metrics(true) { uri -> uriTag(uri) }`. The `uri` tag is templated by `WebClientMetrics` (the object in `ResilientWebClientFactory.kt`), and the tagger is built **per endpoint** from that endpoint's own path template (`uriTagger`, `:739`) rather than from the templates of all endpoints — each `HttpClient` only ever calls its own path, so scoping the mapper removes by construction the chance of a request matching a FOREIGN template first and being counted under another endpoint's series. The template comes from the `app.mesh` triplet and is NOT re-declared here. A URI that does not match falls to the single `other` bucket, so a per-call path variable can NEVER mint a new series. Other tags: `method`, `status`, `remote.address` — `method` being its own tag is why two endpoints sharing a path but not a method stay in separate series. | **`response.time` p99 rising → sidecar/backend latency; correlate with the per-backend `resilience4j.timelimiter` timeouts (§3)** |
| `reactor.netty.connection.provider.*` (`total` / `active` / `idle` / `pending` / `max` / `max.pending` connections) | gauges | Connection-pool metrics from the single shared `ConnectionProvider`, built by the factory's Reactor Netty transport (`WebClientEngineTransports.kt`) when the first `reactor-netty` client is built, and enabled via `.metrics(true)`. One provider serves **all** `reactor-netty` endpoints, but Reactor Netty keeps a sub-pool per remote address and client configuration inside it, and every endpoint's client is configured on its own - so each backend has a sub-pool of its own, with its own `id` tag and its own `max-connections`. The `name` tag is `lap-webclient` (`ResilientWebClientFactory.POOL_NAME`). | **`pending` sustained `> 0` / `active` pinned at `max` → pool saturation; requests wait on connection acquisition (raise `app.mesh.defaults.transport.max-connections` or shorten the per-endpoint mesh timeouts)** |

## 3. Framework-emitted — Resilience4j (instances driven here)

`ReactiveResilienceDecorator` (its own file since the factory split) wraps every reactive call as
`CircuitBreaker( TimeLimiter( Bulkhead( call ) ) )`, resolving each primitive from the registries
auto-configured out of `resilience4j.*.instances.<name>` — where `<name>` is the backend name, i.e. the mesh
endpoint **alias** (derived from the triplet, e.g. `apis-example-com-POST-v8-agreements-products-lookup`). The
factory looks the endpoint up by that alias (`ResilientWebClientFactory.meshByBackend`) and the decorator uses it as the
Resilience4j instance name. `resilience4j-spring-boot4` binds these to Micrometer via `resilience4j-micrometer`,
so the meters below are emitted per instance. This module does **not** rename them into a project namespace
(unlike sync-bridge, which renames its resilience4j meters into `sync.*`) — they stay raw `resilience4j.*`.
Counters are Prometheus `_total`.

| Metric family | Type | Meaning | Suggested alert |
|---|---|---|---|
| `resilience4j.circuitbreaker.*` (`state`, `calls`, `failure.rate`, `not.permitted.calls`, `buffered.calls`, …) | gauges / timers / counters, tag `name=<alias>` | Per-endpoint circuit breaker around the time-limited call; its `slowCallDurationThreshold` is the mesh-derived slow-call rung. The mesh customizer (`veredium-core`, fed with `WebClientVerdicts`) adds `BulkheadFullException` and `DataBufferLimitException` to every instance's ignore predicate while Resilience4j builds it, so neither this pipeline's own load shedding nor its refusal to buffer an oversized body counts as a backend fault. | **`state="open" == 1` / `failure.rate >= threshold` → that endpoint's circuit is open; calls fast-fail** |
| `resilience4j.timelimiter.*` (`calls{kind=successful\|failed\|timeout}`) | counters, tag `name=<alias>` | Per-endpoint end-to-end request budget (`timeoutDuration`), set from the mesh ladder. The Netty `responseTimeout` is NOT a fraction of this — both come independently from the same per-endpoint ladder, where `timeLimiter = responseTimeout + timeLimiterOverWebClientMs` by construction, so the transport guard (the Netty `responseTimeout` set in `ResilientWebClientFactory`) always fires *before* this looser backstop. | **`kind="timeout"` rising → that endpoint exceeds its budget; correlate with `reactor.netty.http.client.response.time`** |
| `resilience4j.bulkhead.*` (`available.concurrent.calls`, `max.allowed.concurrent.calls`) | gauges, tag `name=<alias>` | Per-endpoint semaphore bulkhead bounding in-flight calls (the reactive-compatible `SemaphoreBulkhead`; `ThreadPoolBulkhead` is deliberately not used). | **`available.concurrent.calls` at 0 for sustained periods → the endpoint is concurrency-saturated; callers see `BulkheadFullException`** |

## 4. Not metrics (disambiguation)

The exchange event emits structured **log fields**, not meters. `adapter_outcome`, `adapter_duration_ms`,
`adapter_name`, `adapter_response_status_code`, `adapter_url_template`, the header and body fields and the rest
of the family are `addKeyValue` entries on the `adapter-http-exchange` log event, written by
`legatium-webclient-logging` (since ticket 30; before that by this module's own `ExchangeDiaryLogging`) — they
appear in structured logs, never in Micrometer. This module itself still writes `adapter_name` and
`adapter_url_template` on `ResilientWebClient`'s transport lines, under the library's names. Note the prefix: it is `adapter_`, not
`lap_adapter_`, which earlier revisions of this document claimed. The library does register meters of its own
(`adapter.logging.events`, `adapter.logging.failopen`, the three opt-in body meters) — those are documented in
`legatium/docs/GUIDE.md` §7.4, not here.

> **Scope.** This document covers the `eu.inqudium.veredium.webclient` adapter layer only. The
> performance-model collectors under `eu.dirk.haase.performance` (`*MetricCollector`) are a separate,
> non-Micrometer concern and are out of scope here.

**Related:** [Logging Guide: Reactive WebClient Adapters](logging-guide.md).
