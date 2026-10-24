# veredium-webclient metrics — reactive WebClient adapter layer

> **Ownership & baseline.** Maintained by **Dirk Haase**. Reviewed against the code on 2026-10-23.
> Where this text and the code disagree, the code
> wins.

> **Dependency facts** (`mvn dependency:list`): `resilience4j-spring-boot4:2.4.0` pulls in
> `resilience4j-micrometer:2.4.0` transitively, so the Resilience4j instances used here are bound to the
> host's Micrometer registry. **The artifact is the Boot-4 one**: this module runs on Spring Boot 4, and
> carrying `resilience4j-spring-boot3` alongside it puts two auto-configurations on the classpath and breaks
> the context with a duplicate `BulkheadConfigurationProperties` bean.

The metrics emitted by the reactive WebClient adapter layer in `eu.inqudium.veredium.webclient` — the
resilient, pooled HTTP client (`ResilientWebClientFactory` / `ResilientWebClient`) that every outbound call to
the local sidecar flows through. All meters are Micrometer meters registered in the host's `MeterRegistry`.

The tiers are kept separate below so it is clear which name belongs to whom: the package **defines** one
application meter of its own (§1), **enables** Reactor Netty's transport and pool metrics (§2) and **drives**
the Resilience4j instances, whose meters the framework auto-binds (§3).

## 1. Defined here — `webclient.response.body.size`

| Metric | Type | Meaning | Suggested use |
|---|---|---|---|
| `webclient.response.body.size` (`WebClientMetrics.RESPONSE_BODY_SIZE`) | distribution summary, base unit `bytes`, tag `adapter=<alias>` | The decoded size of each response body in UTF-8 bytes, registered by `ResilientWebClientFactory` per backend when the host wires a `MeterRegistry` (none otherwise). Only responses that FIT the codec limit are sampled - one that exceeds `app.webclient.max-in-memory-size` fails before it is measured. Unit, tag and measurement are shared with the blocking client (`ResponseBodyMetrics` in `veredium-core`). | **Sizing `app.webclient.max-in-memory-size`: read it as the headroom of the responses that work, next to the limit named on the buffer-limit failure line for the ones that do not** |

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
| `reactor.netty.http.client.*` (e.g. `response.time`, `connect.time`, `tls.handshake.time`, `data.received` / `data.sent`, `address.resolver`) | timers / distribution summaries | Per-request transport metrics, enabled in the Reactor Netty engine via `.metrics(true) { uri -> uriTag(uri) }`. The `uri` tag is templated by `WebClientMetrics` (the object in `ResilientWebClientFactory.kt`), and the tagger is built **per endpoint** from that endpoint's own path template (`uriTagger`) rather than from the templates of all endpoints — each `HttpClient` only ever calls its own path, so scoping the mapper removes by construction the chance of a request matching a FOREIGN template first and being counted under another endpoint's series. The template comes from the `app.mesh` triplet and is NOT re-declared here. A URI that does not match falls to the single `other` bucket, so a per-call path variable can NEVER mint a new series. Other tags: `method`, `status`, `remote.address` — `method` being its own tag is why two endpoints sharing a path but not a method stay in separate series. | **`response.time` p99 rising → sidecar/backend latency; correlate with the per-backend `resilience4j.timelimiter` timeouts (§3)** |
| `reactor.netty.connection.provider.*` (`total` / `active` / `idle` / `pending` / `max` / `max.pending` connections) | gauges | Connection-pool metrics from the single shared `ConnectionProvider`, built by the factory's Reactor Netty transport (`WebClientEngineTransports.kt`) when the first `reactor-netty` client is built, and enabled via `.metrics(true)`. One provider serves **all** `reactor-netty` endpoints, but Reactor Netty keeps a sub-pool per remote address and client configuration inside it, and every endpoint's client is configured on its own - so each backend has a sub-pool of its own, with its own `id` tag and its own `max-connections`. The `name` tag is `webclient-reactor-netty` (`ResilientWebClientFactory.POOL_NAME`). | **`pending` sustained `> 0` / `active` pinned at `max` → pool saturation; requests wait on connection acquisition (raise `app.mesh.defaults.transport.max-connections` or shorten the per-endpoint mesh timeouts)** |

## 3. Framework-emitted — Resilience4j (instances driven here)

`ReactiveResilienceDecorator` wraps every reactive call as
`CircuitBreaker( TimeLimiter( Bulkhead( call ) ) )`, resolving each primitive from the registries
auto-configured out of `resilience4j.*.instances.<name>` — where `<name>` is the backend name, i.e. the mesh
endpoint **alias** (derived from the triplet, e.g. `apis-example-com-POST-v8-agreements-products-lookup`). The
factory looks the endpoint up by that alias (`ResilientWebClientFactory.meshByBackend`) and the decorator uses it as the
Resilience4j instance name. `resilience4j-spring-boot4` binds these to Micrometer via `resilience4j-micrometer`,
so the meters below are emitted per instance. This module does **not** rename them into a project namespace — they stay raw `resilience4j.*`.
Counters are Prometheus `_total`.

| Metric family | Type | Meaning | Suggested alert |
|---|---|---|---|
| `resilience4j.circuitbreaker.*` (`state`, `calls`, `failure.rate`, `not.permitted.calls`, `buffered.calls`, …) | gauges / timers / counters, tag `name=<alias>` | Per-endpoint circuit breaker around the time-limited call; its `slowCallDurationThreshold` is the mesh-derived slow-call rung. The mesh customizer (`veredium-core`, fed with `WebClientVerdicts`) adds `BulkheadFullException` and `DataBufferLimitException` to every instance's ignore predicate while Resilience4j builds it, so neither this pipeline's own load shedding nor its refusal to buffer an oversized body counts as a backend fault. | **`state="open" == 1` / `failure.rate >= threshold` → that endpoint's circuit is open; calls fast-fail** |
| `resilience4j.timelimiter.*` (`calls{kind=successful\|failed\|timeout}`) | counters, tag `name=<alias>` | Per-endpoint end-to-end request budget (`timeoutDuration`), set from the mesh ladder. The Netty `responseTimeout` is NOT a fraction of this — both come independently from the same per-endpoint ladder, where `timeLimiter = responseTimeout + transport margin` (connect + pending-acquire + 100 ms) by construction, so the transport guard (the response timeout each engine puts on the request; on Reactor Netty its `responseTimeout`) always fires *before* this looser backstop. | **`kind="timeout"` rising → that endpoint exceeds its budget; correlate with `reactor.netty.http.client.response.time`** |
| `resilience4j.bulkhead.*` (`available.concurrent.calls`, `max.allowed.concurrent.calls`) | gauges, tag `name=<alias>` | Per-endpoint semaphore bulkhead bounding in-flight calls (the reactive-compatible `SemaphoreBulkhead`; `ThreadPoolBulkhead` is deliberately not used). | **`available.concurrent.calls` at 0 for sustained periods → the endpoint is concurrency-saturated; callers see `BulkheadFullException`** |

## 4. Not metrics (disambiguation)

The exchange event emits structured **log fields**, not meters. `adapter_outcome`, `adapter_duration_ms`,
`adapter_name`, `adapter_response_status_code`, `adapter_url_template`, the header and body fields and the rest
of the family are `addKeyValue` entries on the `adapter-http-exchange` log event, written by
`legatium-webclient-logging` — they appear in structured logs, never in Micrometer. This module itself writes
`adapter_name` and `adapter_url_template` on `ResilientWebClient`'s transport lines, under the library's names.
The library does register meters of its own (`adapter.logging.events`, `adapter.logging.failopen`, the opt-in
body meters) — those are documented in legatium's own guide, not here.

> **Scope.** This document covers the `eu.inqudium.veredium.webclient` adapter layer only.

**Related:** [Logging Guide: Reactive WebClient](logging-guide.md).
