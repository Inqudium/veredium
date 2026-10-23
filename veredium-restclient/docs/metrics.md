# rest-client metrics — blocking RestClient adapter layer

> **Ownership & baseline.** Maintained by **Dirk Haase**. Verified against the code at commit `cd5bceb4`
> (2026-10-19). Where this text and the code disagree, the code wins.

> **Dependency facts** (`rest-client/pom.xml`): `resilience4j-spring-boot4:2.4.0` pulls in
> `resilience4j-micrometer:2.4.0` transitively, `micrometer-core` and `micrometer-observation` arrive through
> `spring-boot-starter-actuator`, and `httpclient5-observation` (version from the Boot BOM, same as
> `httpclient5`) supplies the pool binder. No `reactor-*` artifact reaches a consumer's classpath through this
> module: `reactor-netty-http` is an optional dependency, present only when the host adds it to select the
> `reactor-netty` engine — and only then can `reactor.netty.*` pool gauges appear (§2.1, §7).

The metrics emitted by the blocking RestClient adapter layer in `eu.inqudium.veredium.restclient` —
the resilient, pooled HTTP client (`ResilientRestClientFactory` / `ResilientRestClient`) that every outbound
call to the local sidecar flows through. All meters are Micrometer meters exposed through Spring Boot Actuator.

Four tiers, kept apart so it is clear which name belongs to whom:

| Tier | Namespace | Who emits it |
|---|---|---|
| §1 Module-owned | `restclient.response.body.size` | `ResilientRestClient` |
| §2 Enabled here | `restclient.pool.*` | Apache HttpClient's `ConnPoolMeters`, bound by the factory |
| §3 Framework, wired by Boot | `http.client.requests` | Spring's `RestClient` observation |
| §4 Framework, driven here | `resilience4j.*` | Resilience4j's Micrometer binder |

Every meter is **optional in the same sense**: the factory and the decorator take `MeterRegistry` and
`ObservationRegistry` as nullable constructor parameters, so a host without actuator — or a test constructing
the factory directly — gets a working client and no meters, never a fallback to `Metrics.globalRegistry`.

---

## 1. Module-owned — `restclient.response.body.size`

The one meter this module registers itself (the reactive twin registers its sibling
`webclient.response.body.size`). Constants in `RestClientMetrics`.

| Metric | Type | Tags | Meaning |
|---|---|---|---|
| `restclient.response.body.size` | distribution summary, base unit `bytes` | `adapter=<mesh alias>` | The **decoded** size of every response body that **fitted** the buffer, in UTF-8 bytes. |

**What it is for.** `app.restclient.max-in-memory-size` (default 256 KB) is a ceiling somebody has to choose,
and this is what to choose it from: the distribution of bodies that work. Read `max` and the upper percentiles
per adapter, and keep the ceiling a comfortable factor above them.

**What it cannot tell you.** Only bodies that fit are sampled — a response past the limit is never decoded,
so it leaves no sample. For those, the one WARN line `Response body exceeded the buffer limit on
backend '<alias>': limit=N bytes, Content-Length=M` carries the two numbers instead. `Content-Length` there is a
**lower bound**: the engines strip the header when they decompress a gzip body, so under the default
`Accept-Encoding` the line reads `Content-Length=unknown` and only the limit is certain.

**Why bytes after decompression, and why UTF-8.** The bounded reader counts what Apache hands it, which is the
inflated body — the same bytes the limit bounds. For a UTF-8 body (JSON's default, and what a body without a
declared charset is read as) the sample is simply the number of bytes read: Jackson parses those bytes
directly, and no `String` copy of the body is made. A body in another declared charset is decoded with it and
sampled with `ResponseBodyMetrics.utf8Size` (`veredium-core`), so every sample is in UTF-8 bytes, never in
`String.length`: a UTF-16 count would under-state multi-byte content by up to a factor of three, in exactly
the direction that sizes a ceiling too small. `ResponseBodyMetricsTest` and its fuzz target pin the counter
against the JDK encoder; `ResilientRestClientIntegrationTest` pins both paths.

**The tag value is the mesh endpoint alias, not an adapter class.** `InvolvedPartyAdapter` owns two aliases
(`apis-example-com-POST-v5-involved-parties-lookup`, `apis-example-com-GET-v6-involved-parties`), so a filter on
`adapter` selects one *call*, not one adapter. It is the same value the exchange event and the client's own
log lines carry as `adapter_name`, so a meter and a log line correlate on sight.

---

## 2. Enabled here — the shared connection pool (`restclient.pool.*`)

The Apache engine (the default, §2.1 for the others) builds one `PoolingHttpClientConnectionManager` for all
endpoints on it and binds its gauges once, when the factory is constructed, through Apache's `ConnPoolMeters` (`httpclient5-observation`). Micrometer's own
`PoolingHttpClientConnectionManagerMetricsBinder` is deprecated in favour of exactly this binder, which is why
the names are Apache's shape under this module's prefix rather than Micrometer's
`httpcomponents.httpclient.pool.*`.

| Metric | Type | Tags | Meaning | Suggested alert |
|---|---|---|---|---|
| `restclient.pool.leased` | gauge | `pool=restclient-apache` | Connections currently handed out to in-flight exchanges. | Pinned at `max-connections` → the pool is the bottleneck; see below before raising it. |
| `restclient.pool.available` | gauge | `pool=restclient-apache` | Idle keep-alive connections ready for reuse. | — |
| `restclient.pool.pending` | gauge | `pool=restclient-apache` | Callers waiting for a connection. | Sustained `> 0` → saturation; each waiter burns its budget doing nothing and fails after `pending-acquire-timeout-ms` (200 ms). |

**How to read `pending` next to the bulkheads.** The bulkheads (§4) are sized so that
`sum(max-concurrent-calls) <= app.mesh.defaults.transport.max-connections` — `ResilienceConfigContractTest`
enforces it for the module's reference configuration, and `MeshResilienceStartupGuard` (`veredium-core`) logs a
WARN naming both numbers and every alias when a host's configuration breaks it. Under that invariant the bulkheads shed load *before* the pool runs out, and `pending` stays at
zero; a non-zero `pending` therefore means the invariant was broken by configuration, or a second consumer of
the pool exists. Raising `max-connections` without raising the bulkheads moves nothing; raising the bulkheads
without the pool moves the queueing into the pool, where it fails later and with a less obvious error.

**What the pool does NOT bound.** Apache's pending queue is bounded in time only — there is no counterpart of
Reactor Netty's `pending-acquire-max-count`, and the twin's property was dropped rather than kept inert. The
pool also has **no per-request meters** of its own here (no `connect.time`, no `data.received`): the
per-request view is §3.

**The `pool` tag is the constant `restclient-apache`** (`RestClientMetrics.POOL_NAME`). It also names the
evictor thread (`restclient-apache-evictor`) and the pool in the shutdown line. One factory, one pool: no
deployment runs two in one registry, so the name is not a property.

**`max` is not a gauge.** The pool's capacity is configuration (`app.mesh.defaults.transport.max-connections`,
default 100, applied as both `maxConnTotal` and `maxConnPerRoute`), and the factory logs it at startup; the
binder exposes the three live counts only. A dashboard that wants the ratio reads the limit from config.

### 2.1 Pool meters per engine

The gauges above are the **Apache** engine's (`app.restclient.engine`, the default; [the guide's
"Engines"](GUIDE.md#engines)). Each engine in use registers its own, in the host's `MeterRegistry` and never in
Micrometer's global one; with no registry, none:

| Engine | Pool meters |
|---|---|
| `apache` | `restclient.pool.{leased,available,pending}{pool=restclient-apache}` — registered only when a backend runs on Apache |
| `jdk` | **none** — the JDK client exposes no pool statistics |
| `jetty` | **none** — Jetty has no Micrometer binder |
| `reactor-netty` | Reactor Netty's own family under its own names and tags: `reactor.netty.connection.provider.{total,active,idle,pending,max,max.pending}.connections`, tagged `name=restclient-reactor-netty`, `id`, `remote.address` — registered by the engine's registrar per remote address, removed when the pool is disposed |

`restclient.pool.pending` and `reactor.netty.connection.provider.pending.connections` answer the same question for
their engine. The JDK and Jetty engines leave the bulkheads (§4) as the only concurrency signal; the per-engine
startup line says which engine a backend runs on and which pool settings it applies. Everything in §1, §3 and §4
is engine-independent: the size summary, the `http.client.requests` observation and the Resilience4j meters sit
above the request factory.

---

## 3. Framework, wired by Boot — `http.client.requests`

Spring's `RestClient` opens one `http.client.requests` observation per exchange when an `ObservationRegistry`
sits on the builder. The factory does not wire that registry: Boot's `ObservationRestClientCustomizer` puts it
on the `RestClient.Builder` bean the factory clones, and the module toggle
`app.restclient.observability.http-observation-enabled` only decides whether the clone *keeps* it (`true` in
the reference configuration; code default `false`, in which case the factory swaps in `ObservationRegistry.NOOP` for its clients
only). With Boot's `DefaultMeterObservationHandler` behind the registry that becomes:

| Metric | Type | Tags | Meaning |
|---|---|---|---|
| `http.client.requests` | timer | `method`, `uri`, `status`, `outcome`, `client.name`, `exception` | Wall time of the exchange, from request creation to response close — **including** the bounded body read, since the response is closed after the exchange function returns. |
| `http.client.requests.active` | long task timer | `method`, `uri`, `client.name` | Exchanges currently in flight on the worker threads. |

**The `uri` tag is the mesh path template, by construction.** `ResilientRestClient` stamps every request with
the endpoint's template under `ClientRequestLoggingInterceptor.URI_TEMPLATE_ATTRIBUTE` — the same attribute
Spring's own `uri(template, vars)` sets — so a GET built from the template and a POST built with the
`uri { builder -> … }` overload both land under `/v6/involved-parties/{uuid}`-style values. This is the
module's whole cardinality guard, and the reason the twin's URI tagger has no counterpart here (§7). Pinned by
`LegatiumExchangeLoggingIntegrationTest`.

**`client.name` is the request host — `localhost`.** Behind the sidecar every endpoint shares it, so the tag
does not tell backends apart. Use `uri`, or the exchange event's `adapter_name`.

**Where the observation runs, and what that means.** The exchange runs on the decorator's virtual worker
thread, not the caller's. `BlockingResilienceDecorator` reopens the caller's current observation on the worker
before the call, so the client observation nests under the caller's span (`parentObservation` set) instead of
starting a detached root — the blocking counterpart of the twin's Reactor context-propagation hook, pinned by
`BlockingResilienceDecoratorTest` and the integration test's nesting cases.

**What the tags say for each failure.** The convention derives them from the response and from the error the
client recorded on the observation, which for I/O failures is Spring's wrapper, not the cause:

| The call | `status` | `outcome` | `exception` |
|---|---|---|---|
| 2xx, body fits | `200` | `SUCCESS` | `none` |
| 2xx, body past the limit | `200` | `SUCCESS` | `ResponseBodyLimitExceededException` |
| 4xx / 5xx | the code | `CLIENT_ERROR` / `SERVER_ERROR` | the subclass's simple name, e.g. `NotFound`, `InternalServerError` |
| transport response timeout | `CLIENT_ERROR` | `UNKNOWN` | `ResourceAccessException` (cause `SocketTimeoutException`) |
| cancelled — TimeLimiter expiry or interrupted caller | `CLIENT_ERROR` | `UNKNOWN` | `ResourceAccessException` (cause `SocketException: Closed by interrupt`) |

The causes in the last two rows are the Apache engine's; on the other engines the response timeout's cause is
`HttpTimeoutException` (`jdk`), `TimeoutException` (`jetty`) or `ReadTimeoutException` (`reactor-netty`), and a
cancellation's an `IOException` over the interrupt - the tags are the same. The last two rows are
indistinguishable on the timer: `status=CLIENT_ERROR` is the convention's value for "no
response arrived", and the wrapper hides the cause. To split timeouts from cancellations, use
`resilience4j.timelimiter.calls{kind=timeout}` (§4) and the exchange event's `adapter_outcome`
(`timeout` versus `cancelled`, §5). Note also that a 2xx whose body does not fit is a `SUCCESS` here — the
exchange did succeed — and only §1's missing sample and the WARN line say otherwise.

**Three switches can silence it, all silently.** The module toggle above; the central
`management.observations.enable.http.client.requests` / `.all` disable-filter (default `true`); and a host
without a *recording* `ObservationRegistry` bean (Micrometer counts a handler-less registry as noop).
`RestClientObservationReport` (the core's `HttpObservationReport` with this module's name and key) logs the
effective verdict at startup — `on (Boot's registry stays on the builder)` versus `on, but … nothing is
observed` — and the core's `HttpObservationReportTest` pins every branch.
When the timer
is missing, read those two lines before reading Micrometer configuration.

---

## 4. Framework, driven here — Resilience4j

`BlockingResilienceDecorator` wraps every call as `CircuitBreaker( TimeLimiter( Bulkhead( call ) ) )`,
resolving each primitive from the registries auto-configured out of `resilience4j.*.instances.<name>`, where
`<name>` is the mesh endpoint **alias**. `resilience4j-spring-boot4` binds these through
`resilience4j-micrometer`, so the meters below are emitted per instance, raw `resilience4j.*`, tag
`name=<alias>`. Counters are Prometheus `_total`.

| Metric family | Type | Meaning | Suggested alert |
|---|---|---|---|
| `resilience4j.circuitbreaker.*` (`state`, `calls{kind}`, `failure.rate`, `slow.call.rate`, `not.permitted.calls`, `buffered.calls`) | gauges / timers / counters | Per-endpoint breaker, outermost. Two startup contributors shape what it counts (see "three verdicts" below). Its `slowCallDurationThreshold` is the mesh-derived rung only when `circuit-breaker.enabled` is on for the endpoint. | `state="open" == 1` → calls fast-fail with `CallNotPermittedException`. |
| `resilience4j.timelimiter.calls{kind=successful\|failed\|timeout}` | counters | Per-endpoint end-to-end budget. **Blocking shape:** the call runs on a worker, the caller waits on its future for at most `timeoutDuration`; `kind=timeout` means the caller was released and the worker cancelled. | `kind="timeout"` rising → the endpoint exceeds its budget **in the phases the response timeout does not cover** (connect, pending-acquire): the transport's own timeout fires first in the normal case and is a `failed`, not a `timeout`. |
| `resilience4j.bulkhead.*` (`available.concurrent.calls`, `max.allowed.concurrent.calls`) | gauges | Per-endpoint semaphore bulkhead, innermost; the permit is taken **on the worker**, so it counts calls genuinely in flight at the backend. | `available.concurrent.calls == 0` sustained → callers see `BulkheadFullException`; check `restclient.pool.pending` (§2) in the same breath. |

**Three verdicts, not two — what `calls{kind}` actually counts.** Resilience4j has three answers for an error,
and one customizer per mesh alias (`veredium-core`, fed with `RestClientVerdicts`) sorts every exception into them while
Resilience4j builds the instance:

| The call | Verdict | Source |
|---|---|---|
| never reached the backend, or the caller stopped waiting: `BulkheadFullException`, `ResponseBodyLimitExceededException`, `InterruptedException` (any of them as a cause too) | **ignored** — absent from the window | `RestClientVerdicts.clientSideFailure` |
| reached it and got a benign 4xx (every 4xx except 408 and 429) | **success** — enters the window as such | `RestClientVerdicts.httpExchangeError` |
| anything else: transport faults, 5xx, 408/429, the TimeLimiter abort | **failure** | the config defaults |

So `failure.rate` is a statement about the *backend*, not about this process: a 404-heavy lookup, a full
bulkhead or an oversized body never move it. Deserialisation and Bean Validation sit outside the decoration
entirely and are invisible to every `resilience4j.*` meter. The middle row is the one that is easy to get wrong
in the safe-looking direction — ignoring a benign 4xx empties the window on a 4xx-dominated backend until a
handful of real faults are its whole content — which is why it is a *success*, not an *ignore*.

**`state` reflects the customized instance.** The customizers run while Resilience4j's Boot configuration
builds each instance from its `instances.<alias>` entry, so the binder binds to the final instance and nothing
is rebuilt later. `MeshResilienceStartupGuard` fails the context when an alias is undeclared in
`resilience4j.<layer>.instances` (the instance then exists, but on the registry default instead of `mesh-backend`)
or a customizer's effect is not observable on the instance the registry serves
— a breaker whose meters look like the defaults therefore means the context did not start, and the failure
message names the key to add.

---

## 5. Framework, wired by Boot — legatium exchange logging

`legatium-restclient-logging` rides on Boot's `RestClient.Builder` bean and registers meters of its own; they
are documented in `legatium/docs/GUIDE.md` §7.4, not here. The ones worth knowing from this side:

| Metric | Tags | Note for this module |
|---|---|---|
| `adapter.logging.exchanges.open` | `client=restclient` | Exchanges between wiring and their exactly-once completion. The `client` tag separates the two twins in one registry. A growing baseline means responses are never closed — not possible through `ResilientRestClient`, which closes every response, so a rise points at a host using the builder directly. |
| `adapter.logging.events` | `outcome` | Exchange events actually emitted; the reconciliation base for the log index. A **cancelled** call counts as `cancelled` here, as on the reactive twin (since legatium 1.3.0; on Jetty a call cancelled during its body counts as `failure`, see timeouts-bulkheads-threads.md §3). |
| `adapter.logging.failopen` | `stage` | The logging gave up on an exchange. Independent of every meter above. |
| `adapter.logging.correlation.id` | `source=trace\|header\|generated` | A rising `generated` share means the host stopped propagating `traceparent` — or, for this module, that §3's observation is off. |
| `adapter.request.body.size`, `adapter.response.body.size`, `adapter.response.body.read` | `uri`, `host`, `name` | Opt-in (`measure-*-body-size`, `true` in the reference configuration). `name` is the same alias as §1's `adapter` tag. The response size here is what the library's tee saw, **not** the decoded size §1 records; the two disagree under compression by exactly the compression ratio. |

---

## 6. Not metrics (disambiguation)

The exchange event emits structured **log fields**, not meters: `adapter_outcome`, `adapter_duration_ms`,
`adapter_name`, `adapter_response_status_code`, `adapter_url_template`, the header and body fields and the rest
of the family are `addKeyValue` entries on the `adapter-http-exchange` log event. `ResilientRestClient`'s own
transport lines carry `adapter_name` and `adapter_url_template` under the same names, so one dashboard field
groups both loggers. The applier's `resilience_layer` / `resilience_instance` / `resilience_event` fields are
log fields too.

Startup lines are the other non-metric source an operator reads next to these meters: the resolved mesh
ladder per endpoint (`MeshLadderLogger`), the two observation verdicts (§3), the Bean Validation summary, and
the startup guard's two policy lines naming the aliases the verdicts were installed on.

---

## 7. Differences to the reactive twin (`web-client/docs/metrics.md`)

| web-client | rest-client | Why |
|---|---|---|
| `reactor.netty.http.client.*` per-request transport meters, URI-templated by `WebClientMetrics.uriTagger` | **none** | Apache records no per-request meter here; `http.client.requests` (§3) carries the template from the stamped attribute, so a second cardinality guard would guard nothing. |
| `reactor.netty.connection.provider.*{name=webclient-reactor-netty}` (`total`, `active`, `idle`, `pending`, `max`, `max.pending`) | `restclient.pool.{leased,available,pending}{pool=restclient-apache}` | Apache's three live counts; no `max` gauge, no `max.pending` (unbounded in count). |
| `webclient.response.body.size{adapter}` | `restclient.response.body.size{adapter}` | Same meaning, same unit, this module's bounded reader instead of the codec. |
| `resilience4j.timelimiter.calls{kind=timeout}` = the reactive operator cancelled the chain | same name; = the caller was released and the worker interrupted | The blocking TimeLimiter needs a thread hand-off; the worker's exchange then ends as `Closed by interrupt` (§3). |
| exchange event `adapter_outcome=cancelled` | `adapter_outcome=cancelled` | The same outcome since legatium 1.3.0, except a Jetty call cancelled during its body (`failure`); `adapter.logging.events{outcome}` counts accordingly (§5). |

**If a dashboard or alert references `reactor_netty_*` for this module**, it is absent, not zero, unless a backend
runs on the `reactor-netty` engine (§2.1) — then under `name=restclient-reactor-netty`, which keeps it apart from
the reactive twin's pool. Point pool panels at `restclient.pool.*` for the Apache engine and latency panels at
`http.client.requests`.

**Related:** [rest-client — the blocking twin of web-client's `adapter/web`](restclient-vs-webclient.md).
