# rest-client — the blocking twin of web-client's `adapter/web`

> **Ownership & baseline.** Maintained by **Dirk Haase**. Verified against the code of 2026-10-19 (the module
> cut of ADR-0001). Where this text and the code disagree, the code wins.

`veredium-restclient` is `veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient` re-implemented on Spring's
blocking `RestClient` over Apache HttpClient 5 by default (the JDK client, Jetty and Reactor Netty are
selectable per backend, §3.1), with **no reactive dependency on a consumer's classpath** - Reactor Netty is an
optional dependency a host adds only to select that engine. Package
`eu.inqudium.veredium.restclient`. What is identical by construction between the two stacks - the mesh timeout
ladder, the breaker verdicts and the Resilience4j wiring that installs them - is not re-implemented at all:
both modules depend on `veredium-core` for it ([ADR-0001](../../docs/adr/ADR-0001-one-stack-neutral-core-module.md)).
The design, the names, the configuration surface and the test suite follow the reactive module one-to-one
wherever the blocking stack allows it; this document records what maps directly and what deliberately differs,
so the differences are decisions on record rather than surprises in production.

---

## 1. What maps one-to-one

| web-client (`adapter/web`)            | rest-client (`adapter/rest`)           | Notes |
|---------------------------------------|----------------------------------------|-------|
| `ResilientWebClient`                  | `ResilientRestClient`                  | Same per-call pipeline; `get`/`post` return `T` or throw, `getAsync`/`postAsync` return a `CompletableFuture<T>` (§2). |
| `ResilientWebClientFactory`           | `ResilientRestClientFactory`           | Same optional collaborators, same required `RestClient.Builder` bean from Boot, same startup lines. |
| `ReactiveResilienceDecorator`         | `BlockingResilienceDecorator` (`veredium-core`) | Same order `CircuitBreaker(TimeLimiter(Bulkhead(call)))`, see §2. The blocking one is a component of the core, usable without this client; this module declares its bean in `RestClientConfiguration`. |
| `WebClientVerdicts` / `WebClientConfiguration` | `RestClientVerdicts` / `RestClientConfiguration` | Each stack's reading of the shared verdicts (`veredium-core`): the status-carrying exception (`WebClientResponseException` / `RestClientResponseException`), the client-side failures (`DataBufferLimitException` / `ResponseBodyLimitExceededException` + `InterruptedException` + `RejectedExecutionException`), the guard's 404 probe; the config bean that imports the core's `MeshTimeoutConfig` (§8). |
| `WebClientProperties` (`app.webclient`) | `RestClientProperties` (`app.restclient`) | Same keys except `pool.pending-acquire-max-count` (§3). |
| `WebClientObservationReport` | `RestClientObservationReport` | Each a five-line subclass of the core's `HttpObservationReport` with the stack's name and toggle key; the two startup lines and their four verdicts are written once. |
| `ResponseValidation`, `ResponseValidationProperties` (`validation.*` of either properties tree) | the same classes (`veredium-core`) | The validator derived from the host's factory with its messages pinned to English, the per-alias toggle, the provider verdict - the webclient used to carry this inline in its factory. |
| `eu.inqudium.veredium.core.mesh`, `eu.inqudium.veredium.core.resilience` | the same classes | **Shared, not copied.** The ladder (`MeshProperties`, `ResolvedTimeouts`, `responseTimeout` and `response-timeout-margin-ms` on both stacks), the two policies, the customizers and the guard live once, in `veredium-core`. |
| the per-call lines, the size sample, `validate` (inline in `ResilientWebClient`) | the same (inline in `ResilientRestClient`) | **Shared, not copied** since the cut: `ClientCallLog` (`veredium-core`), held once per client; each stack passes its buffer-limit wording, its properties prefix and its `Content-Length` reader. `ResponseBodyMetrics` carries the UTF-8 size, the unit and the `adapter` tag; the meter NAMES stay per stack. |
| `BaseAdapter`                         | `BaseAdapter` (`eu.inqudium.veredium.core`) | **Shared, not copied.** The two budgets an adapter publishes, one contract for both stacks. |
| legatium-webclient-logging            | legatium-restclient-logging            | Same `adapter-logging.*` surface, same `adapter_*` fields, same attribute constants. |
| `WebClientMetrics`                    | `RestClientMetrics`                    | `restclient.response.body.size{adapter}`; no URI tagger (§4). |

The `mesh` and `resilience` packages were copies at first - a self-contained twin, with "extract at the third
stack" as the rule. That misread the Rule of Three: it guards against abstracting from one example, whereas here
two real, identical copies were maintained by one person and had started to diverge within a day of the copy.
They are one module now, `veredium-core`, and each stack keeps only its client, decorator, factory and its
verdicts object ([ADR-0001](../../docs/adr/ADR-0001-one-stack-neutral-core-module.md)).

The origin project's adapters and DTOs (`AccessProfileAdapter` and its siblings, the `agreement`/`party`/`profile`
DTOs) did not come along: veredium is the client infrastructure only, and a host writes its adapters over
`ResilientRestClient` or `ResilientWebClient` and implements `BaseAdapter`. The two client modules are
alternatives, not companions - each brings its own factory and configuration, and a host picks one stack.

---

## 2. The thread hand-off (`BlockingResilienceDecorator`)

A blocking call cannot time itself out: the thread that would notice the deadline is the one stuck in the socket
read. Resilience4j's blocking `TimeLimiter` therefore works on a `Future`: the call runs on another thread, the
caller waits on the future for at most the budget, and on expiry the caller is released with a `TimeoutException`
while the future is cancelled. That hand-off is **the** structural difference to the reactive twin, and it has
three consequences the decorator owns. The decorator itself lives in `veredium-core`
([ADR-0002](../../docs/adr/ADR-0002-blocking-resilience-decorator-as-a-core-component.md)): nothing in it is
specific to an HTTP exchange, so a host bounds a blocking call of its own with the same class; what this module
adds is its bean (`RestClientConfiguration`) with the worker name `restclient-call-`.

1. **The worker.** Every call runs on a fresh virtual thread (`restclient-call-N`). No pool to size, nothing that
   could become a second, undeclared bulkhead. The bulkhead permit is taken *on the worker*, so it still counts
   what is in flight at the backend. The executor is owned by the decorator and not injectable, so a host's own
   `ExecutorService` bean can never be picked up and shut down by a component that does not own it.
2. **The caller's context.** The worker starts with an empty MDC and no current observation. The decorator copies
   the caller's MDC map onto the worker for the duration of the call and reopens the caller's current observation
   there (when the host has an `ObservationRegistry`). Both are undone before the worker is released. This is why
   a caller's `CORRELATION_ID` reaches legatium's exchange event **without any host recipe** — on the reactive
   stack that was a documented gap closed by a `ThreadLocalAccessor` — and why the `http.client.requests`
   observation nests under the caller's span instead of starting a detached root. There is no JVM-wide hook: the
   twin's `WebClientReactorContextPropagationConfig` has no counterpart here.
3. **What cancellation does.** Cancelling the future interrupts the worker, and since JDK 13 a blocking socket
   read *does* react to that: the socket is closed and the read fails at once with
   `SocketException: Closed by interrupt` (connect and pool lease are interruptible too). The worker is released
   promptly in every phase, its bulkhead permit with it; the half-read connection is discarded. The caller is
   released at the budget regardless. That is the Apache engine; the other engines reach the same release by
   their own routes (§3.1).

A caller being interrupted (its own budget expired, its scope cancelled) is the blocking counterpart of a
subscriber cancelling: Resilience4j does not cancel the future in that case, so the decorator does, and rethrows
the `InterruptedException`, which `ResilientRestClient` logs as a cancellation at DEBUG.

The ladder is unchanged: the per-endpoint `responseTimeout` sits just below the TimeLimiter budget, so under
normal conditions the transport trips first (a `ResourceAccessException` over `SocketTimeoutException`, which the
breaker records and legatium classifies as `timeout`) and the TimeLimiter is the outer backstop over connect and
pending-acquire.

**The asynchronous form.** Where the twin's every call is a `Mono` the subscriber composes, this module's client
offers both: `get`/`post` block, `getAsync`/`postAsync` return a `CompletableFuture<T>` at once. The second is
not a second pipeline - it is the core decorator's `decorateAsync` (ADR-0002), which puts the wait on a second
virtual thread; deserialisation, validation and the log lines then run there, with the same verdicts. Every
outcome arrives on the future, unwrapped - a serialisation failure included, which the blocking form throws out
of `post` instead - and `cancel` on the future abandons the exchange exactly as interrupting a blocking caller
does (worker interrupted, permit released, the `Cancelled request` line at DEBUG). Only `cancel` does:
`orTimeout`/`completeOnTimeout` complete that one future and leave the exchange running, permit and connection
included, until it ends or the TimeLimiter budget expires - a caller wanting a shorter deadline cancels the
future itself. The origin module's asynchronous API was dropped at the port for want of a consumer
(architecture review 2026-10-19, finding 3); this one exists because the mechanism exists anyway and the client is the natural place to expose it.

---

## 3. The transport (Apache HttpClient 5 by default)

Apache's pool is the one blocking engine Spring supports that expresses everything the mesh transport config needs,
which is why it is the default (§3.1 for the others). The mapping, as the Apache engine of
`ResilientRestClientFactory` makes it:

| Reactor Netty (web-client)                     | Apache HttpClient 5 (rest-client)                                        |
|------------------------------------------------|--------------------------------------------------------------------------|
| one `ConnectionProvider` for all endpoints     | one `PoolingHttpClientConnectionManager`, shared by every per-endpoint client (`setConnectionManagerShared(true)`) |
| `maxConnections`                               | `maxConnTotal` **and** `maxConnPerRoute` — every endpoint is one route, and Apache's per-route default of 5 would otherwise cap each backend silently |
| `pendingAcquireTimeout`                        | `RequestConfig.connectionRequestTimeout`                                 |
| `pendingAcquireMaxCount`                       | **none** — Apache's pending queue is bounded in time only; the property was dropped rather than kept inert |
| `CONNECT_TIMEOUT_MILLIS`                       | `ConnectionConfig.connectTimeout`                                        |
| per-endpoint `responseTimeout`                 | per-client `RequestConfig.responseTimeout` (Apache applies it as the socket timeout of the request) |
| `maxIdleTime` / `maxLifeTime` / `evictInBackground` | `closeIdle` / `ConnectionConfig.timeToLive` + `closeExpired`, run by one scheduler the factory owns (Apache's own `evictIdleConnections` would start a thread per client); `evictInBackground: 0` = off, `maxLifeTime: 0` = unbounded |
| `compress(true)` / `keepAlive(true)`           | Apache's defaults (on)                                                   |
| `.metrics(true)` on the pool                   | Apache's `ConnPoolMeters` (`httpclient5-observation`; Micrometer's own hc5 binder is deprecated in its favour) → `restclient.pool.{leased,available,pending}{pool="restclient-apache"}` |
| `.metrics(true)` per request, URI-templated    | **none** — see §4                                                        |
| `disposeLater().timeout(...)` on shutdown      | bounded wait for `leased == 0` (`LockSupport.parkNanos`), then `close(GRACEFUL)`; same WARN on expiry |

Three Apache defaults are switched **off** because Reactor Netty does not have them and the ladder does not expect
them: automatic retries (the sidecar owns retries; an in-app second attempt would spend the ladder's budget twice),
redirect following, and the cookie store.

The factory replaces whatever request factory Boot put on the builder from `spring.http.client.*`: these clients
run on the ladder's timeouts and the shared pool, which the host's global settings know nothing about — the same
split the twin makes for its connector and codecs.

### 3.1 The selectable engines

`app.restclient.engine.default` and `app.restclient.engine.endpoints.<alias>` select `apache`, `jdk`, `jetty` or
`reactor-netty` - Spring's four blocking request factories over their engines. The selection is the module's own:
Boot's `spring.http.clients.imperative.factory` is neither read nor written, no engine bean is registered, and the
request factory goes onto the per-backend clone of Boot's builder only, so a host's other clients keep the engine
Boot gave them. The full mapping of the ladder and the pool settings per engine, what each cannot express, the
JDK's restricted Host header and how each engine abandons an interrupted exchange are in
[the guide's "Engines"](GUIDE.md#engines) and [timeouts, bulkheads and threads](timeouts-bulkheads-threads.md) §9.

Two points touch the twin. With `reactor-netty` the blocking client runs on the twin's transport library - but on
its own `ConnectionProvider` (`restclient-reactor-netty`) and event loops, never on Reactor Netty's global
`HttpResources` the twin's defaults would share, and with Reactor Netty's own pool gauge names in the host's
registry. And the twin's `pending-acquire-max-count` still has no key here: on `reactor-netty` the pending queue is
bounded at Reactor Netty's default of twice `max-connections`.

---

## 4. The buffer limit (`BoundedBodyReader`)

A blocking `RestClient` has no `maxInMemorySize`: its message converters read whatever the peer sends. The module
enforces the bound itself, at the one place every response passes through — the `exchange` function in
`ResilientRestClient` — so the ceiling means the same on both stacks: the **decoded** bytes buffered per
concurrently handled response (Apache decompresses transparently, so the count is after decompression, as the
codec counts).

- A **success** body that does not fit fails the call with `ResponseBodyLimitExceededException` (this module's
  type; the counterpart of `DataBufferLimitException`). It is a client-side failure for the breaker
  (`ClientSideFailurePolicy`) and produces the WARN line naming the limit and the declared
  `Content-Length`. Under gzip Apache strips that header, so the line then reads `Content-Length=unknown` — the
  lower-bound caveat the twin documents as well.
- An **error** body is truncated at the limit, never rejected, so a 5xx stays a 5xx for the breaker whatever size
  its error page has — the same boundary the twin's cause-walk test pins.
- `exchange` applies no status handlers, so the status decision is taken the way Spring's default handler takes it:
  a 4xx becomes `HttpClientErrorException.<Status>`, a 5xx `HttpServerErrorException.<Status>`, with status,
  headers, the (truncated) body and a message that previews it (capped at 200 characters; the whole body stays on
  the exception).

Successful bodies contribute their UTF-8 byte size to `restclient.response.body.size{adapter="<alias>"}`.

**No URI tagger.** The twin's `WebClientMetrics.uriTagger` exists for Reactor Netty's per-request transport
metrics, which would mint a series per path variable without it. Apache records no per-request meter here; the
per-request view is Spring's `http.client.requests` observation, which takes its `uri` tag from the template
attribute every client stamps on its requests (`ClientRequestLoggingInterceptor.URI_TEMPLATE_ATTRIBUTE`), so the
cardinality guard is the attribute and a second mapper would guard nothing.

---

## 5. The exchange event vocabulary

legatium-restclient-logging's interceptor rides on Boot's `RestClient.Builder` bean and writes one event per
exchange on the worker thread, under the caller's MDC. The vocabulary differs from the reactive twin's at one
point, pinned by `LegatiumExchangeLoggingIntegrationTest`:

| Situation                                | web-client event              | rest-client event                                          |
|------------------------------------------|-------------------------------|------------------------------------------------------------|
| 2xx / benign 4xx / 5xx                   | `success` / `rejected` / `failure` | same                                                  |
| transport response timeout               | `timeout` (WARN)              | `timeout` (WARN)                                           |
| caller cancels / TimeLimiter expires     | `cancelled` (WARN)            | **`failure` (ERROR)**, cause `SocketException: Closed by interrupt` on Apache (an I/O failure over the interrupt on the other engines), written promptly — there is no `cancelled` on the blocking stack |
| serialisation failure before the exchange | no event                      | no event                                                   |

An alert keyed on `adapter_outcome == "cancelled"` never fires for this module. The resilience verdict itself
(`TimeoutException`, `CallNotPermittedException`, `BulkheadFullException`) stays on `ResilientRestClient`'s own
transport line, as in the twin.

---

## 6. Configuration surface

```yaml
app:
  restclient:
    max-in-memory-size: 256KB        # bounded read ceiling (§4)
    pool:
      max-idle-time: 30s
      max-life-time: 5m              # 0 = unbounded
      evict-in-background: 30s       # 0 = off
    observability:
      http-observation-enabled: true # ON in the reference config (code default false)
    validation:
      enabled: true                  # ON, report only, in the reference config (code defaults: off / fail)
      fail-on-violation: false
      endpoints: {}
    # user-agent: my-service/2.3     # the host's own token instead of veredium/<version>
    engine:
      default: apache                # apache | jdk | jetty | reactor-netty (§3.1)
      endpoints: {}                  # <mesh-alias>: <engine>
  mesh: ...                          # the shared tree, see veredium-core/docs/mesh-timeout.md
adapter-logging: ...                 # unchanged, read by legatium-restclient-logging
logging:
  level:
    eu.inqudium.legatium.restclient.logging: WARN
```

Everything under `app.mesh`, `resilience4j.*` and `adapter-logging.*` reads exactly as in
`web-client/docs/webclient-config-guide.md`; only the `app.webclient` block becomes `app.restclient`, minus
`pool.pending-acquire-max-count`.

---

## 7. Test suite

The suite mirrors the twin's class for class where the blocking stack allows it. Classes specific to this
module:

- `BlockingResilienceDecoratorTest` — the hand-off: virtual worker, MDC and observation propagation, exception
  unwrapping, bulkhead permit on the worker, TimeLimiter release + worker interrupt, caller interruption,
  short-circuit without a worker.
- `BoundedBodyReaderTest` — both readings of the limit, the inclusive boundary, and that an oversized body is not
  drained into memory - each also through short and zero-length reads and across the reader's buffer growth;
  `BoundedBodyReaderFuzzTest` (Jazzer, `src/test/java`) explores the same invariants with arbitrary bodies, limits,
  declared lengths and read patterns.
- `RestClientEngineIntegrationTest` — every engine against WireMock: GET/POST, the status mapping, the response
  timeout and its cause type, the buffer limit, the Host and User-Agent headers, the four ways an exchange is
  abandoned (head and body phase of a TimeLimiter expiry, the caller's interrupt, the cancelled future - permit and
  connection back promptly), the lifecycle (created only when selected, shared, closed), the per-backend override,
  the fail-fast checks and the startup lines; `RestClientEngineIsolationTest` — in a real context, that the
  selection does not reach a host client built from Boot's builder bean, that Boot's global switch does not reach
  the module's clients, and that no engine bean is registered.
- `RestClientVerdictsTest` — the verdicts through the real decorator and against this stack's exception shapes
  (Spring's status-specific subclasses, the `ResourceAccessException` wrapper, the four client-side failures of
  the hand-off); `MeshResilienceShippedConfigTest` — this module's reference configuration booted with the
  core's wiring and these verdicts. The mechanism itself (the ladder, the customizers, the guard, both
  directions of its failure) is pinned once, in `veredium-core`'s test suite, with a stand-in stack; the
  reactive twin carries the same two classes (`WebClientVerdictsTest`, `MeshResilienceShippedConfigTest`).

The origin project's adapter tests did not come along: veredium ships no adapters, so there is nothing for an
`AdapterIntegrationTest` to test. The response contract those tests pinned per adapter - deserialisation,
validation in both postures, the empty body - is pinned once on the client, in `ResilientRestClientIntegrationTest`.
What was ported are the configuration contract tests, bound to both the reference configuration and the fixture
(`ConfigurationFiles`): `LoggingLevelContractTest` (the exchange logger's level line against its name) and
`ResilienceConfigContractTest` (every mesh alias declared on all three Resilience4j layers, and
`sum(max-concurrent-calls) <= max-connections`). That a host's own configuration keeps the second invariant is
checked at startup instead, where both numbers are known: `MeshResilienceStartupGuard` WARNs when it does not.

**No Helm consistency test.** The origin project carried a `HelmTwinConsistencyTest` with a vendored copy of the
`helm-mesh-timeouts` chart, shelling out to the `helm` binary to compare the chart's derived sidecar `global`
with the Kotlin ladder. Neither veredium module carries it: a vendored copy can only ever prove agreement with
itself, never with the chart that is deployed. The derived numbers of the shared ladder are pinned by `veredium-core`'s `MeshPropertiesDerivationTest`.

---

## 8. Resilience wiring: customizers, not amendments - shared, not copied

The twin used to install its breaker verdicts and the mesh-derived rungs with a home-grown amendment framework:
three contributors filled a plan, an applier folded it per instance in `afterSingletonsInstantiated`, removed
and recreated each Resilience4j instance, verified the result by identity and reported an ERROR event when an
alias was unknown. It had rejected Resilience4j's own extension point because nothing there booted the
auto-configuration, so a customizer's effect could not be observed in its tests.

This module used that extension point from the start, and since the module cut of ADR-0001 both stacks do, from
one implementation in `veredium-core`: `MeshResilienceCustomizers.kt` registers one `CircuitBreakerConfigCustomizer`
per mesh alias and one `TimeLimiterConfigCustomizer` per alias whose time-limiter derivation is on (a Spring 7
`BeanRegistrar`, imported by the core's `MeshTimeoutConfig`, because the number of beans follows the endpoint
list). Resilience4j's Boot configuration consults them as the last step of building each `instances.<alias>`
entry — after the named base config and the instance's own YAML, before the instance exists — so the binders
bind to the final instance and nothing is rebuilt later. The verdicts are folded onto what YAML produced (ignore
OR-ed, record AND-ed), so an operator's own lists survive.

What the stack contributes is one bean, `BreakerVerdicts` (`RestClientVerdicts` here, `WebClientVerdicts` in the
twin): how to read a response status off this stack's exception, which exception types this stack raises
itself, and a 404 in the shape this stack throws it as the guard's probe. `RestClientConfiguration` declares the
bean and `@Import`s `MeshTimeoutConfig`, so a host that scans this module's package gets the core wiring too.
The RULES — a benign 4xx except 408/429 is a success, a client-side failure is ignored, the cause chain is
walked to a bounded depth — are written once and cannot drift between the stacks any more.

What the twin's applier reported as an ERROR is a failed start on both stacks. An alias missing from
`resilience4j.<layer>.instances` does not go unserved — Resilience4j creates an instance for every customizer
name it finds no declaration for — but it is built on the registry default, not on the named base config
(`mesh-backend`), so the operator's policy is silently lost (and a bulkhead, which has no customizer, is created
on the first call). `MeshResilienceStartupGuard` (`SmartInitializingSingleton`, `veredium-core`) checks for every
mesh alias that all three layers DECLARE it (read off the bound `resilience4j.*` property beans, not the
registry) and that the customizer's effect is observable on the instance the registry serves, and fails the
context naming the key to add. The twin's reference configuration once declared a single TimeLimiter instance
while mesh derives four; it declares all four now, and each module's `MeshResilienceShippedConfigTest` boots the
three auto-configurations against its own file.

The consequence for consumers of `BaseAdapter`: the budget an adapter publishes is final as soon as the registry
bean exists; reading it on access is still the right shape (the registry owns it), but no longer an
init-order requirement.
