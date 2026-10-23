# WebClient configuration guide

> **Ownership & baseline.** Maintained by **Dirk Haase**. Verified against the code at commit `589d3890` (2026-07-30)
> Where this text and the code disagree, the code
> wins.

This guide is the complete reference for configuring the reactive WebClient adapter layer (`adapter/web`). It
covers **every** knob the module reads, grouped by the four configuration surfaces it uses:

| Surface | Owner | What it configures |
|---|---|---|
| `app.webclient` | `WebClientProperties` | Global client knobs: buffer limit, connection-pool lifecycle, HTTP observation, **response validation**, the **HTTP engine** per backend |
| `app.mesh` | `MeshProperties` | The per-endpoint tree: target, path, and the whole timeout ladder |
| `resilience4j.*` | `resilience4j-spring-boot4` | The three R4j primitives (Bulkhead, CircuitBreaker, TimeLimiter) per backend |
| `adapter-logging.*` / `logging.level` | `legatium-webclient-logging` / Spring | The outbound exchange log: one event per exchange, its headers, bodies and cap, and the level of its stream |
| `management.observations.*` | Micrometer | The central observation disable-filter |

Every value shown is the code default unless it is marked as the **reference configuration**'s - the
documented example `src/test/resources/application.yml` of this module, a test resource the contract tests pin
(a library ships no `application.yml`; a host writes its own). The two module property classes validate at **binding time** — a value
that cannot be meant is rejected where it is set, with a message naming the property key, because a wrong number
here does not announce itself: it binds, the context starts, and the effect surfaces as a runtime symptom that
points anywhere but at the configuration.

> **Where the per-endpoint config lives.** There is no `app.webclient.targets` tree. A backend's base URL, Host
> header, path template and timeout ladder all come from its **one** `app.mesh.endpoints[]` entry (see
> [mesh-timeout.md](../../veredium-core/docs/mesh-timeout.md) for the derivation rationale). `app.webclient` keeps only the truly global
> knobs, plus response validation — which is per-backend but does not derive from the ladder.

---

## 1. `app.webclient` — global client knobs (`WebClientProperties`)

```yaml
app:
  webclient:
    max-in-memory-size: 256KB        # buffered-body ceiling
    pool:                            # shared connection-pool lifecycle
      pending-acquire-max-count: 100
      max-idle-time: 30s
      max-life-time: 5m
      evict-in-background: 30s
    observability:
      http-observation-enabled: true   # reference ON (code default false) - see §1.3
    validation:                      # response Bean Validation (see §1.4)
      enabled: true                  # reference ON, report only (code defaults: off / fail)
      fail-on-violation: false
      endpoints: {}
    # user-agent: my-service/2.3     # the host's own token instead of veredium/<version> (see §1.5)
    engine:                          # the HTTP engine (see §1.6)
      default: reactor-netty         # reactor-netty | jdk | jetty | apache
      endpoints: {}                  # per-alias overrides
```

> **Reference versus code default.** Two keys above are set in the reference configuration to a value that
> contradicts their code default, and `LegatiumExchangeLoggingIntegrationTest` pins both against it: the
> observation toggle is **on** there (§1.3), and response validation is **on in report-only mode** (§1.4) -
> every backend checked, violations logged at WARN, no call failed. A host that omits the keys gets the code
> defaults. Everything else shown is the code default.

### 1.1 `max-in-memory-size` (`DataSize`, default `256KB`)

The upper bound on how much of a response body the codec will buffer. It bounds the *exchange*, not the log: how
much of a body reaches the exchange event is `adapter-logging.max-body-bytes` (§4.1), an independent number.
(Until ticket 30 the module's own diary derived its capture bound from this value; the library has its own.)

256 KB is Spring's standard `ExchangeStrategies` ceiling. The previous 8 KB value was too small for a real
access-profile list and deterministically rejected the otherwise successful response. At the shared pool's
default maximum of 100 concurrent connections, the new ceiling bounds simultaneous response buffers at roughly
25 MB.

- **Guard:** must be in `1 B .. Int.MAX_VALUE B`. Both ends are load-bearing:
  - *Zero or negative* binds silently and then fails **every** call with a `DataBufferLimitException` — which the
    CircuitBreaker deliberately ignores (§3.3), so no breaker opens and monitoring reads "all backends down"
    while the cause is one line of YAML.
  - *Past `Int.MAX_VALUE`* would wrap to a negative `Int` when the factory narrows it for the codec, landing back
    at the zero case by a route nobody would suspect.
- A response that exceeds the limit fails the call with a `DataBufferLimitException`, wrapped by Spring into a
  `WebClientResponseException` reporting **200 OK** — a client-side refusal (the backend answered correctly), so
  it does not count towards the circuit breaker.
- It stays a **module** property and wins over Boot's `spring.http.codecs.max-in-memory-size` for these clients:
  the factory clones Boot's auto-configured `WebClient.Builder` (§1.3) and applies this limit to the clone
  *after* Boot's codec customizer ran, so the host's global ceiling governs the host's own WebClients and this one
  governs the mesh clients — deliberately, because the number is sized against this module's pool (above).
- Successful decoded bodies contribute their UTF-8 byte size to
  `webclient.response.body.size{adapter="<mesh alias>"}`. Bodies that exceed the ceiling cannot be sampled because
  decoding aborts first; their WARN line therefore reports the configured limit and `Content-Length` when the
  backend supplied it.

### 1.2 `pool.*` — the connection pool (`WebClientProperties.Pool`)

One Reactor-Netty `ConnectionProvider` serves every endpoint's `HttpClient`, but it does **not** make them share
connections: Reactor Netty keeps a separate sub-pool per remote address *and* per client configuration, and each
endpoint's `HttpClient` is configured on its own (its response timeout, its URI tagger). On `reactor-netty` the
pool limits (`max-connections`, the pending-acquire limits) therefore apply **per backend**; two backends behind
the same sidecar never share a connection (`WebClientEngineIntegrationTest` pins this). The other engines
(§1.6) share one pool between their backends, per destination. Per-backend isolation is the R4j bulkhead's job
on every engine. The keys are written in Reactor Netty's terms, the default engine; another engine applies
what it has an equivalent for - the table in §1.6 says which.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `pending-acquire-max-count` | `Int` | `100` | Max callers queued waiting for a connection. `-1` = unbounded, `0` = never queue (fail fast on exhaustion). |
| `max-idle-time` | `Duration` | `30s` | Evict a connection idle for longer than this. Must be positive. |
| `max-life-time` | `Duration` | `5m` | Evict a connection older than this regardless of use. |
| `evict-in-background` | `Duration` | `30s` | Background eviction interval. `0` = off (Reactor Netty's documented value). |

- **Guards:** `pending-acquire-max-count >= -1` (below `-1` means nothing); the three durations must **not be
  negative** (a negative duration is accepted by the pool builder and then quietly means "evict the moment it is
  created"). Zero is allowed for `max-life-time` (unbounded) and `evict-in-background` (off), but **not** for
  `max-idle-time`: the engines read a zero idle time in opposite ways - Reactor Netty evicts every released
  connection (no keep-alive at all, a new connection per call), Jetty never closes an idle connection, Apache's
  evictor closes all of them on every sweep - so there is no one meaning to give it, and binding refuses it.
- **Note the split:** `max-connections` and `pending-acquire-timeout` are **not** here — they belong to the mesh
  transport (`app.mesh.defaults.transport`, §2.5), because they are part of the timeout ladder's accounting. This
  `Pool` block owns only the lifecycle knobs that have no mesh equivalent.

### 1.3 `observability.http-observation-enabled` (`Boolean`, default `false`)

Toggles Spring WebClient's native Micrometer observation on the outbound calls **of this module**. The factory
clones Boot's auto-configured `WebClient.Builder` bean, so everything the host puts on that builder reaches these
clients too — its `WebClientCustomizer` beans, its observation convention, Boot's codec settings, and the
`ObservationRegistry` that Boot's `ObservationWebClientCustomizer` wires. The factory does **not** wire the registry
itself; this flag decides whether it *keeps* it.

- **On:** Boot's registry stays on the builder and each request opens an `http.client.requests` observation. With
  a tracing bridge on the host classpath that becomes a **CLIENT span** and injects the W3C `traceparent` into the
  outgoing request; with only a metrics registry it records the client timer; with no `ObservationRegistry` bean
  at all Boot wires nothing and it is a NOOP.
- **Off:** the factory replaces the registry on its clone with `ObservationRegistry.NOOP`, so these clients open
  no observation while every other WebClient of the host keeps its own. That is the difference to the central
  filter in §4.2, which acts on the registry and therefore on *all* WebClients of the host.
- **The builder bean is required.** `ResilientWebClientFactory` takes `WebClient.Builder` as a required
  constructor parameter — no default, not nullable — so a host that excludes `WebClientAutoConfiguration` fails
  at startup with a message naming `WebClient.Builder` instead of running on a bare builder without a span. The
  module brings `spring-boot-starter-webclient` itself, so the auto-configuration is on the classpath whether or
  not the host declares that starter; a host that excludes it declares its own `WebClient.Builder` bean.
- Enabling it also flips a **JVM-wide** hook: `WebClientReactorContextPropagationConfig` calls
  `Hooks.enableAutomaticContextPropagation()` so the active observation survives Reactor thread hops. This lives
  in its own `@Configuration` (not the factory constructor) precisely because the effect is process-wide and
  cannot be undone — it must be deliberate wiring, gated on this toggle, not a side effect of constructing a
  factory (which a test may do repeatedly). Both branches are pinned by
  `WebClientReactorContextPropagationConfigTest` (a ThreadLocal carried across a thread hop with the toggle on,
  nothing changed with it off).
- Self-contained: it resolves from this module's own property and references nothing from sync-bridge.
- See also §4.2 for the **central** disable-filter that can suppress the observation even when this is on.

### 1.4 `validation.*` — response Bean Validation (`ResponseValidationProperties`, shared with the blocking client in `veredium-core`)

Whether a deserialised response DTO is checked against its Bean Validation constraints. **Off by default**,
because the constraints are generated from the published OpenAPI specs and those specs are demonstrably stricter
than reality in places — `ProductAgreementsDataResponseV8DTO` declares `@Size(min = 1)` on a result list a lookup
may legitimately return empty; `AccessProfileAdapter` documents a second, unrelated defect (incident
INC15288523). Validation is therefore adopted one backend at a time, against real traffic.

| Key | Type | Default | Scope | Meaning |
|---|---|---|---|---|
| `enabled` | `Boolean` | `false` | shared default | Whether responses validate when no per-alias override applies. |
| `endpoints` | `Map<String, Boolean>` | `{}` | per-backend | Per-**alias** overrides; an alias not listed inherits `enabled`. |
| `fail-on-violation` | `Boolean` | `true` | **shared only** | Whether a violation **fails** the call or is only **reported**. |

- **`endpoints` is keyed by the derived alias** (the same `host-METHOD-path` name the R4j instances use, §2.2),
  **not** by the triplet. A per-alias entry wins in **both** directions — it can switch one backend on above an
  off default, or off above an on default. A misspelt key matches no backend and silently falls back to the
  default, so keep it in sync with the derived alias (the startup log prints the effective set).
- **`fail-on-violation`** is deliberately shared, not per-endpoint: `true` throws a
  `ConstraintViolationException` carrying every violation (logged at WARN with its cause: it is not an "expected
  client error" of `HttpExchangeErrorPolicy`, so it keeps full severity); `false` logs the same detail at WARN and returns the response
  **unchanged**, so a newly-enabled backend can be watched against real traffic before enforcing. Report-only is
  a rollout posture for the whole adopting wave; the per-alias choice is only *which* backends validate.
- The check runs **outside** the Resilience4j decoration (like deserialisation): a response that violates its own
  schema is a client-side contract problem, not an outage, so it must not open the CircuitBreaker — the backend
  answered, promptly and with a 200. The factory resolves the toggle **once** when it builds each client (the
  `Validator` is `null` when off), so the hot path holds no flag to consult and cannot disagree with the
  configuration mid-flight.
- **Provider:** validation needs a Bean Validation provider on the classpath. The module brings
  `spring-boot-starter-validation` (Hibernate Validator) at compile scope, so the host's auto-configured
  `ValidatorFactory` is injected; the factory pins constraint messages to **English** (via a message
  interpolator) so a violation reads the same on every host regardless of the JVM default locale. If no provider
  is present the startup log says so — validation then cannot run for any backend, whatever the properties say.

Two worked examples:

```yaml
# (a) Adopt validation for a single backend, everything else off
app:
  webclient:
    validation:
      enabled: false                                       # shared default (off)
      endpoints:
        apis-example-com-GET-v6-involved-parties: true         # only THIS backend validates

# (b) Validate everywhere but exempt one endpoint whose spec is wrong, report-only during rollout
app:
  webclient:
    validation:
      enabled: true                                        # shared default (on)
      fail-on-violation: false                             # report only, do not fail the call yet
      endpoints:
        apis-example-com-POST-v8-agreements-products-lookup: false
```

---

### 1.5 `user-agent` (`String?`, default `null` = the library's token)

The `User-Agent` every client of the factory sends. Unset, it is the library's own token, `veredium/<version>`
(`Veredium.userAgent()`, the version read from the jar manifest; the bare `veredium` outside a jar) - so a
peer's access log names what called it rather than Reactor Netty's default. A host that is read per caller in
those logs names itself instead: `user-agent: my-service/2.3` replaces the token verbatim, trimmed. An
endpoint's static `headers` under `app.mesh` still win over either. Blank, or carrying a control character,
is rejected at bind time naming the key (`Veredium.resolveUserAgent`), because the transport would otherwise
refuse the header per request with a message that points nowhere near the configuration. The blocking client
binds the same key under `app.restclient.user-agent`.

### 1.6 `engine.*` — the HTTP engine (`WebClientProperties.Engine`, `WebClientEngine`)

Which library carries a backend's exchanges. A shared default with per-alias overrides, the same shape as
`validation.*` (§1.4). What each engine does on its own, and what the module does about it, is collected in
[HTTP engine behaviour](../../docs/engine-behaviour.md).

| Key | Type | Default | Meaning |
|---|---|---|---|
| `default` | `WebClientEngine` | `reactor-netty` | The engine of every alias without an override. |
| `endpoints` | `Map<String, WebClientEngine>` | `{}` | Per-**alias** overrides (the derived alias of §2.2). |

The values are `reactor-netty`, `jdk`, `jetty` and `apache`; each maps onto one of Spring's reactive connectors:

| Value | Spring connector | Library | Dependency the host adds |
|---|---|---|---|
| `reactor-netty` | `ReactorClientHttpConnector` | Reactor Netty | none - `spring-boot-starter-webflux` brings it |
| `jdk` | `JdkClientHttpConnector` | `java.net.http.HttpClient` | none - part of the JDK; **plus the JVM flag below** |
| `jetty` | `JettyClientHttpConnector` | Jetty `HttpClient` 12 | `org.eclipse.jetty:jetty-client` and `org.eclipse.jetty:jetty-reactive-httpclient` |
| `apache` | `HttpComponentsClientHttpConnector` | Apache HttpClient 5, async | `org.apache.httpcomponents.client5:httpclient5` and `org.apache.httpcomponents.core5:httpcore5-reactive` |

The three non-default libraries are `optional` dependencies of this module: a host does not inherit them and
declares the one it selects; the Spring Boot BOM manages their versions.

```yaml
app:
  webclient:
    engine:
      default: reactor-netty
      endpoints:
        apis-example-com-GET-v6-involved-parties: jetty   # move ONE backend, e.g. to try an engine
```

**Scoped to this module - the isolation guarantee.** Boot has a switch of its own,
`spring.http.clients.reactive.connector`, and it is global: it decides the connector Boot puts on its
`WebClient.Builder` bean, and with it the engine of **every** WebClient the host builds from that bean. Using it
to move the mesh clients would move all the others too. So this module never reads or writes Boot's switch (nor
any other `spring.http.clients.*` key), registers no `ClientHttpConnector`, HTTP-client, connector-builder or
settings bean, does not use the host's `ReactorResourceFactory` bean, and leaves Boot's builder bean as it found
it: the factory builds the connector itself and sets it on **its clone** of the builder only. The reverse holds
as well - a host that sets Boot's switch moves its own WebClients and not these. `WebClientEngineIsolationTest`
pins both directions in a context with Boot's HTTP-client auto-configuration, reading the engine of the host's
calls off the wire (the `User-Agent` each engine sends on its own).

The engine resources belong to the factory: created when the first client of an engine is built (an engine no
backend selected is never created - not even Reactor Netty's pool), shared by that engine's backends, and
released in `shutdown()`. That includes Reactor Netty: its pool, its event loops (threads named after the pool,
`webclient-reactor-netty-...`) and its resolver (Netty's `InetAddress`-based `DefaultAddressResolverGroup`, set explicitly
because Reactor Netty's default resolver comes from the global resources) are the factory's, not the
process-wide `HttpResources` every other Reactor Netty client of the JVM shares and Boot's `ReactorResourceFactory`
manages. Until the engine switch existed the default engine ran on those global loops and that global resolver;
this is the one change the default wiring received. The loops are disposed after the pool drain bound has
passed rather than with the pool, because closing a loop group closes every channel on it - including those of
the in-flight calls the pool disposal deliberately lets finish (see "Shutdown" below).

**What each engine applies.** The mesh transport (§2.5), the per-endpoint response timeout of the ladder (§2.7)
and the `pool.*` knobs (§1.2) map onto every engine as far as it has an equivalent. Where it has none, the
setting does **not** apply - and the factory says so in one INFO line per engine in use at startup
(`HTTP engine 'jdk' (...) for [...]: applies ...; does NOT apply ...; transport meters ...; cancellation ...`),
next to a line naming the engine of every backend in alias order
(`HTTP engine per backend (app.webclient.engine, default reactor-netty): a=jdk, b=reactor-netty`).

| Setting | `reactor-netty` | `jdk` | `jetty` | `apache` |
|---|---|---|---|---|
| `transport.connect-timeout-ms` | `CONNECT_TIMEOUT_MILLIS` | client `connectTimeout` | client `connectTimeout` | `ConnectionConfig.connectTimeout` |
| response timeout (per endpoint, §2.7) | `responseTimeout` - read inactivity | `HttpRequest.timeout` - until the response headers arrive | per-request `idleTimeout` - inactivity | `RequestConfig.responseTimeout` - socket inactivity |
| `transport.max-connections` | per backend (sub-pool per remote address and client configuration) | **not applied** | per destination | per route |
| `transport.pending-acquire-timeout-ms` | pending-acquire timeout | **not applied** | **not applied** (no queue timeout) | **not applied by itself**: set as the connection-request timeout, but Apache's async pool checks a waiting lease's deadline only when the pool is next used, so the TimeLimiter bounds the wait |
| `pool.pending-acquire-max-count` | pending-acquire queue | **not applied** | requests waiting beyond `max-connections` per destination: Jetty's queue capacity is `max-connections` + the count, because Jetty keeps every request in its queue until it is on a connection (`-1` = unbounded; `0` is rejected at startup - no capacity means "none wait") | **not applied** (no bound) |
| `pool.max-idle-time` | max idle time | **not applied** | connection idle timeout | idle-connection eviction (only with `evict-in-background` > 0) |
| `pool.max-life-time` | max life time | **not applied** | pool `maxDuration` | connection time-to-live |
| `pool.evict-in-background` | background eviction | **not applied** | **not applied** (idle connections close on their own timer) | interval of the idle-connection evictor |
| compression (`gzip`) | yes | **no** (no content decoding) | yes | yes |

Notes on the table:

- The JDK client has no per-client pool settings at all; its pool is tuned only by JVM-wide system properties
  (`jdk.httpclient.connectionPoolSize`, `jdk.httpclient.keepalive.timeout`), which this module does not set -
  they would change every other `java.net.http` client of the process.
- Every non-default engine is aligned with Reactor Netty's defaults where they differ: HTTP/1.1 only (the JDK
  client would otherwise offer an `h2c` upgrade on plaintext, Apache would negotiate HTTP/2 over TLS), no
  redirects followed, no cookies stored (Jetty, Apache), and for Apache no automatic retries - an invisible
  retry would spend the mesh budget behind the TimeLimiter's back. Apache's I/O reactor checks its timeouts
  every 50 ms instead of every second, so its response timeout expires within the headroom below the
  TimeLimiter rung.
- The response-timeout semantics differ in detail - three engines bound inactivity, the JDK client bounds the
  wait for the headers - but on every engine its expiry is a `WebClientRequestException` before the
  TimeLimiter rung, which the breaker records as a transport failure (`WebClientEngineIntegrationTest`).

**Meters per engine.** The per-request transport meters with the templated `uri` tag
(`reactor.netty.http.client.*`) and the pool gauges (`reactor.netty.connection.provider.*`) exist on
`reactor-netty` only ([metrics.md](metrics.md) §2). The `jdk`, `jetty` and `apache` engines expose no transport
meter; what every engine has is the `http.client.requests` observation (§1.3), the Resilience4j meters and
`webclient.response.body.size`.

**Engine-agnostic.** Everything above the connector is the same on every engine, because it lives on the
cloned builder or in the client, not in the transport. `WebClientEngineIntegrationTest` pins per engine: the
codec limit (§1.1, with its `DataBufferLimitException` verdict), the status mapping, the empty-body contract
(§3.4), the Host, User-Agent and Accept headers, and that the builder's filters (legatium's exchange filter is
one) and its observation registry still run - the filter sees the backend name, the `http.client.requests`
timer is recorded. Reactor's context propagation (§1.3) is a property of the Reactor pipeline above the
connector and is not tested per engine.

**The `jdk` engine and the Host header.** Every client of this module sends an explicit `Host` header, from
the triplet (§2.1). The JDK client refuses that header unless the JVM was started with

```
-Djdk.httpclient.allowRestrictedHeaders=host
```

The module does **not** set this property: it is JVM-wide and would lift the restriction for every other
`java.net.http` client of the process, which is exactly what the isolation guarantee rules out - and the JDK
reads it only once, when its HTTP classes initialise. A host that selects `jdk` passes the flag at JVM start;
without it the factory refuses to start, naming the key, the backends on `jdk` and the flag. The check asks the
JDK itself, so a property set programmatically after the JDK client initialised is reported as missing -
correctly, since the client ignores it. (`JdkEngineRestrictedHostHeaderTest` pins this in a JVM of its own.)

**Cancellation.** The TimeLimiter cancels the subscription when the budget runs out; on every engine that
aborts the exchange and closes its connection promptly - well before the response timeout - and gives the
bulkhead permit back (`WebClientEngineIntegrationTest`, against a backend that never answers). Reactor Netty and
Jetty (`abortOnCancel`) do it themselves, the JDK client through the cancelled `CompletableFuture`. Spring's
Apache connector drops the `Future` of the exchange it starts, so a cancellation alone would leave the exchange
running until the response timeout; the module keeps that `Future` per exchange and cancels it - while waiting
for the response and while reading the body. Some engines report the aborted exchange as a failure after the
cancellation; with no one left to receive it, Reactor would log it at ERROR as a dropped error, so the client
writes it at DEBUG instead (`Exchange of backend '...' ended after cancellation: ...`). Only that case: an error
an operator drops while the exchange was *not* cancelled still goes to the caller's own `onErrorDropped` hook
or to Reactor's global handling.

**Shutdown.** `shutdown()` releases each engine in use against ONE deadline (the longest response timeout of
any endpoint + connect + pending-acquire + 1 s from the start of the shutdown), shared by the engines released
one after another - so a factory on several engines does not take the bound once per engine. Every engine lets
its calls in flight finish first, those still waiting for a connection included: Reactor Netty's pool is disposed
gracefully (its `disposeTimeout` is the bound) - it refuses new calls, serves the waiting ones and completes once
every connection came back, with a WARN when the deadline ran out first - and its event loops are disposed
once the bound has passed (daemon threads, so an exiting JVM is not held up); the JDK client shuts down in order -
in-flight exchanges run to completion until the deadline, then it is shut down hard with a WARN; Jetty's client
waits for its in-flight exchanges until the deadline and then stops, with a WARN naming those it aborts; Apache waits for its exchanges in flight until the deadline, then its I/O reactor shuts down gracefully and is
closed, with a WARN when the deadline ran out - and the exchanges it still had in flight are cancelled (WARN
with their number), because closing the reactor completes none of their callers, who would otherwise wait out
their budget. After `shutdown()` no new client can be built (`IllegalStateException`): it would create engine
resources nothing releases.

**Fail-fast** (§5): an override key that names no mesh alias, a selected engine whose library is missing (the
message names the coordinates), `jdk` without the Host flag, and `jetty` with `pool.pending-acquire-max-count: 0`.
Only the engines in use are checked - a missing Apache library does not matter to a host that never selects it.

## 2. `app.mesh` — the per-endpoint tree (`MeshProperties`)

`app.mesh` is **REQUIRED**: `ResilientWebClientFactory` injects `MeshProperties`, and a backend with no matching
mesh endpoint fails fast at first use rather than surfacing as a confusing 404/connection error. The tree has two
parts:

```yaml
app:
  mesh:
    defaults: { ... }        # shared fallbacks + policy knobs (optional; omit to accept the Kotlin defaults)
    endpoints:               # one entry per backend operation (REQUIRED)
      - triplet: "..."
        individual-request-timeout-ms: 400
```

### 2.1 The triplet — the canonical endpoint id

Each endpoint is one **operation**, identified by a `triplet`:

```
/endpoint/<host>/<METHOD>/<path...>
```

- **Format guard:** at least four parts and `parts[0] == "endpoint"`. Three parts would pass every other check
  and produce a bare `/` path template (matching any URI) plus a `root` alias — plausible-looking consequences of
  a typo whose first sign would be a 404 pointing nowhere near the config.
- From the triplet the module derives:
  - the **Host header** = `<host>` (routes the sidecar to the upstream API),
  - the **path template** = `/<path...>` (keeps `{vars}`; the adapter builds its request URI from it, and
    `WebClientMetrics` derives low-cardinality metric tags from it),
  - the **alias** (see §2.2), unless an explicit `alias:` is set.

### 2.2 The alias — the logical backend name

The `alias` is the name adapters pass to `ResilientWebClientFactory.create(backendName)`; it is also the R4j
instance key, the metric label, and the `validation.endpoints` map key (§1.4). The reference configuration sets **no
explicit alias** — it is **derived from the triplet**:

> host (lowercased, dots → hyphens) · UPPERCASE method · static path segments in order (`{placeholders}`
> dropped), joined by `-`.

Example: `/endpoint/api.example.com/GET/v5/access-profiles/by-involved-party/{id}` →
`api-example-com-GET-v5-access-profiles-by-involved-party`.

- The host prefix means the same method+path on different hosts cannot collide.
- Aliases (and triplets) are **unique-checked at binding** (`MeshProperties.init`) — a duplicate fails fast.
- A host's adapters must pass the derived value to `ResilientWebClientFactory.create` (an unknown alias fails
  there, §5), and the `resilience4j.*.instances.<alias>` keys must match it under all three layers - checked
  for this module's reference configuration and test fixture by `ResilienceConfigContractTest`, and for a
  host's configuration at startup by `MeshResilienceStartupGuard` (§3.2).
- An explicit `alias:` is still supported and wins over the derivation — but changing an alias renames every
  metric label derived from it, so keep it stable.

### 2.3 `endpoints[]` fields (`EndpointConfig`)

| Key | Type | Default | Meaning |
|---|---|---|---|
| `triplet` | `String` | **required** | The canonical id (§2.1). |
| `alias` | `String?` | `null` → derived | Explicit override of the derived alias (§2.2). |
| `individual-request-timeout-ms` | `Long?` | `null` → `defaults` | The measured per-endpoint anchor `p99(single attempt) + margin`. |
| `max-retries` | `Int?` | `null` → `defaults` | Mirrors the sidecar's max-retries; folds into `global`. |
| `base-url` | `String?` | `null` → `transport.sidecar-base-url` | Per-endpoint WebClient base URL (the sidecar). |
| `headers` | `Map<String,String>` | `{}` | Extra static headers (the Host header is derived, not set here). |
| `margins` | `MarginsOverride?` | `null` | **Partial** override, field-merged onto `defaults.margins` (§2.4). |
| `time-limiter` | `{enabled: Boolean?}` | `null` → inherit | Whether mesh derives this endpoint's TimeLimiter (§2.6). |
| `circuit-breaker` | `{enabled: Boolean?}` | `null` → inherit | Whether mesh derives this endpoint's CircuitBreaker (§2.6). |

The per-endpoint toggles are **tri-state**: unset (`null`) inherits the `defaults` value; an explicit
`true`/`false` overrides it in either direction.

### 2.4 `defaults.*` — shared fallbacks + policy (`Defaults`)

The whole `defaults` block may be omitted; each field then takes its Kotlin default. Set a field only to tune it.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `individual-request-timeout-ms` | `Long` | `4000` | Fallback anchor where an endpoint sets none. |
| `max-retries` | `Int` | `0` | `0` = no retries (matches the production sidecar `max-retries: 0`). |
| `margins` | `Margins` | see §2.4.1 | Shapes the derived ladder. |
| `transport` | `Transport` | see §2.5 | The app→sidecar pre-response phases. |
| `time-limiter.enabled` | `Boolean` | `true` | Default: mesh derives + overrides each endpoint's TimeLimiter. |
| `circuit-breaker.enabled` | `Boolean` | `false` | Default: mesh does **not** derive the CircuitBreaker knob. |

#### 2.4.1 `margins.*` (`Margins`)

Under `defaults`, and — partially, field-merged — per endpoint. Every field optional.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `global-margin-ms` | `Long` | `200` | Folded into `global = (1 + maxRetries)·individual + globalMargin`. The **one** margin the sidecar also reads. |
| `response-timeout-margin-ms` | `Long` | `200` | `responseTimeout = global + responseTimeoutMargin` - Reactor Netty's `responseTimeout` on this stack, Apache's on the blocking one. |
| `p99-margin-ms` | `Long` | `200` | `p99 = individual − p99Margin` (single-attempt latency recovered from the anchor). |
| `healthy-retries` | `Int` | `1` | Routine retries still counting as "healthy" (capped at `maxRetries`). |
| `slow-call-backoff-ms` | `Long` | `200` | Per-attempt backoff feeding the healthy ceiling. |
| `slow-call-cap-fraction` | `Double` | `0.8` | Hard cap: `slowCall` never exceeds `global · this`. |

> The TimeLimiter margin is **not** a configurable field here — it is a computed property of `transport` (§2.5).

A per-endpoint `margins:` is a **partial** override: only the fields you set change; the rest stay at
`defaults.margins`.

### 2.5 `defaults.transport.*` (`Transport`)

The app→sidecar hop — the WebClient pre-response phases that sit **outside** the `responseTimeout` window, so
only the TimeLimiter covers them.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `sidecar-base-url` | `String` | `http://localhost:8042` | Default base URL when an endpoint sets none. |
| `connect-timeout-ms` | `Long` | `100` | TCP connect timeout of the engine (small because app→sidecar is localhost + plaintext). |
| `pending-acquire-timeout-ms` | `Long` | `200` | Fail fast on pool exhaustion. |
| `max-connections` | `Int` | `100` | Cap on the pool: per destination on `jetty` and `apache`, per backend on `reactor-netty` (§1.2), not applied on `jdk`. |

> **Computed, not configurable:** `timeLimiterMargin = connectTimeoutMs + pendingAcquireTimeoutMs + 100`
> headroom. Because it is derived from *these* caps, the TimeLimiter always overshoots the worst-case
> pre-response phases by construction — the ladder invariant can never be violated by tuning transport.

### 2.6 The per-endpoint feature toggles

`time-limiter.enabled` (default `true`) and `circuit-breaker.enabled` (default `false`) decide whether mesh
**auto-derives and overrides** that Resilience4j backend's timeout knob for the endpoint:

- `false` does **not** remove or disable the R4j backend — the primitive stays in the call path (§3.1). It just
  means mesh leaves that backend's config untouched, so it keeps whatever it already has (per-instance YAML, a
  `configs:` base, or the registry default).
- The CircuitBreaker toggle governs the derived `slowCallDurationThreshold` **and** its conditional 100%
  slow-call-rate pin as **one unit** (§3.2), so mesh can never leave the CB in an incoherent "duration derived,
  rate inherited" state.

```yaml
# Shared default for every endpoint
app:
  mesh:
    defaults:
      circuit-breaker:
        enabled: true          # let mesh derive the slow-call threshold for ALL endpoints
# ...or per endpoint, which wins over the default in both directions
    endpoints:
      - triplet: "/endpoint/apis.example.com/POST/v8/agreements/products/lookup"
        individual-request-timeout-ms: 400
        circuit-breaker:
          enabled: true        # this endpoint only
        time-limiter:
          enabled: false       # leave this endpoint's TimeLimiter at its own R4j config
```

### 2.7 The derived ladder

You maintain **one** value per endpoint — `individual-request-timeout-ms`. Everything else is derived
(`ResolvedTimeouts`):

```
global          = (1 + maxRetries) · individual + globalMarginMs
responseTimeout = global + responseTimeoutMarginMs
timeLimiter     = responseTimeout + transport.timeLimiterMargin        (margin computed, not configurable)
p99             = individual − p99MarginMs
slowCall        = min( p99·(1 + eff) + eff·slowCallBackoffMs ,  global · slowCallCapFraction )
                  eff = min(maxRetries, healthyRetries)
```

Worked example — anchor 1 s .. 5 s at `maxRetries = 0`, default margins/transport (all ms):

| individual | global | responseTimeout | slowCall | timeLimiter |
|---:|---:|---:|---:|---:|
| 1000 | 1200 | 1400 | 800 | 1800 |
| 2000 | 2200 | 2400 | 1760 | 2800 |
| 3000 | 3200 | 3400 | 2560 | 3800 |
| 4000 | 4200 | 4400 | 3360 | 4800 |
| 5000 | 5200 | 5400 | 4160 | 5800 |

**Binding-time guards** (`ResolvedTimeouts.init`, message prefixed with the triplet):

- `individual-request-timeout-ms >= 1`
- `max-retries >= 0`
- `responseTimeout > global` (so the transport backstop sits below the logical budget)
- `p99-margin-ms` in `0 ..< individual` (so the derived p99 is `>= 1`)
- `healthy-retries >= 0`, `slow-call-backoff-ms >= 0`
- `slow-call-cap-fraction` in `(0, 1)` (else the slow-call cap is `<= 0` or `>= global`, making detection inert)

The `responseTimeout` is tighter than the TimeLimiter budget **by construction**, so the transport guard can
never drift past the logical budget — the TimeLimiter is always the one that trips first, yielding a clean
timeout rather than a raw transport error.

---

## 3. `resilience4j.*` — the three primitives

`ReactiveResilienceDecorator` applies **all three** primitives to every call, **unconditionally** — nested
CircuitBreaker (outermost) → TimeLimiter → Bulkhead (innermost), Resilience4j's own default aspect order, so a
bulkhead rejection travels outward and the breaker can see it (§3.3). The `app.mesh` toggles only decide whether
mesh *derives* a knob, never whether the primitive is in the path. So an undeclared instance does **not** mean "off"; it means the backend silently runs
on Resilience4j's own defaults. The reference configuration therefore declares every instance explicitly, at
the R4j default values written out **deliberately** so a library upgrade cannot change behaviour behind its back.
Instance keys are the derived aliases (§2.2).

### 3.1 Declared instances

```yaml
resilience4j:
  bulkhead:
    configs:
      mesh-backend:
        max-concurrent-calls: 25    # sum across backends <= transport.max-connections (100); see below
        max-wait-duration: 0        # fail fast rather than queue (a waiting caller blocks its subscribing thread)
    instances: { <alias>: { base-config: mesh-backend }, ... }
  circuitbreaker:
    configs:
      mesh-backend:
        failure-rate-threshold: 50
        sliding-window-type: COUNT_BASED
        sliding-window-size: 100
        minimum-number-of-calls: 100
        wait-duration-in-open-state: 60s
        permitted-number-of-calls-in-half-open-state: 10
        slow-call-rate-threshold: 100
        slow-call-duration-threshold: 60s   # inert unless mesh derives it (§2.6); the TimeLimiter cancels first
    instances: { <alias>: { base-config: mesh-backend }, ... }
  timelimiter:
    configs:
      mesh-backend:
        timeout-duration: 3s        # the fallback for an endpoint whose time-limiter toggle is off
        cancel-running-future: true
    instances: { <alias>: { base-config: mesh-backend }, ... }   # EVERY alias, or the start fails
```

- **The bulkheads partition the shared pool.** `sum(max-concurrent-calls)` over the mesh aliases must not exceed
  `app.mesh.defaults.transport.max-connections`; otherwise the excess queues for a connection instead of being
  shed with a named `BulkheadFullException`, and the pending-acquire timeout that follows is a **failure** on
  the breaker of whichever backend is waiting. `ResilienceConfigContractTest` fails the build when the reference
  configuration or the test fixture breaks the rule; for a host's own configuration `MeshResilienceStartupGuard`
  (veredium-core) logs a **WARN** at startup naming the sum, the pool size and every alias - not a failed start,
  since an over-committed pool can be deliberate. The sum is conservative on `reactor-netty`, where each backend
  has a sub-pool of its own (see above), and exact for the engines that share one pool per destination.
- **Why `max-wait-duration: 0`.** A bulkhead that waits for a permit does so inside `subscribe()`, on the
  subscribing thread, *before* the TimeLimiter's timer starts (Resilience4j's reactive bulkhead acquires before it
  passes its subscription on; Reactor's `timeout` starts counting on that subscription). A waiting caller
  therefore does not spend its budget - it parks a thread, possibly an event loop, for up to the wait, which no
  timeout of the ladder bounds. Pinned in `ReactiveResilienceDecoratorTest`, together with the operator order.

### 3.2 How mesh customizes the registries

The mesh wiring lives in `veredium-core` and is shared with the blocking client; `WebClientConfiguration`
imports it and contributes the one thing the core cannot know - this stack's exception vocabulary
(`WebClientVerdicts`). For each endpoint, a `CircuitBreakerConfigCustomizer` and (when `time-limiter.enabled`)
a `TimeLimiterConfigCustomizer` named after the alias are consulted by Resilience4j's Boot configuration as the
**last** step of building that `instances.<alias>` entry - after the named base config and the instance's own
YAML, before the instance exists - and set **only** the derived knobs on top of what YAML produced:

- TimeLimiter `timeoutDuration` ← `timeLimiterTimeout` (when `time-limiter.enabled`),
- CircuitBreaker `slowCallDurationThreshold` ← `slowCallDuration` (when `circuit-breaker.enabled`), **plus** a
  conditional pin of `slowCallRateThreshold` to **100%** exactly when the derived slow-call sits below
  `individual` (the "inversion") — the derived duration is only safe against a healthy-but-slow endpoint when the
  rate demands total degradation before opening. Duration and rate are set by the **one** customizer.

Nothing is removed or recreated and nothing is re-bound: the instance the metric and health binders see is the
final one. The price is that **every** derived alias must be declared under all three
`resilience4j.<layer>.instances` - Resilience4j creates an instance for an undeclared customizer name on the
registry *default*, not on `mesh-backend`, and says nothing. `MeshResilienceStartupGuard` (also `veredium-core`)
fails the start in that case and names the key to add; `MeshResilienceShippedConfigTest` boots the reference
configuration of this module against it.

### 3.3 What the CircuitBreaker ignores

The breaker sits **outside** the bulkhead, and out of the box it charges every error against the failure rate.
The customizer of §3.2 folds `WebClientVerdicts` into each instance (additively — the base predicate is OR-ed back in) to discount
faults the client causes **itself**, matched on the thrown exception **and its causes**:

- `BulkheadFullException` — this pipeline's own load shedding, not a backend failure.
- `DataBufferLimitException` — the response exceeded `max-in-memory-size` (§1.1), sometimes wrapped by Spring in a
  `WebClientResponseException` reporting 200, hence the cause-chain match.

Deserialisation failures and validation violations never reach the breaker at all — they run in the `.map`
**after** the R4j decoration, so the breaker only ever sees the raw HTTP outcome.

### 3.4 The response contract: no empty success

The `Mono<T>` of `get`/`post` emits exactly one value or an error. A 2xx answer **without a body** — a 204, or a
200 with an empty entity — fails with `EmptyResponseBodyException` (veredium-core) naming the backend and the
expected type. Like a deserialisation failure it is raised **after** the decoration: the breaker records the
exchange as the success it was, and the client's error line reports the exception at WARN.

> **Behaviour change (2026-10-21).** Before, such an answer completed the `Mono` **empty**: no error line, and a
> `NoSuchElementException` out of the caller's `awaitSingle()` that named neither the backend nor the cause —
> while the blocking client failed the same answer with a Jackson parse error. A caller that relied on the empty
> completion (`awaitSingleOrNull()`, `switchIfEmpty`) now receives the exception and has to handle it instead.
> Pinned in `ResilientWebClientIntegrationTest` with a 204 and an empty 200.

---

## 4. Logging & observation switches

### 4.1 Exchange logging (`adapter-logging.*`, `logging.level`) — legatium

One structured event per outbound HTTP exchange, written by
[`legatium-webclient-logging`](https://github.com/Inqudium/legatium) on the logger `adapter-http-exchange`. The
library's auto-configuration hangs its filter on Boot's `WebClient.Builder` bean as a `WebClientCustomizer`, and
every client of the factory is a clone of that bean (§1.3) — nothing in this module wires it. The backend name
reaches the event as `adapter_name` through a request attribute the factory sets once per client
(`ClientRequestLoggingFilter.ADAPTER_NAME_ATTRIBUTE`); the URI template reaches it as `adapter_url_template`
through the attribute `ResilientWebClient` stamps on every request. (Until ticket 30 the module wrote this event
itself, as the "exchange diary"; [logging-guide.md](logging-guide.md) §0 records the change.)

```yaml
adapter-logging:
  enabled: true                       # context-start switch: false removes filter and customizer
  logger-name: adapter-http-exchange  # the library default, kept so its documentation applies verbatim
  request-headers:
    includes: ["*"]                   # every header is logged ...
    unmasked: ["Host", "Accept", "Content-Type", "Content-Length", "User-Agent", "traceparent", "tracestate"]
  response-headers:                   # ... in plaintext only when listed; the rest as a fingerprint
    includes: ["*"]
    unmasked: ["Content-Type", "Content-Length", "Date", "Cache-Control"]
  log-request-body: on-failure        # bodies on every outcome but success: failure, rejected 4xx, cancel
  log-response-body: on-failure
  max-body-bytes: 16384               # the capture cap per body
  include-query-string: true
  slow-request-threshold: 5s

logging:
  level:
    adapter-http-exchange: INFO                      # every exchange; WARN keeps failures, timeouts, cancels, slow, 401/403/408/429
    eu.inqudium.legatium.webclient.logging: WARN     # the library's own breadcrumb and fail-open lines
```

- **The level is a filter, not a switch** (unlike the diary's): INFO shows every exchange, WARN keeps failures,
  timeouts, cancellations, slow calls and the four escalated rejections (401, 403, 408, 429), ERROR only calls
  that threw. The context-start switch is
  `adapter-logging.enabled`. Both logger names are pinned by `LoggingLevelContractTest`.
- **Headers:** everything selected is masked by default; the `unmasked` lists are the plaintext allowlists. `Host`
  is readable because it is the second view of the backend next to `adapter_name`; `X-Access-Token` and
  everything unknown render as `<length>:<16 hex>` — stricter than the diary's prefix-only masking.
- **Bodies:** `on-failure` logs a body only when the outcome is not `success` — a 4xx is `rejected`, so a 404's
  error body is there and a 200's customer data is not. The cap is 16 KB with a `[truncated, N bytes total]`
  note; the diary's 256 KB (derived from `max-in-memory-size`) is gone, and so is the coupling.
- **Outcome vocabulary:** a 4xx answer is `rejected` with its status (the peer refused the request; INFO, and WARN
  for 401, 403, 408 and 429 — legatium ADR-0012, since 1.2.0); a 5xx a WARN
  `failure`; a caller's cancel *and* a TimeLimiter budget expiring are `cancelled` (the TimeLimiter cancels the
  WebClient call); the connector's own timeouts are `timeout`; a breaker short-circuit or a full bulkhead
  produces no exchange and therefore no event — those verdicts stay on `ResilientWebClient`'s transport lines,
  which carry `adapter_name` and `adapter_url_template` under the library's field names so both loggers group
  by one field.
  Pinned by `LegatiumExchangeLoggingIntegrationTest`; the runbook reads the fields accordingly.
- **The caller's MDC** is restored around the event from the Reactor Context through the `ThreadLocalAccessor`s
  the host registered with Micrometer's context propagation (legatium ADR-0010). A coroutine `MDCContext` alone
  does not reach it — the host registers an accessor per key it wants on the event; the recipe is pinned in the
  same test.
- **`ResilientWebClient`'s own transport lines** (the error, cancellation and closing lines, and the report-only
  validation line) need no accessor: each subscription snapshots the caller's MDC on the subscribing thread and
  writes those lines under it on the event loop, restoring the loop's own MDC afterwards
  (`ClientCallLog.withMdc`). They carry the caller's correlation keys whatever the host configured — with or
  without the propagation hook of §1.3. Pinned in `ResilientWebClientIntegrationTest` for a plain subscriber and
  for a coroutine `MDCContext`.
- Every other key and its default: the library's `docs/adapter-logging-reference.yml`.

### 4.2 The central observation disable-filter (`management.observations.*`)

When `app.webclient.observability.http-observation-enabled` is on (§1.3), Spring's **only** central switch over
the client observation is a name-prefixed **DISABLE** filter (default `true` = enabled):

```yaml
management:
  observations:
    enable:
      all: true                    # false scopes OFF everything
      http.client.requests: true   # false scopes OFF just the client observation
```

A `false` here suppresses the `http.client.requests` observation even though the module toggle is on — and,
because it acts on the registry, for every WebClient of the host, not just this module's. The factory logs the
effective values at startup so an operator can spot a central override.

---

## 5. Fail-fast summary

The module refuses to start (or fails at first use) rather than run mis-configured:

| Condition | Where | Effect |
|---|---|---|
| `max-in-memory-size` out of `1B..Int.MAX` | `WebClientProperties.init` | binding fails, names the key |
| `pending-acquire-max-count < -1` / negative pool duration | `WebClientProperties.Pool.init` | binding fails, names the key |
| `engine.default` / an `engine.endpoints` value not one of the four engines | Boot's binder | binding fails, names the key |
| An `engine.endpoints` key that is no mesh alias | `ResilientWebClientFactory` (construction) | context start fails, names the key and the known aliases |
| A selected engine's library missing from the classpath | `ResilientWebClientFactory` (construction) | context start fails, names the Maven coordinates |
| `jdk` selected, JVM started without `-Djdk.httpclient.allowRestrictedHeaders=host` | `ResilientWebClientFactory` (construction) | context start fails, names the flag |
| `jetty` selected with `pool.pending-acquire-max-count: 0` | `ResilientWebClientFactory` (construction) | context start fails, names both keys |
| Invalid triplet (`< 4` parts / not `endpoint/...`) | `EndpointConfig` | fails when the endpoint is resolved |
| Colliding aliases or triplets | `MeshProperties.init` | binding fails, names the duplicates |
| Ladder cannot form (see §2.7 guards) | `ResolvedTimeouts.init` | binding fails, prefixed with the triplet |
| `create()` for an alias with no mesh endpoint | `ResilientWebClientFactory` | `IllegalStateException` at first use |
| `sum(bulkhead max-concurrent-calls) > max-connections` in the reference configuration or the test fixture | `ResilienceConfigContractTest` | build fails |
| `sum(bulkhead max-concurrent-calls) > max-connections` in a host's configuration | `MeshResilienceStartupGuard` (veredium-core) | **WARN** at startup naming both numbers; the context starts |
| A mesh alias undeclared under one of the three `resilience4j.*.instances` | `MeshResilienceStartupGuard` (and `ResilienceConfigContractTest` for the reference) | context start fails, names the key |

---

## 6. Complete annotated example

```yaml
logging:
  level:
    adapter-http-exchange: INFO                    # the exchange stream (§4.1); WARN keeps only the failures
    eu.inqudium.legatium.webclient.logging: WARN   # the library's own logger

adapter-logging:                                   # §4.1 - the values explained there; every other key is the library default
  enabled: true
  request-headers:
    includes: ["*"]
    unmasked: ["Host", "Accept", "Content-Type", "Content-Length", "User-Agent", "traceparent", "tracestate"]
  response-headers:
    includes: ["*"]
    unmasked: ["Content-Type", "Content-Length", "Date", "Cache-Control"]
  log-request-body: on-failure
  log-response-body: on-failure
  max-body-bytes: 16384

app:
  webclient:
    max-in-memory-size: 256KB                  # §1.1
    pool:                                      # §1.2 (lifecycle only; max-connections lives in mesh transport)
      pending-acquire-max-count: 100
      max-idle-time: 30s
      max-life-time: 5m
      evict-in-background: 30s
    observability:
      http-observation-enabled: true           # §1.3 reference ON (also flips the JVM context-propagation hook)
    validation:                                # §1.4 reference ON, report only; exempt per alias
      enabled: true
      fail-on-violation: false
      endpoints: {}
    engine:                                    # §1.6 the code default, shown explicitly
      default: reactor-netty
      endpoints: {}

  mesh:
    # defaults omitted -> every endpoint sets its own anchor; policy knobs default in Kotlin (§2.4)
    endpoints:                                 # §2.3 (alias derived from the triplet)
      - triplet: "/endpoint/apis.example.com/POST/v8/agreements/products/lookup"
        individual-request-timeout-ms: 400
      - triplet: "/endpoint/api.example.com/GET/v5/access-profiles/by-involved-party/{id}"
        individual-request-timeout-ms: 300
      - triplet: "/endpoint/apis.example.com/POST/v5/involved-parties/lookup"
        individual-request-timeout-ms: 500
      - triplet: "/endpoint/apis.example.com/GET/v6/involved-parties/{uuid}"
        individual-request-timeout-ms: 400

resilience4j:                                  # §3 (instance keys = derived aliases; declared explicitly)
  # bulkhead / circuitbreaker / timelimiter as in the reference configuration, src/test/resources/application.yml
```

---

## See also

- [mesh-timeout.md](../../veredium-core/docs/mesh-timeout.md) — the mesh derivation and its R4j registration in depth.
- [logging-guide.md](logging-guide.md) — the design of the former exchange diary, superseded by legatium (§0 there).
- `legatium/docs/GUIDE.md` and `legatium-webclient-logging/docs/GUIDE.md` (outside this repository) — the library that writes the exchange event.
- [metrics.md](metrics.md) — the emitted metrics and their cardinality.
- `../../sync-bridge/docs/jvm-sidecar/timeout-leiter-mesh-call-muster.md` — the ladder rationale and the sidecar twin.
- The reference configuration `src/test/resources/application.yml` is the authoritative example: a test resource,
  not part of the artefact, pinned by `ResilienceConfigContractTest`, `MeshResilienceShippedConfigTest`,
  `LegatiumExchangeLoggingIntegrationTest` and `WebClientPropertiesTest`.
