# Mesh-derived Resilience4j timeouts

> **Ownership & baseline.** Maintained by **Dirk Haase**. Reviewed against the code on 2026-10-23. Where this text and the code disagree, the code wins.

The `eu.inqudium.veredium.core.mesh` package is the **single per-endpoint tree**, one entry per **operation**
(triplet). From one *measured* anchor per endpoint it drives that operation's **client target** (base URL, Host
header), its **request path template** (a consumer builds its URI from it) AND its **whole timeout ladder** — the
Resilience4j knobs (TimeLimiter timeout, CircuitBreaker slow-call threshold) and the transport timeouts the
client uses (the response timeout, TCP connect, pool `pendingAcquireTimeout`, pool `maxConnections`). There is
no separate targets tree, and the path template is NOT re-declared anywhere else.

The tree is **the same for both client stacks and every HTTP engine**: `veredium-restclient` (Apache HttpClient
5 by default) and `veredium-webclient` (Reactor Netty by default) bind it from here, so one deployment's
`app.mesh` block reads identically whichever stack the host runs on, and a host that switches stacks or
engines (`app.restclient.engine` / `app.webclient.engine`) changes nothing under `app.mesh`. The module is
self-contained: the derivation and its rationale are stated here and in the KDoc of `ResolvedTimeouts`.

## What it computes

You maintain one value per endpoint — `individual-request-timeout-ms` (the measured `p99(single attempt) +
margin`). Everything else is derived (see `ResolvedTimeouts`):

```
global          = (1 + maxRetries) * individual + globalMarginMs
responseTimeout = global + responseTimeoutMarginMs
timeLimiter     = responseTimeout + transport.timeLimiterMargin
                  timeLimiterMargin = connectTimeoutMs + pendingAcquireTimeoutMs + headroom   (computed, not configurable)
slowCall        = min( healthy_ceiling, global * slowCallCapFraction )
                  healthy_ceiling = p99*(1 + eff) + eff*slowCallBackoffMs,  eff = min(maxRetries, healthyRetries)
                  p99             = individual - p99MarginMs          (recovered from the anchor)
```

`maxRetries`, the `margins` block (which carries `globalMarginMs` — the one margin the sidecar also reads) and
the `transport` block all default in Kotlin (`MeshProperties.Margins()` / `Transport()`), so they need not
appear in the config — set a field only to tune it. The margins are one policy under `defaults` for every
endpoint; an endpoint is tuned through its anchor and its `max-retries`. The TimeLimiter margin is **not** a
configurable field: it is a computed property of `Transport` (worst-case pre-response phases + a fixed headroom),
so it can never be set below the ladder invariant.

The `responseTimeout` rung is named for what it is on both stacks — the transport's bound on waiting for the
response — rather than for a client class: Apache applies it as `RequestConfig.responseTimeout`, Reactor Netty
as `responseTimeout`, and the JDK client and Jetty with their own per-request timeouts. What exactly each
engine bounds with it (per read, until the head, or the whole exchange) differs; the stack guides list it per
engine. The margin that feeds it is `response-timeout-margin-ms` accordingly.

## What comes from mesh vs. the stack's own properties

`app.mesh` is **REQUIRED**: each stack's client factory injects `MeshProperties`, and a backend with no
matching mesh endpoint fails fast. The mesh tree owns everything per-endpoint that derives from the **timeout
ladder** (target, path, the R4j timeout knobs). The stack's own properties (`app.restclient` /
`app.webclient`) keep the truly **global** knobs *and* **response validation** — which is per-backend but does
**not** come from the ladder, so it lives there rather than in the mesh tree.

| Config | Scope | Source |
|---|---|---|
| client `baseUrl` | per-endpoint | endpoint `baseUrl`, else the shared `Transport.sidecarBaseUrl` |
| `Host` header | per-endpoint | derived from the triplet host (`/endpoint/<host>/...`) |
| request path template | per-endpoint | derived from the triplet path; a consumer reads `client.pathTemplate` |
| extra static headers | per-endpoint | endpoint `headers` (usually empty) |
| transport `responseTimeout` | per-endpoint | ladder `ResolvedTimeouts.responseTimeout` |
| TimeLimiter `timeoutDuration`, CircuitBreaker `slowCallDurationThreshold` | per-endpoint | ladder, handed to the R4j registries through the customizers below |
| whether mesh derives the TimeLimiter / CircuitBreaker knob | per-endpoint (global default + per-endpoint override) | mesh `time-limiter.enabled` / `circuit-breaker.enabled` (`defaults` + endpoint `BackendToggleOverride`) |
| TCP connect, pool `pendingAcquireTimeout`, pool `maxConnections` | global | mesh `Transport` (`defaults.transport`); applied as far as the selected engine supports it, the startup line names what does not apply |
| HTTP engine | global default + per-backend override | the stack's `engine` properties (`app.restclient.engine` / `app.webclient.engine`), no mesh equivalent |
| `maxInMemorySize`, pool lifecycle, idle guards | global | the stack's properties (no mesh equivalent) |
| response Bean Validation on/off, fail vs. report-only | per-backend / global | the stack's `Validation` properties |

The `responseTimeout` is tighter than that endpoint's TimeLimiter budget **by construction** (the ladder puts
`timeLimiter = responseTimeout + transport.timeLimiterMargin`), so the transport guard can never drift past the
logical budget.

## How the R4j registration works

The mesh wiring comes in two halves, and neither is registered on its own: each stack's auto-configuration
(`VerediumRestClientAutoConfiguration` / `VerediumWebClientAutoConfiguration`) `@Import`s both, so a host that
depends on its stack's module gets the mesh wiring with it.

With both client modules as dependencies, both stacks run in one context and each endpoint names the stack that
serves it (`stack: restclient` / `stack: webclient`); each stack's customizers, guard and factory cover its own
endpoints only, and `MeshOwnershipCheck` fails the start for an endpoint claimed by both or by none. Each stack
opens its own pools to the sidecar: `defaults.transport.max-connections` applies per stack, so the sidecar can see
up to twice as many connections from one host. With one client module, `stack` is never needed.

- `MeshResilienceConfiguration`, the `@Configuration` of this package, is stack-neutral: the `app.mesh` binding
  (`MeshProperties`) and `MeshLadderLogger`, the startup line below.
- `MeshResilienceRegistrar`, a Spring `BeanRegistrar` (because the number of beans follows the endpoint list),
  needs the stack's `BreakerVerdicts` - its reading of the two breaker verdicts (see below). It is abstract; each
  stack subclasses it with its verdicts object (`RestClientMeshRegistrar`, `WebClientMeshRegistrar`), so the
  verdicts never pass through the context and no bean of a host can replace them. It registers one
  `CircuitBreakerConfigCustomizer` per mesh alias, one `TimeLimiterConfigCustomizer` per alias whose
  time-limiter derivation is on, and `MeshResilienceStartupGuard`, which fails the start when an alias is not
  wired the way the decorators assume.

Resilience4j's Boot configuration builds one config per `resilience4j.<layer>.instances.<name>` entry when it
creates the registry bean, and consults the customizer bean named after the instance as the **last** step of
that build — after the named base config and the instance's own YAML, **before the instance exists**. The
customizer therefore sees the operator's YAML on its builder and shapes the instance the metric and health
binders then bind to: no second lifecycle, no remove + recreate, no re-binding. Per endpoint it sets:

- TimeLimiter `timeoutDuration` ← `timeLimiterTimeout` (when `time-limiter.enabled`);
- CircuitBreaker `slowCallDurationThreshold` ← `slowCallDuration` (when `circuit-breaker.enabled`), **plus** a
  conditional pin of `slowCallRateThreshold` to **100 %** exactly when the derived slow-call sits below
  `individual` (the "inversion") — the derived duration is only safe against a healthy-but-slow endpoint when
  the rate demands total degradation before opening. Duration and rate are set by the **one** customizer, so
  they can never be split apart;
- the two breaker **verdicts**, regardless of the toggles, because the breaker is in the call path regardless:
  a failure the client raises itself (a bulkhead rejection, the buffer limit; the blocking stack adds an
  interrupted caller and a shut-down worker) is **ignored**, and a benign 4xx (every 4xx except 408 and 429) is
  a **success**. Both are folded onto what YAML produced — the ignore predicate OR-ed, the record predicate
  AND-ed — so an operator's own `ignore-exceptions` and `record-exceptions` survive. The customizer then empties
  the builder's two class lists, whose effect is already inside the predicates it read: Resilience4j's `build()`
  OR-s the lists back in on top of any predicate, so a `record-exceptions` entry naming a superclass of the
  stack's 4xx exception would otherwise re-record the benign 404 past the verdict. The rules live in `HttpExchangeErrorPolicy` and
  `ClientSideFailurePolicy`; which exception carries a status and which types the client raises itself is
  what the stack's `BreakerVerdicts` contributes.

**What the guard checks, and why.** Nothing fails on its own when `resilience4j.<layer>.instances.<alias>` is
missing for an alias the decorator uses: Resilience4j creates an instance for every customizer name it finds no
declaration for — on the registry **default**, not on the operator's named base config — and the bulkhead,
which has no customizer, is created on the first call likewise. The verdicts and the derived budget are present;
every value of the operator's policy is silently gone. `MeshResilienceStartupGuard` runs after every singleton
exists and, for every mesh endpoint, checks that all three layers **declare** the alias (read off the bound
`resilience4j.*` property beans, not the registry) and that the customizer's effect is **observable** on the
instance the registry serves (the predicates applied to probe exceptions, the derived numbers compared). Every
problem is collected before anything is thrown, so one failed start names them all, with the key to add. A
registry a host built by hand is the second silent case the guard catches: every alias present, no customizer
ever consulted. A breaker that records the benign 4xx while it DOES ignore the client-side probe is reported
differently - the customizer's client-side verdict is in place, so the message points at a record rule
composed after it (`record-exceptions`, `record-exception-predicate`) rather than at the registry.

**What the guard warns about.** Once every alias is wired, it checks the bulkheads against the connection pools
they lease from. The pools come from the client factories: each factory is a `MeshConnectionPools` bean and
reports one pool per engine in use, with the aliases that run on it and its `max-connections`
(`app.mesh.defaults.transport.max-connections`); the reactive stack reports Reactor Netty as one pool per backend,
because its client holds a sub-pool per backend. For each pool the guard adds up `max-concurrent-calls` of the
bulkheads of its own stack's aliases on that pool (the merged values of the registry's instances) and compares the
sum with the pool's bound. The bulkheads are a partition of the pool: if together they admit more calls than it has
connections, the excess queues for a connection instead of being shed with a named `BulkheadFullException`, and the
pending-acquire timeout that follows is a **failure** on the breaker of whichever backend is waiting - not
necessarily the one holding the connections. The guard logs a **WARN** naming the pool, the sum, the pool size and
every alias with its permits, and does not fail the start: an over-committed pool can be a deliberate choice for
backends that are never busy at once. Per pool the sum is exact for a total cap shared by all routes (Apache) and
conservative where the engine caps per remote address and an endpoint has a `base-url` of its own. The JDK client
has no connection cap, so its pool is reported without a bound and **not checked** - no WARN comes for it. Only
when no factory reports a pool (a context without a client factory, e.g. a test) does the guard fall back to one
shared pool of `max-connections` over its stack's aliases.

## One endpoint per operation; the alias is the logical backend name

Each mesh entry is one **operation** (one triplet = host + method + path), matching the sidecar's
endpoint-per-triplet model, so each gets its own client + CircuitBreaker/TimeLimiter/Bulkhead. A consumer that
calls several operations therefore holds several aliases, so a slow GET cannot trip the POST breaker.

The `alias` is the name a consumer passes to the stack's factory (`create(backendName)`); the factory looks up
the endpoint (target + path + timeouts) by it, and it is also the R4j instance key and the metric label. The
reference configuration sets **no explicit `alias`** — it is **derived from the triplet** (host with dots →
hyphens, then UPPERCASE method, then the static path segments, in triplet order), so the triplet is the single
source of the endpoint identity and the same method+path on different hosts cannot collide:
`/endpoint/api.example.com/GET/v5/access-profiles/by-involved-party/{id}` →
`api-example-com-GET-v5-access-profiles-by-involved-party`. An explicit `alias:` is still supported (it wins over
the derivation). Aliases and triplets are unique-checked at binding (`MeshProperties.init`).

## Startup log

On `ApplicationReadyEvent` each endpoint's resolved ladder is logged once (`MeshLadderLogger`):

```
mesh ladder [api-example-com-GET-v5-access-profiles-by-involved-party] /endpoint/api.example.com/GET/v5/access-profiles/by-involved-party/{id}: individual=300ms global=500ms responseTimeout=700ms slowCall=100ms timeLimiter=1100ms
```

The guard adds, after every singleton exists, the two policy lines naming the aliases the verdicts were
installed on and one line per endpoint stating what mesh decided for its time limiter and circuit breaker
(`mesh backends [alias] triplet: timeLimiter=on (1100ms), circuitBreaker=off (untouched)`).

## Enabling

See the `app.mesh` and `resilience4j.*` blocks of either stack's reference configuration
(`veredium-restclient/src/test/resources/application.yml`, `veredium-webclient/src/test/resources/application.yml`;
a library ships none): one `endpoints[]` entry per backend, and every derived alias declared under all three
`resilience4j.<layer>.instances` — the guard refuses to start otherwise. Render the sidecar's config and
`app.mesh` from one source, so both sides describe the same endpoints.
