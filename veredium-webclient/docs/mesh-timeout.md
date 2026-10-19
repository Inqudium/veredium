# Mesh-derived Resilience4j timeouts

> **Ownership & baseline.** Maintained by **Dirk Haase**. Verified against the code at commit `589d3890` (2026-07-30)
> Where this text and the code disagree, the code
> wins.

The `adapter/web/mesh` package is the **single per-endpoint tree**, one entry per **operation** (triplet). From
one *measured* anchor per endpoint it drives that operation's **WebClient target** (base URL, Host header), its
**request path template** (the adapter builds its URI from it) AND its **whole timeout ladder** — the
Resilience4j knobs (TimeLimiter timeout, CircuitBreaker slow-call threshold) and the transport timeouts the
WebClient uses (Netty `responseTimeout`, TCP connect, pool `pendingAcquireTimeout`, pool `maxConnections`).
There is no separate `app.webclient.targets` tree, and the path template is NOT re-declared in the adapters or
the metrics. It is a self-contained port of the standalone PoC (`helm-mesh-timeouts/kotlin`, package
`eu.mesh.timeouts.poc`, which stays the reference); the derivation and its rationale live in
`../../sync-bridge/docs/jvm-sidecar/timeout-leiter-mesh-call-muster.md`.

## What it computes

You maintain one value per endpoint — `individual-request-timeout-ms` (the measured `p99(single attempt) +
margin`). Everything else is derived (see `ResolvedTimeouts`):

```
global          = (1 + maxRetries) * individual + globalMarginMs
responseTimeout = global + webClientMarginMs
timeLimiter     = responseTimeout + transport.timeLimiterMargin
                  timeLimiterMargin = connectTimeoutMs + pendingAcquireTimeoutMs + headroom   (computed, not configurable)
slowCall        = min( healthy_ceiling, global * slowCallCapFraction )
                  healthy_ceiling = p99*(1 + eff) + eff*slowCallBackoffMs,  eff = min(maxRetries, healthyRetries)
                  p99             = individual - p99MarginMs          (recovered from the anchor)
```

`maxRetries`, the `margins` block (which now carries `globalMarginMs` — the one margin the sidecar also reads)
and the `transport` block all default in Kotlin (`MeshProperties.Margins()` / `Transport()`), so they need not
appear in the config — set a field only to tune it (partial overrides are field-merged, per endpoint too). The
TimeLimiter margin is **not** a configurable field: it is a computed property of `Transport` (worst-case
pre-response phases + a fixed headroom), so it can never be set below the ladder invariant.

## What comes from mesh vs. WebClientProperties

`app.mesh` is now **REQUIRED**: `ResilientWebClientFactory` injects `MeshProperties` and a backend with no
matching mesh endpoint fails fast. The mesh tree owns everything per-endpoint that derives from the **timeout
ladder** (target, path, the R4j timeout knobs). `WebClientProperties` keeps the truly **global** knobs *and*
**response validation** — which is per-backend but does **not** come from the ladder, so it lives under
`app.webclient.validation` rather than in the mesh tree (see the next section).

| Config | Scope | Source |
|---|---|---|
| WebClient `baseUrl` | per-endpoint | endpoint `baseUrl`, else the shared `Transport.sidecarBaseUrl` |
| `Host` header | per-endpoint | derived from the triplet host (`/endpoint/<host>/...`) |
| request path template | per-endpoint | derived from the triplet path; the adapter reads `client.pathTemplate`, `WebClientMetrics` derives its cardinality tags from it (`tagsOf`) |
| extra static headers | per-endpoint | endpoint `headers` (usually empty) |
| Netty `responseTimeout` | per-endpoint | ladder `ResolvedTimeouts.webClientResponseTimeout` |
| TimeLimiter `timeoutDuration`, CircuitBreaker `slowCallDurationThreshold` | per-endpoint | ladder, injected into the R4j registries |
| whether mesh derives the TimeLimiter / CircuitBreaker knob | per-endpoint (global default + per-endpoint override) | mesh `time-limiter.enabled` / `circuit-breaker.enabled` (`defaults` + endpoint `BackendToggleOverride`) |
| TCP connect, pool `pendingAcquireTimeout`, pool `maxConnections` | global | mesh `Transport` (`defaults.transport`) |
| `maxInMemorySize`, read/write idle guards, pool lifecycle (`pendingAcquireMaxCount`, `maxIdleTime`, `maxLifeTime`, `evictInBackground`) | global | `WebClientProperties` (no mesh equivalent) |
| response Bean Validation on/off | per-backend (global default + per-alias override) | `WebClientProperties.Validation` (`enabled` + `endpoints[alias]`) |
| validation: fail vs. report-only (`fail-on-violation`) | global | `WebClientProperties.Validation.failOnViolation` |

The `responseTimeout` is tighter than that endpoint's TimeLimiter budget **by construction** (the ladder puts
`timeLimiter = responseTimeout + transport.timeLimiterMargin`), so the transport guard can never drift past the
logical budget — the old `responseTimeoutFraction` mechanism is gone.

## Response validation

Whether a deserialised response DTO is checked against its Bean Validation constraints is configured under
`app.webclient.validation`, **not** in the mesh tree — it is per-backend, but nothing about it derives from the
timeout ladder, so it does not belong there. `WebClientProperties.Validation` carries:

- `enabled` (default **false**) — the shared default, plus `endpoints[<alias>]` per-backend overrides keyed by
  the **derived alias** (the same `host-METHOD-path` name the R4j instances use). A per-alias entry wins in both
  directions. Off by default because the constraints are generated from the published OpenAPI specs and those
  are stricter than reality in places (`ProductAgreementsDataResponseV8DTO` declares `@Size(min = 1)` on a list
  a lookup may legitimately return empty; `AccessProfileAdapter` documents a second defect, INC15288523), so
  validation is adopted one backend at a time against real traffic.
- `fail-on-violation` (default **true**, **SHARED** only) — whether a detected violation **fails** the call with
  a `ConstraintViolationException` or is only **reported** (logged at WARN, the response returned unchanged). Set
  it false to watch a newly-enabled backend against real traffic before enforcing. It is one posture for the
  whole app; the per-alias choice is only *which* backends validate.

The check runs **outside** the Resilience4j decoration (like deserialisation): a response that violates its own
schema is a client-side contract problem, not an outage, so it must not count against the CircuitBreaker — the
backend answered, promptly and with a 200. `ResilientWebClientFactory` resolves the toggle once when it builds
each client (`validator` is `null` when off), so the hot path holds no flag to consult; the startup log prints
the effective set of validated aliases and whether a provider is present.

## How the R4j registration works

`MeshTimeoutConfig` is a `@Configuration` picked up by the same component scan as the rest of the module (it
enables `MeshProperties` binding). For each endpoint, `MeshTimeoutRegistrar` overrides **only** the one derived
knob on top of the endpoint's **existing** R4j config:

- TimeLimiter `timeoutDuration` <- `timeLimiterTimeout`
- CircuitBreaker `slowCallDurationThreshold` <- `slowCallDuration`

Each config is built with `from(<existing instance config, else the registry default>)`, so the rest of that
backend's policy — the shared default **and** any per-instance override under `resilience4j.*.instances.<alias>`
— is preserved; only the derived knob changes. It uses the registries auto-configured by
`resilience4j-spring-boot4`, **not** new registry beans, so nothing conflicts.

A pre-existing instance under the alias is **removed and recreated** with the derived config (an R4j config is
immutable after creation, so overriding means remove + recreate), so mesh wins over a YAML-pinned instance —
`circuitBreaker(alias, config)` would otherwise return the existing one untouched. This is safe because
`ReactiveResilienceDecorator` resolves the instance per call and the WebClient factory is lazy, so nothing
holds a stale reference at startup.

## One endpoint per operation; the alias is the logical backend name

Each mesh entry is one **operation** (one triplet = host + method + path), matching the sidecar's
endpoint-per-triplet model, so each gets its own WebClient + CircuitBreaker/TimeLimiter/Bulkhead. An adapter
that calls several operations therefore holds several aliases — e.g. `InvolvedPartyAdapter` looks up the POST
lookup and the GET separately, so a slow GET cannot trip the POST breaker.

The `alias` is the name the adapters pass to `ResilientWebClientFactory.create(backendName)`; the factory
looks up the endpoint (target + path + timeouts) by it, and it is also the R4j instance key and the metric
label. The shipped config sets **no explicit `alias`** — it is **derived from the triplet** (host with dots →
hyphens, then UPPERCASE method, then the static path segments, in triplet order), so the triplet is the single
source of the endpoint identity and the same method+path on different hosts cannot collide:
`/endpoint/api.example.com/GET/v5/access-profiles/by-involved-party/{id}` →
`api-example-com-GET-v5-access-profiles-by-involved-party`. An explicit `alias:` is still supported (it wins over
the derivation). Aliases are unique-checked at binding (`MeshProperties.init`), and the adapters' `ALIAS`
constants must equal the derived values — pinned by `MeshAliasContractTest`.

## Startup log

On registration each endpoint's resolved ladder is logged (`replaced=true` when a pinned instance was
overridden):

```
mesh ladder [api-example-com-GET-v5-access-profiles-by-involved-party] /endpoint/api.example.com/GET/v5/access-profiles/by-involved-party/{id}: individual=300ms global=2200ms responseTimeout=2500ms slowCall=900ms timeLimiter=3100ms (replaced=true)
```

## Enabling

See the active `app.mesh` block in `src/main/resources/application.yml` (one entry per backend). The PoC in
`helm-mesh-timeouts` renders the same config from a single Helm source (`meshEndpoints`) for both the sidecar
and the app side.
