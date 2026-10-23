# veredium-core guide

> The long-form guide of the module: architecture, integration, configuration. It is
> rendered on the documentation site by the Docs workflow
> (`.github/scripts/generate-guide-pages.py` copies it under `docs/guides/` with
> rewritten links), so relative links from here must point at files that exist.

## Scope

`veredium-core` is the stack-neutral core both client modules of the Veredium project build
on. It holds everything that is identical by construction between the blocking client
(`veredium-restclient`, Spring `RestClient`, by default over Apache HttpClient 5) and the reactive one
(`veredium-webclient`, Spring `WebClient`, by default over Reactor Netty). Each stack can switch its HTTP
engine; see [Configuration](#configuration). The core is the same for every engine:

- the **mesh timeout ladder** (`eu.inqudium.veredium.core.mesh`): the `app.mesh` tree, one
  entry per endpoint, from which every timeout rung, the client target and the alias derive;
- the **breaker verdicts** (`eu.inqudium.veredium.core.resilience`): the one decision "is this
  failure the backend's fault?" that both the error log's severity and the circuit breaker's
  predicates derive from;
- the **Resilience4j wiring** that installs both into the registries Spring Boot
  auto-configures: one `*ConfigCustomizer` per mesh alias, consulted while each instance is
  built, and a fail-fast startup guard;
- the **blocking resilience decorator**: `CircuitBreaker( TimeLimiter( Bulkhead( call ) ) )`
  around any blocking call, with the virtual-thread hand-off the blocking TimeLimiter needs -
  usable on its own, without a client;
- **response validation** (`eu.inqudium.veredium.core.validation`): the `validation.*` block
  both properties trees bind, and the one Bean Validation `Validator` every client derives from
  the host's factory, its messages pinned to English;
- the **HTTP observation report** (`eu.inqudium.veredium.core.observation`): the two startup
  lines that say what a stack's observation toggle achieves in the host at hand;
- the library's **identity** (`Veredium`): its name, its manifest version and the `User-Agent`
  token `veredium/<version>` both clients send on every request unless the host configures its
  own (`Veredium.resolveUserAgent` validates it);
- `BaseAdapter`: the two budgets an adapter over either client publishes to its callers,
  `timeLimiterTimeout` and `responseTimeout`, one contract for both stacks;
- the **per-call log and validation** (`eu.inqudium.veredium.core.client`): `ClientCallLog`, the
  transport lines a client writes under the caller's correlation keys, the verdict-driven severity
  of its error line, the size sample and the Bean Validation of the decoded response; and
  `ResponseBodyMetrics`, the UTF-8 size the sample is taken in.

Nothing in here knows an HTTP stack. A host does not depend on this module directly; it comes
with the client module the host chooses. The decision to cut the modules this way is
[ADR-0001](../../docs/adr/ADR-0001-one-stack-neutral-core-module.md).

## Prerequisites

- Java 21 or newer at runtime (the build itself needs JDK 24+, see the
  [README](../README.md)).
- Spring Boot 4 and `resilience4j-spring-boot4` in the host — both come transitively with
  either client module.

## Dependency

Transitive through `veredium-restclient` or `veredium-webclient`. Declared directly only by a
module that wants the ladder or the verdicts without a client:

```xml
<dependency>
    <groupId>eu.inqudium</groupId>
    <artifactId>veredium-core</artifactId>
    <version>...</version>
</dependency>
```

## Architecture

**The ladder.** `MeshProperties` binds `app.mesh`; `resolveAll()` derives one `ResolvedTimeouts`
per endpoint - alias, host, path template, base URL, and the rungs `global`, `responseTimeout`,
`slowCall` and `timeLimiter` from the one measured anchor. The derivation, the invariants it
enforces at bind time and the startup line that prints the result are described in
[the mesh timeout ladder](mesh-timeout.md).

**The verdicts.** Resilience4j has three answers for an error - recorded, ignored, success - and
records every exception by default. `HttpExchangeErrorPolicy` says which answers are a benign
4xx (every 4xx except 408 and 429: a *success*, the backend answered), `ClientSideFailurePolicy`
which failures the client raised itself (a bulkhead rejection, the buffer limit: *ignored*,
evidence in neither direction). The rules are the same for both stacks; what differs is the
exception vocabulary - which type carries a status, which type is the buffer limit - and that is
what each stack contributes as its one `BreakerVerdicts` bean (`RestClientVerdicts`,
`WebClientVerdicts`).

**The wiring.** `MeshResilienceConfiguration` registers one circuit-breaker customizer per alias and one
time-limiter customizer per alias whose derivation is on; Resilience4j's Boot configuration
consults them as the last step of building each `instances.<alias>` entry, so the instance the
metric and health binders bind to is already the final one. `MeshResilienceStartupGuard` fails
the context when an alias is not declared under all three `resilience4j.<layer>.instances` (the
instance would silently run on the registry default) or when the customizer's effect is not
observable on the instance the registry serves, and logs a WARN when the mesh bulkheads together
admit more calls (`sum(max-concurrent-calls)`) than the shared pool has connections
(`app.mesh.defaults.transport.max-connections`). Each stack's `*VerdictsConfig` `@Import`s
`MeshResilienceConfiguration`, so a host that scans its stack's package gets all of it.

**The blocking decorator.** `BlockingResilienceDecorator` runs a `() -> T` as
`CircuitBreaker( TimeLimiter( Bulkhead( call ) ) )` - Resilience4j's own aspect order - on a fresh
virtual worker thread, because a blocking call cannot time itself out: the caller waits on the
worker's future for at most the TimeLimiter budget and is released at the budget whatever the
worker is doing; the worker is interrupted, which a blocking socket read honours. The caller's
MDC and current observation are carried onto the worker for the duration of the call, and a
caller that is interrupted itself cancels its worker. `decorateAsync` runs the same pipeline
with the wait on a second virtual thread and returns a `CompletableFuture` whose `cancel`
interrupts the worker; its failures arrive on the future, unwrapped. The class carries no Spring annotation;
`veredium-restclient` declares its bean (`RestClientConfiguration`, worker name
`restclient-call-`), and a host with a blocking call of its own declares another or constructs
one directly ([ADR-0002](../../docs/adr/ADR-0002-blocking-resilience-decorator-as-a-core-component.md)).

**Validation and the observation report.** `ResponseValidationProperties` is the
`validation.*` block of `app.restclient` and `app.webclient` alike - a shared default, per-alias
overrides, one `fail-on-violation` posture; `ResponseValidation` turns it and the host's
`ValidatorFactory` into the validator a client's factory hands each client (or `null` for an
alias that does not validate), with the provider verdict for the startup line.
`HttpObservationReport` reads the stack's toggle, the central Micrometer disable-filter and the
host's `ObservationRegistry` and says, once, which of the three silences the
`http.client.requests` observation; each stack subclasses it with its name and key.

**The per-call log.** Each client holds one `ClientCallLog` per backend and hands it what it does
with a call besides the exchange: the opening DEBUG line, the size sample, `validate` of the
decoded value, the error line (DEBUG without a stack trace for a benign 4xx, WARN with one for
everything else - the same verdict the breaker's record predicate follows), the buffer-limit line
naming the number to raise, and the cancellation line at DEBUG. Every line carries `adapter_name`
and `adapter_url_template`, the fields legatium's exchange event uses, under the client's own
logger name. What a stack contributes is its wording of the buffer limit, its properties prefix and
how it reads the declared `Content-Length` off its exception.

## Bounding a blocking call of your own

```kotlin
val decorator = BlockingResilienceDecorator(
    circuitBreakerRegistry, timeLimiterRegistry, bulkheadRegistry,   // Boot's beans, or Registry.of(...)
    observationRegistry = observationRegistry,                       // optional: nests the call under the caller's span
    workerNamePrefix = "ledger-call-",                               // the thread name on every log line of the call
)

// by name: the three instances under resilience4j.<layer>.instances.ledger
val balance = decorator.decorate("ledger") { ledgerSdk.balanceOf(accountId) }

// or with instances built by hand, no registry involved
val breaker = CircuitBreaker.of("ledger", CircuitBreakerConfig.custom()
    .ignoreExceptions(*BlockingResilienceDecorator.SELF_INFLICTED_FAILURES.toTypedArray())
    .build())
val balance = decorator.decorate(breaker, timeLimiter, bulkhead) { ledgerSdk.balanceOf(accountId) }

// or without blocking the caller: the same pipeline, the wait on a second virtual thread
val pending: CompletableFuture<Balance> = decorator.decorateAsync("ledger") { ledgerSdk.balanceOf(accountId) }
pending.cancel(true)   // interrupts the worker and frees its permit - unlike a plain CompletableFuture
```

Two things the decorator cannot do for you. The breaker it is used with has to be told to ignore
the three failures the pipeline raises itself - the bulkhead's rejection, the caller's
`InterruptedException`, the `RejectedExecutionException` after shutdown - or load shedding opens
the circuit against a healthy backend; `SELF_INFLICTED_FAILURES` names them, and on a mesh alias
the customizer installs them. (A cancelled `decorateAsync` future reaches the breaker as that
`InterruptedException`, so the list matters for the asynchronous form too.) And the call has to honour interruption: the budget releases the
caller regardless, but a worker that ignores its interrupt runs on until it returns. Call
`shutdown()` when done (`@PreDestroy` does it for a bean).

## Configuration

One tree, `app.mesh`, documented key by key in the KDoc of `MeshProperties` and in
[the mesh timeout ladder](mesh-timeout.md). The `resilience4j.*` blocks a host declares next to
it must name every derived alias under `timelimiter`, `circuitbreaker` and `bulkhead`
`instances`; either client module's reference configuration (`src/test/resources/application.yml`)
is the worked example.

**The HTTP engine is a property of the client stack, not of the mesh.** `app.mesh` describes the timeout
ladder whatever transport carries it. Which transport carries it is chosen under the stack's own tree, as a
default plus an optional override per mesh alias:

```yaml
app:
  restclient:
    engine:
      default: apache            # apache | jdk | jetty | reactor-netty
  webclient:
    engine:
      default: reactor-netty     # reactor-netty | jdk | jetty | apache
      endpoints:
        apis-example-com-GET-v6-involved-parties: jdk   # this backend only; the key is its mesh alias
```

The setting reaches only the stack's own clients. Boot's global `spring.http.clients.*` switches, the
host's own `RestClient`/`WebClient` beans and any other client in the process stay as they are. Not every
engine can apply every rung of `app.mesh.defaults.transport`: the JDK client, for one, has no pool size and no
pending-acquire timeout. Each stack logs what applies at startup. The per-engine tables, the dependency each
engine needs and the fail-fast checks are in the
[blocking client guide](../../veredium-restclient/docs/GUIDE.md#engines) and in the reactive client's
[configuration guide](../../veredium-webclient/docs/webclient-config-guide.md) (section "`engine.*` — the HTTP
engine").

## Guides

- [The mesh timeout ladder](mesh-timeout.md)
