# veredium-core guide

> The long-form guide of the module: architecture, integration, configuration. It is
> rendered on the documentation site by the Docs workflow
> (`.github/scripts/generate-guide-pages.py` copies it under `docs/guides/` with
> rewritten links), so relative links from here must point at files that exist.

## Scope

`veredium-core` is the stack-neutral core both client modules of the Veredium project build
on. It holds everything that is identical by construction between the blocking client
(`veredium-restclient`, Spring `RestClient` over Apache HttpClient 5) and the reactive one
(`veredium-webclient`, Spring `WebClient` over Reactor Netty):

- the **mesh timeout ladder** (`eu.inqudium.veredium.core.mesh`): the `app.mesh` tree, one
  entry per endpoint, from which every timeout rung, the client target and the alias derive;
- the **breaker verdicts** (`eu.inqudium.veredium.core.resilience`): the one decision "is this
  failure the backend's fault?" that both the error log's severity and the circuit breaker's
  predicates derive from;
- the **Resilience4j wiring** that installs both into the registries Spring Boot
  auto-configures: one `*ConfigCustomizer` per mesh alias, consulted while each instance is
  built, and a fail-fast startup guard.

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

**The wiring.** `MeshTimeoutConfig` registers one circuit-breaker customizer per alias and one
time-limiter customizer per alias whose derivation is on; Resilience4j's Boot configuration
consults them as the last step of building each `instances.<alias>` entry, so the instance the
metric and health binders bind to is already the final one. `MeshResilienceStartupGuard` fails
the context when an alias is not declared under all three `resilience4j.<layer>.instances` (the
instance would silently run on the registry default) or when the customizer's effect is not
observable on the instance the registry serves. Each stack's `*VerdictsConfig` `@Import`s
`MeshTimeoutConfig`, so a host that scans its stack's package gets all of it.

## Configuration

One tree, `app.mesh`, documented key by key in the KDoc of `MeshProperties` and in
[the mesh timeout ladder](mesh-timeout.md). The `resilience4j.*` blocks a host declares next to
it must name every derived alias under `timelimiter`, `circuitbreaker` and `bulkhead`
`instances`; either client module's reference configuration (`src/test/resources/application.yml`)
is the worked example.

## Guides

- [The mesh timeout ladder](mesh-timeout.md)
