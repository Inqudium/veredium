# ADR-0001: One stack-neutral core module instead of twin copies

**Status:** Accepted  
**Date:** 2026-10-19  
**Last updated:** 2026-10-19 (ADR-0002 moves the blocking decorator into the core as well; the same day, response validation and the HTTP observation report followed - see below)  
**Deciders:** Dirk Haase (maintainer)  
**Related:** ADR-0002 (the blocking resilience decorator, the one stack-specific piece this ADR left in the blocking client, moves to the core too)

## Context

Veredium ships two client stacks: `veredium-restclient` (Spring `RestClient`
over Apache HttpClient 5) and `veredium-webclient` (Spring `WebClient` over
Reactor Netty). Both were ported from dhaase/tool-box, where the blocking
module had been created as a self-contained twin of the reactive one: its
`mesh` package (the timeout ladder), its `resilience` package and its error
policies were copies, renamed per package, with the rule "extract when a
third stack appears" recorded in `restclient-vs-webclient.md`.

The architecture review of the origin module
(`tool-box/rest-client/docs/assessment/ARCHITECTURE_REVIEW-2026-10-19T14-54-27.md`,
finding 1, High) found that cut to be the one real architecture problem
around an otherwise well-argued module: 59 of 60 production files had a
counterpart, the ladder and the policies were identical modulo package,
and the copies had already diverged within a day - a logger-name fix and
undeclared TimeLimiter instances were corrected on one side and recorded
as "not changed there". The review's reading of the justification: the
Rule of Three guards against abstracting from one example; here two real,
identical copies were maintained by one person, and the "abstraction" is a
plain module boundary, not a new concept.

In Veredium the same cut was reproduced at the port, with one further
divergence already present: the blocking module installed its verdicts and
rungs through Resilience4j's own `*ConfigCustomizer` beans with a fail-fast
guard, the reactive module still carried the origin's home-grown amendment
framework (plan, fold, remove + recreate after `afterSingletonsInstantiated`,
identity verification, ERROR events, a lazy-read rule for consumers). The
twin document described the two mechanisms as the same policy installed
two ways. A `veredium-core` module existed but held only the library's
identity.

Three things were identical by construction and stack-neutral in content:
the ladder (`MeshProperties`, `ResolvedTimeouts`, the ladder logger), the
two breaker verdicts (benign 4xx is a success, a client-side failure is
ignored, the cause chain is walked) and the Resilience4j wiring that
installs them. Two things were legitimately stack-specific: which exception
carries a response status and which exceptions the client raises itself.

## Decision

We move everything that is identical by construction into `veredium-core`
and make both client modules depend on it. Each stack keeps only its
client, decorator, factory, properties and one object stating its exception
vocabulary.

### What lives in the core

- `eu.inqudium.veredium.core.mesh`: `MeshProperties` (`app.mesh`),
  `ResolvedTimeouts`, `MeshLadderLogger`, `MeshResilienceConfiguration`.
- `eu.inqudium.veredium.core.resilience`: `HttpExchangeErrorPolicy`,
  `ClientSideFailurePolicy`, `BreakerVerdicts`, the per-alias customizers
  (`MeshResilienceCustomizers.kt`, `MeshResilienceCustomizerRegistrar`) and
  `MeshResilienceStartupGuard`.
- The guide of the ladder, `veredium-core/docs/mesh-timeout.md`.
- Added after the cut, by the same rule: `eu.inqudium.veredium.core.validation`
  (`ResponseValidationProperties`, the `validation.*` block of both
  properties trees, and `ResponseValidation`, the validator derived from the
  host's factory with English messages) and
  `eu.inqudium.veredium.core.observation` (`HttpObservationReport`, the
  startup lines on what the observation toggle achieves). Both had been
  written twice - once as classes in the blocking client, once inline in
  the reactive client's factory - and each stack now keeps a subclass or a
  property of the shared type. `BaseAdapter`, the two budgets an adapter
  publishes to its callers, followed for the same reason; so did the
  per-call log, size sample and validation of the two clients
  (`eu.inqudium.veredium.core.client`: `ClientCallLog`, `ResponseBodyMetrics`),
  which had been identical private methods in both.

The core depends on `spring-boot-starter` and `resilience4j-spring-boot4`
and on no HTTP stack.

### What a stack contributes

The policies are classes rather than objects, parameterised with the
stack's reading of a throwable:

```kotlin
class HttpExchangeErrorPolicy(responseStatus: (Throwable) -> Int?)
class ClientSideFailurePolicy(clientSideFailures: List<Class<out Throwable>>, bufferLimitFailure: Class<out Throwable>)
class BreakerVerdicts(httpExchangeError, clientSideFailure, benignClientErrorProbe: Throwable)
```

Each stack holds one instance (`RestClientVerdicts`, `WebClientVerdicts`)
and one `@Configuration` (`RestClientConfiguration`, `WebClientConfiguration`)
that declares the `BreakerVerdicts` bean and `@Import`s the core's
`MeshResilienceConfiguration`. The core's configuration is therefore never scanned
on its own: a host that scans its stack's package gets the shared wiring
with it. `BreakerVerdicts` verifies at construction that the stack's own
policy reads the probe as a benign client error, so a stack cannot ship a
guard that fails every start.

### One mechanism for both stacks

The reactive module adopts the customizer wiring and drops the amendment
framework (`RegistryAmendments`, `ResilienceAmendmentApplier`,
`AmendmentLogFields`, the three registrars) and its tests. The review's
finding 2 on the origin module - the amendment framework replicates the
library's own extension point - is thereby resolved for the reactive stack
as a consequence of the cut, not as a separate change.

### One field name

The response-timeout margin is `response-timeout-margin-ms`
(`Margins.responseTimeoutMarginMs`) on both stacks, and the rung is
`ResolvedTimeouts.responseTimeout`; the former `rest-client-margin-ms` /
`web-client-margin-ms` and `webClientResponseTimeout` are gone. The rung is
named for what it is on both stacks - the transport's bound on waiting for
the response - rather than for a client class. The per-endpoint `base-url`
and `headers` fields of the reactive module are part of the shared tree and
therefore honoured by the blocking factory too.

### Test suites

The mechanism is pinned once, in the core, with a stand-in stack
(`TestVerdicts`): the derivation, the YAML binding, the customizers, the
two policies, the guard, and a real context booted on a core-owned fixture
with both failure directions. Each stack keeps what only it can prove: its
verdicts through its real decorator and against its exception shapes
(`RestClientVerdictsTest`, `WebClientVerdictsTest`), and its reference
configuration booted with the core's wiring
(`MeshResilienceShippedConfigTest`). The reactive module's reference
configuration now declares every derived alias under
`resilience4j.timelimiter.instances`, which the guard requires.

## Consequences

**Positive:**

- Every change to the ladder, a policy, the alias derivation or the
  customizer semantics is made and tested once. The two stacks cannot
  drift on the "same API contract" any more, and the `app.mesh` block of a
  deployment is identical whichever stack the host runs on.
- The reactive stack loses a second lifecycle (remove + recreate, binder
  re-bind, the lazy-read rule) and the two self-diagnosis ERROR events;
  an undeclared alias is a failed start naming the key, on both stacks.
- The stack-specific remainder is explicit and small: one object per
  stack names its exception vocabulary, and that object is the only place
  the stack's exception types appear in the resilience layer.
- The core's test suite proves the mechanism without either HTTP stack on
  the classpath.

**Negative:**

- A breaking rename of two configuration keys (`rest-client-margin-ms`,
  `web-client-margin-ms` → `response-timeout-margin-ms`) and of one public
  property (`webClientResponseTimeout` → `responseTimeout`). Acceptable
  before the first release; recorded in the CHANGELOG.
- The blocking module can no longer be lifted into another project as a
  single directory; it needs `veredium-core` next to it.
- The per-stack test helpers that apply the customizers to plain
  registries (`CustomizedRegistries`) exist three times - once in the core
  with the stand-in stack, once per client with its real verdicts - because
  a Maven test-jar would be published to Central alongside the modules.
  Twenty lines each, deliberately.

**Neutral:**

- A stack's `*VerdictsConfig` is the single import point of the shared
  wiring. A host that wants the ladder without a client depends on the
  core directly and declares a `BreakerVerdicts` bean of its own.
- The review's finding 3 on the origin (the unused asynchronous form) and
  the Helm twin test were already absent from the port and are not
  affected.
