# ADR-0002: The blocking resilience decorator is a core component, usable without a client

**Status:** Accepted  
**Date:** 2026-10-19  
**Deciders:** Dirk Haase (maintainer)  
**Related:** ADR-0001 (the module cut this ADR completes)

## Context

ADR-0001 moved everything identical by construction between the two
client stacks into `veredium-core` and left each stack its client,
decorator, factory and verdicts. The blocking stack's decorator,
`BlockingResilienceDecorator`, stayed in `veredium-restclient` because it
has no reactive twin to share with: it is the mechanism by which a
BLOCKING call is bounded - `CircuitBreaker( TimeLimiter( Bulkhead( call ) ) )`
on a fresh virtual worker thread, because a blocking call cannot time
itself out, with the caller's MDC and observation carried onto the worker
and the worker cancelled when the caller is interrupted.

Nothing in that mechanism is about an HTTP exchange. The class depended
on Resilience4j, Micrometer's `ObservationRegistry` and SLF4J's MDC only;
what tied it to the blocking client was a `@Component` annotation, the
worker-thread name `restclient-call-`, and KDoc written in terms of
RestClient, legatium and the mesh ladder. A host with a blocking call of
its own to bound - a JDBC lookup, a legacy SDK, a file system - could not
use it without depending on the whole blocking client, and the three
failures the pipeline raises itself (which every breaker used with it
must be told to ignore) were listed only inside the client's verdicts.

## Decision

We move `BlockingResilienceDecorator` into `veredium-core`
(`eu.inqudium.veredium.core.resilience`) and generalise it to the extent
a standalone user needs, and no further.

### The class

- **No Spring annotation.** The core's configuration is never scanned
  (ADR-0001); a host that wants the decorator as a bean declares one.
  `veredium-restclient` does so in `RestClientResilienceConfig`, with the
  host's `ObservationRegistry` as an optional dependency
  (`ObjectProvider`).
- **The worker name is the host's.** `workerNamePrefix` is a constructor
  parameter, `veredium-call-` by default; the blocking client passes
  `restclient-call-`, so its exchange events keep the thread name they
  had. Two decorators in one JVM are told apart by nothing else.
- **Two shapes of `decorate`.** By name - the three instances under
  `resilience4j.<layer>.instances.<name>` in the registries the decorator
  was built with, the Spring shape - and with the three instances handed
  in directly, the standalone shape, for a host that has no registry
  entry and builds `CircuitBreaker.of(...)`, `TimeLimiter.of(...)`,
  `Bulkhead.of(...)` by hand. The first delegates to the second.
- **An asynchronous form.** `decorateAsync` returns a `CompletableFuture`
  at once and runs the SAME pipeline, with the wait on the worker's future
  on a second virtual thread (the *waiter*), which is simply the caller of
  the blocking form. Cancelling the returned future interrupts the waiter
  and through it the worker, so a cancelled call frees its permit and its
  connection - which a `CompletableFuture` chain over a thread pool never
  does, since its `cancel` does not interrupt by specification. Failures
  arrive on the future unwrapped, the refusal after `shutdown` included;
  derived stages are plain futures whose cancellation does not touch the
  call. Two virtual threads per call are the price of one set of
  semantics for both forms. `ResilientRestClient` does not expose it: the
  origin's asynchronous client API had no consumer (its architecture
  review, finding 3), and a host that wants one composes it from the
  decorator.
- **The decorator names its own failures.** `SELF_INFLICTED_FAILURES`
  lists `BulkheadFullException`, `InterruptedException` and
  `RejectedExecutionException`: the backend was never called, or the
  caller stopped waiting. A breaker used with the decorator has to ignore
  them, which the class cannot enforce on a config it does not own; the
  blocking client's `ClientSideFailurePolicy` is built from this list
  plus its own buffer-limit exception, a standalone user passes it to
  `CircuitBreakerConfig.ignoreExceptions`.

Everything else is unchanged: the aspect order, the unbounded
thread-per-task executor (and why not a `ThreadPoolBulkhead`), the MDC and
observation propagation, the cancellation on caller interrupt,
`requestBudget`, `shutdown`.

### What stays with the blocking client

The exchange function, the bounded body read, the response-timeout rung
on Apache's `RequestConfig`, the exception vocabulary
(`RestClientVerdicts`) and the bean declaration. The decorator's test
moves to the core and runs against plain registries and hand-built
instances; the client keeps `RestClientVerdictsTest`, which proves its
verdicts through the real decorator.

## Consequences

**Positive:**

- A host bounds any blocking call with the same pipeline, the same
  context propagation and the same cancellation semantics the blocking
  client has, without a client on the classpath.
- The failures the pipeline raises itself are declared next to the code
  that raises them, not in a consumer's list; the client's verdicts
  derive from the declaration.
- The core's test suite proves the hand-off without an HTTP stack.

**Negative:**

- One more Maven dependency on the core, `micrometer-observation` (which
  Spring's core already brings transitively).
- A host that declares the bean itself has to remember `shutdown()`;
  `@PreDestroy` covers a bean, a direct construction does not.

**Neutral:**

- The reactive stack's `ReactiveResilienceDecorator` stays where it is:
  it is three operator applications over a `Mono` and has no mechanism
  worth sharing.
- The decorator cannot make a call interruptible. A worker that ignores
  its interrupt runs on until it returns; the caller is released at the
  budget regardless. Documented, not enforced.
