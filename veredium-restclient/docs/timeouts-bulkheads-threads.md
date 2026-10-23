# Timeouts, bulkheads and threads — how a blocking call through rest-client is bounded

> **Ownership & baseline.** Maintained by **Dirk Haase**. Verified against the code at commit `cd5bceb4`
> (2026-10-19). Where this text and the code disagree, the code wins.

This guide explains what happens to a call made through `ResilientRestClient` from the moment an adapter
invokes it to the moment the caller gets a value or an exception: which thread does what, which of the five
timeouts fires in which phase, how the bulkhead and the circuit breaker see the call, and what cancellation
does. It builds on the shared ladder of `veredium-core/docs/mesh-timeout.md` and is the blocking counterpart
of the reactive twin's `webclient-config-guide.md`, and it goes deeper where the blocking stack differs: the thread hand-off, the
interrupt semantics, and the shutdown.

The configuration values quoted are the module's reference configuration (`src/test/resources/application.yml`,
a test resource - the library ships none) and the Kotlin defaults. The worked example
throughout is the endpoint `GET /v6/involved-parties/{uuid}` with its anchor of 400 ms, and, where a rounder
number helps, an endpoint with a 1 s anchor.

---

## 1. The three layers in one picture

Every call is wrapped as `CircuitBreaker( TimeLimiter( Bulkhead( exchange ) ) )` by
`BlockingResilienceDecorator` (a component of `veredium-core`, which this module instantiates as a bean in
`RestClientConfiguration`), and the exchange underneath runs on Apache HttpClient 5 with its own three
timeouts. Nothing in the call is unbounded; the question is only *which* bound fires, and that is a matter of
which phase the call is in. The picture and §2-§3 describe the default engine; with `app.restclient.engine` set
to `jdk`, `jetty` or `reactor-netty` the decorator, the ladder and the caller's side are unchanged and only the
transport column differs - §9 says how.

```
caller thread                                   worker thread (virtual, "restclient-call-N")
─────────────────────────────────────────────   ─────────────────────────────────────────────────────────────
ResilientRestClient.get/post
  serialise body (POST only)
  "Executing request" (DEBUG)
BlockingResilienceDecorator.decorate
  CircuitBreaker.acquirePermission ─ OPEN? ──▶ CallNotPermittedException, nothing else happens
  executor.submit ────────────────────────────▶ install caller's MDC, reopen caller's observation
  TimeLimiter: future.get(timeLimiterTimeout)    Bulkhead.acquirePermission ─ full? ──▶ BulkheadFullException
    │                                            RestClient.exchange
    │                                              legatium interceptor wires the exchange
    │                                              pool lease ............ connectionRequestTimeout (200 ms)
    │                                              connect ............... connectTimeout (100 ms)
    │                                              send, wait for head ... responseTimeout (ladder rung)
    │                                              bounded body read ..... responseTimeout, per read
    │                                              response close → legatium exchange event
    │                                            release permit, restore MDC, close observation scope
    ◀──────────────────────────────────────────  future completes (value or exception)
  CircuitBreaker.onResult / onError
deserialise, validate (outside the decoration)
return / throw
```

Three properties of this picture carry everything that follows:

- **The caller never touches the socket.** It waits on a `Future` for at most the TimeLimiter budget. That is
  the only way a blocking call can have a hard deadline: the thread stuck in a socket read cannot notice one.
- **The bulkhead permit is taken on the worker**, right before the exchange, so it counts calls that are
  genuinely in flight at the backend — not callers queued somewhere in front of it.
- **Deserialisation and validation happen after the decoration**, on the caller's thread. An empty body, a parse
  failure or a schema violation is the backend ANSWERING with a success status - a client-side problem that must
  not reach the breaker.

---

## 2. The timeout ladder, rung by rung

All per-endpoint timeouts derive from ONE measured number, the endpoint's `individual-request-timeout-ms`
(the sidecar's per-attempt timeout: `p99(single attempt) + margin`). `ResolvedTimeouts` derives the rest:

| Rung | Formula | Owner | 400 ms anchor | 1 s anchor |
|---|---|---|---|---|
| `individual` | configured | sidecar, per attempt | 400 | 1000 |
| `global` | `(1 + maxRetries) × individual + globalMarginMs` (200) | sidecar, whole call incl. its retries (`max-retries: 0` by default) | 600 | 1200 |
| `responseTimeout` | `global + responseTimeoutMarginMs` (200) | **this module**, Apache `RequestConfig.responseTimeout` | 800 | 1400 |
| `timeLimiterTimeout` | `responseTimeout + connect + pendingAcquire + 100` (= + 400) | **this module**, Resilience4j TimeLimiter | 1200 | 1800 |
| `slowCall` | `min(p99 × (1 + healthyRetries), 0.8 × global)` | breaker's slow-call threshold, only if `circuit-breaker.enabled` | 200 | 800 |

The ordering `individual < global < responseTimeout < timeLimiterTimeout` holds by construction
(`ResolvedTimeouts.init` rejects anything else), and it encodes a division of labour:

- The **sidecar** gives up first. It retries within `global`; the app sees one answer, or none.
- The **transport** gives up next. `responseTimeout` is the time Apache waits for the response *head* once the
  request is on the wire, and Apache applies it as the socket timeout of that request, so every subsequent
  read of the body is bounded by it too. Its expiry is a `SocketTimeoutException`, wrapped by Spring into a
  `ResourceAccessException` — a **transport failure the breaker records** and legatium classifies as `timeout`.
  The two pre-response guards fail the same way for the breaker but not for the event: a connect timeout is a
  `SocketTimeoutException` too (`timeout`), a pool-lease timeout (`ConnectionRequestTimeoutException`) is classified as
  `timeout` by name since legatium 1.2.1 (§7).
- The **TimeLimiter** gives up last. Its margin of 400 ms over the response timeout is not a guess: it is
  exactly the two pre-response phases the response timeout does not cover — the pool lease (`pendingAcquire`,
  200 ms) and the connect (100 ms) — plus 100 ms of headroom. Under normal conditions it never fires; it is the
  outer backstop that releases the *caller* if the transport's own guards fail to.

`MeshLadderLogger` prints the resolved ladder per endpoint at startup
(`mesh ladder [alias] triplet: individual=…ms global=…ms responseTimeout=…ms slowCall=…ms timeLimiter=…ms`),
and the mesh customizers (`veredium-core`, `MeshResilienceCustomizers.kt`) hand the `timeLimiter` rung to Resilience4j
while it builds each `instances.<alias>` entry — before the instance exists, so the registry never holds a
pre-mesh value. `MeshResilienceStartupGuard` fails the start when an alias is undeclared or the rung did not
arrive. Adapters still read their budget on every access: the registry owns it.

### 2.1 The two transport timeouts that are not rungs

| Apache setting | Value | Source | What it bounds |
|---|---|---|---|
| `ConnectionConfig.connectTimeout` | 100 ms | `app.mesh.defaults.transport.connect-timeout-ms` | TCP connect to the sidecar on loopback. Small on purpose: localhost, plaintext. |
| `RequestConfig.connectionRequestTimeout` | 200 ms | `app.mesh.defaults.transport.pending-acquire-timeout-ms` | Waiting for a free connection in the shared pool. Fail fast: a caller queued here is burning its budget doing nothing. |

Both are global (one pool, one sidecar); `responseTimeout` is the only per-endpoint one, set on each
endpoint's `CloseableHttpClient` over the shared manager.

### 2.2 What is deliberately NOT configured

- **No read timeout on `HttpComponentsClientHttpRequestFactory`.** The factory leaves Spring's own timeouts
  unset so the client's `RequestConfig` stands; a value there would silently override the ladder.
- **No Apache retries, no redirects, no cookie store.** Retries belong to the sidecar; an in-app second attempt
  would spend the ladder's budget twice without any rung knowing.
- **No time-to-live on the request.** Apache has no end-to-end deadline; the TimeLimiter is that deadline.

---

## 3. The TimeLimiter in its blocking shape

Resilience4j's blocking TimeLimiter is `decorateFutureSupplier`: it takes a `Future`, calls
`future.get(timeoutDuration)`, and on expiry throws `TimeoutException` after cancelling the future
(`cancelRunningFuture`, `true` by default and pinned explicitly in the reference YAML). The decorator feeds it the
future of the worker it submitted. Three things follow.

**The caller is released at the budget, whatever the worker is doing.** This is the guarantee an adapter's
`timeLimiterTimeout` publishes, and the one upstream orchestration sums into its own end-to-end budget. It
holds in every phase — connecting, leasing, waiting for the head, reading the body — because the caller is
never *in* any of those phases.

**The worker is cancelled, and that cancellation is effective.** `future.cancel(true)` interrupts the worker.
Since JDK 13 the JDK's socket implementation reacts to an interrupt by closing the socket, so a worker blocked
in a read fails at once with `SocketException: Closed by interrupt`; a worker blocked in Apache's pool lease
or in connect is interruptible too. The worker therefore ends promptly, releases its bulkhead permit, and the
half-used connection is discarded rather than returned to the pool. (This is the one place the blocking stack
is *better off* than a naive reading of it: "a blocking read cannot be cancelled" was true of the old socket
implementation, not of the current one. `BlockingResilienceDecoratorTest` pins that the worker is interrupted.)

**The exchange event reports a `cancelled`.** legatium's interceptor runs on the worker. Since legatium 1.3.0 it
reads the worker's interrupt flag, and the interrupt in the cause chain where the flag is already consumed, and
writes WARN `adapter_outcome=cancelled` without a cause - the outcome the reactive twin writes for an abandoned
subscription. That holds on all four engines for a call cancelled before its response head, and on Apache, JDK
and Reactor Netty for one cancelled in the body as well. **One exception:** Jetty's body stream turns an
interrupt during the body into a bare `InterruptedIOException` and consumes the flag, so legatium has no signal
left and logs ERROR `failure`, with the head's status. `LegatiumExchangeLoggingIntegrationTest` pins each case per
engine. The budget expiry itself is visible as `resilience4j.timelimiter.calls{kind=timeout}` and as the
`TimeoutException` on `ResilientRestClient`'s own WARN line.

### 3.1 Step by step: the transport's response timeout fires (the normal case)

The 1 s anchor: `responseTimeout` 1400 ms, `timeLimiterTimeout` 1800 ms. The backend accepts the connection
and the request, then does not answer. Times are from the moment the adapter called the client.

| t | Caller thread | Worker thread (`restclient-call-N`) |
|---|---|---|
| 0 | `ResilientRestClient.get`: DEBUG `Executing request to backend 'x' … responseTimeout: 1400ms, timeLimiterTimeout: 1800ms`; `startNanos` taken. `decorate`: breaker CLOSED, permission granted; `executor.submit`; blocks in `future.get(1800 ms)`. | starts; installs the caller's MDC, reopens the caller's observation; takes the bulkhead permit (`available.concurrent.calls` − 1). |
| ≈1 | — | `RestClient.exchange`: Spring starts the `http.client.requests` observation; legatium's interceptor wires the exchange (its own start time, the correlation header); Apache leases a connection from the pool (immediately — the pool is not saturated), reuses or connects (≤ 100 ms), sends the request and sets the connection's socket timeout to **1400 ms** for this request. |
| 1 … 1400 | blocked | blocked in the socket read, waiting for the response head. |
| ≈1400 | — | the read throws `SocketTimeoutException`. Apache **discards the connection** (a timed-out connection is not returned to the pool) and lets the exception out. legatium's interceptor catches it: one WARN breadcrumb on its own logger, then the exchange event `WARN adapter_outcome=timeout adapter_duration_ms≈1400`, no status; rethrows unchanged. `DefaultRestClient` wraps the `IOException` into `ResourceAccessException`, records it on the observation (`exception=ResourceAccessException`, `status=CLIENT_ERROR`, `outcome=UNKNOWN`) and stops it. The decorator's `finally` runs: permit released, observation scope closed, MDC restored. The future completes **exceptionally**; the worker ends. |
| ≈1401 | `future.get` throws `ExecutionException`; the TimeLimiter unwraps it, records `timelimiter.calls{kind=failed}` and rethrows the `ResourceAccessException` itself. `CircuitBreaker.decorateCallable` catches it: not ignored, not a benign 4xx → **recorded as a failure** (`circuitbreaker.calls{kind=failed}`, `failure.rate` moves). `ResilientRestClient` catches it: WARN `Error during request to backend 'x' … duration so far: 1401ms` **with** stack trace; DEBUG `Completed request … outcome=error`; rethrows. The adapter and its caller see `ResourceAccessException` whose cause is the `SocketTimeoutException`. | — |

Everything is over within a millisecond of the transport giving up, on both threads, and the TimeLimiter
never fired: 1400 < 1800 by construction. The connection is gone; the next call to that endpoint opens a new
one (`connectTimeout` 100 ms, on loopback).

### 3.2 Step by step: the TimeLimiter budget expires

By construction the TimeLimiter cannot fire while the transport's three guards are doing their job: lease
≤ 200 ms, connect ≤ 100 ms, head ≤ 1400 ms, sum 1700 < 1800. It fires when the call is somewhere those
guards do not reach:

- **the body.** `responseTimeout` is applied as the socket timeout, which bounds every single read — not the
  whole body. A backend that sends the head promptly and then drips a large body, each chunk inside 1400 ms,
  passes every transport guard and never finishes. This is the realistic case.
- **the worker not running.** A GC pause, a carrier shortage, a pinned carrier (§5.4): time passes with no
  socket read in progress.
- **misconfiguration.** `time-limiter.enabled: false` for the endpoint keeps the YAML or default budget (1 s)
  instead of the derived one, which can lie *below* the response timeout.

Same anchor; the backend answers `200` at 300 ms and then drips the body.

| t | Caller thread | Worker thread |
|---|---|---|
| 0 … 300 | as in §3.1 | as in §3.1; the head arrives at ≈300 ms: legatium's wrapper snapshots status 200 and the headers, the observation context gets the response. |
| 300 … 1800 | blocked in `future.get` | in `ResilientRestClient.exchange`'s bounded body read, each `InputStream.read` returning a few bytes well inside its 1400 ms socket timeout. |
| 1800 | `future.get` throws the JDK's `TimeoutException`. The TimeLimiter: `onError` → `timelimiter.calls{kind=timeout}`; `cancelRunningFuture` is `true` → **`future.cancel(true)`**, which interrupts the worker; rethrows its own `TimeoutException` (`TimeLimiter 'x' recorded a timeout exception.`). The breaker records it as a **failure**. `ResilientRestClient`: WARN `Error during request … duration so far: 1800ms` with stack trace; DEBUG `Completed … outcome=error`; rethrows. **The caller is released now**, with a `TimeoutException`, whatever the worker does next. | the interrupt lands while the worker is parked in a socket read: the JDK closes the socket and the read throws `SocketException: Closed by interrupt` — out of `BoundedBodyReader`, out of the exchange function. |
| ≈1801 | returned | `DefaultRestClient` wraps the `IOException` into `ResourceAccessException`, records it on the observation and, in its `finally`, **closes the response**. legatium's wrapper had already recorded the read failure through its `onFailure` hook and the worker's interrupt, so the event it emits on close is `WARN adapter_outcome=cancelled`, **with** `adapter_response_status_code=200` and `adapter_duration_ms≈1800` — a 200 that failed its caller is never logged as a success (had the cancel come before the head, the event would carry no status; on Jetty this body case is `ERROR failure`, §3). Apache's `streamClosed` sees the dead socket and discards the connection. The decorator's `finally`: permit released, scope closed, MDC restored. The future is already cancelled, so the worker's `ResourceAccessException` goes nowhere; the worker ends. |

Two facts about this timeline are the ones to remember:

1. **The caller's exception and the worker's exception differ.** The caller gets the TimeLimiter's
   `TimeoutException`; the wire, the exchange event and the `http.client.requests` timer see
   `ResourceAccessException` over `Closed by interrupt`. Correlate them by `adapter_name` and time, or by
   the MDC keys the decorator carried across.
2. **Nothing lingers.** Because the interrupt closes the socket, the bulkhead permit, the pool connection and
   the virtual thread are all released within milliseconds of the budget — the property §4.1 is built on.
   With `cancel-running-future: false` the second column would instead continue until a single read exceeds
   1400 ms or the body ends, and the permit would be held for that long.

A caller interrupted by its own deadline (§5.3) produces the same worker-side timeline; the caller-side
differs only in the exception (`InterruptedException`), the DEBUG `Cancelled request` line in place of the WARN,
and the breaker's verdict (**ignored**, §7).

**If an operator sets `cancel-running-future: false`** for an instance, the caller is still released at the
budget, but the worker runs on until the transport's own timeout, holding its bulkhead permit and its pool
connection for up to `responseTimeout` longer. That is a legitimate choice only for a call whose completion
has side effects worth waiting for; for the lookups this module fronts, it is not, and the reference
configuration does not do it.

---

## 4. The bulkhead

`SemaphoreBulkhead`, one per mesh alias, `max-concurrent-calls: 25`, `max-wait-duration: 0`. The permit is
acquired on the worker (`Bulkhead.decorateCallable` wraps the exchange, inside the executor), and because
the wait is zero, a full bulkhead rejects immediately with `BulkheadFullException` — the worker was started
and ends at once, no exchange took place, and legatium writes no event because nothing reached the
`RestClient`.

**Why the permit lives on the worker.** Taken on the caller before the hand-off, it would be held during the
`future.get` wait as well — which is the same time span, so the count would be the same in the normal case.
The difference shows under cancellation: a caller released by the TimeLimiter would return its permit while
its worker is still finishing, and the bulkhead would briefly under-count what the backend is actually
handling. On the worker, the permit's lifetime is exactly the exchange's.

**Why the breaker must ignore it.** The breaker is outermost, so a `BulkheadFullException` travels outward
through it like any other error. Resilience4j records every exception unless told otherwise, and a load peak
would then open the circuit against a backend that was never even called. The per-alias circuit-breaker
customizer (`veredium-core`, fed with `RestClientVerdicts`) installs the ignore predicate while Resilience4j builds the
instance — in code, not in YAML, because it is a structural invariant of this pipeline, not a tunable.

**Sizing — the invariant with the pool.** Four aliases × 25 permits = 100 = `app.mesh.defaults.transport.
max-connections`. `ResilienceConfigContractTest` enforces `sum(max-concurrent-calls) <= max-connections` for the
reference configuration; for a host's own, `MeshResilienceStartupGuard` checks the same sum at startup over the
merged instances and logs a WARN naming both numbers and every alias - a WARN, not a failed start, because an
over-committed pool can be a deliberate choice for backends that are never busy at once.
Under that invariant the bulkheads shed load *before* the pool runs out, so `restclient.pool.pending` stays at
zero and `connectionRequestTimeout` never fires in practice. Raising a bulkhead without raising the pool moves
the queueing into the pool — where it fails after 200 ms with a less obvious error — and raising the pool
without the bulkheads changes nothing.

**What the bulkheads are, and are not** (a decision inherited from the twin, recorded in `decisions.md`, D1): a pool partition and a
backstop, not a load-steering mechanism. Callers are expected to bound their own concurrency before reaching
here; a `BulkheadFullException` on a known path is a signal that the caller's bound is mis-sized, or that a
second consumer in the same JVM shares the alias.

### 4.1 Why a semaphore bulkhead over an unbounded executor, and not a `ThreadPoolBulkhead`

Resilience4j offers a second bulkhead, `ThreadPoolBulkhead`, that looks made for exactly this situation: it
runs the call on a bounded `ThreadPoolExecutor` of its own (`core-thread-pool-size`, `max-thread-pool-size`,
`queue-capacity`, default 100) and returns a `CompletionStage`, combining the concurrency limit and the
thread hand-off in one primitive. This module uses the `SemaphoreBulkhead` instead and makes the hand-off
itself, onto an executor with **no bound of its own** — one virtual thread per call. That is a decision, and
these are its reasons, in order of weight.

**1. Cancellation would stop working.** The TimeLimiter releases the caller at the budget and cancels what it
was waiting on. With the decorator's `Future` from `ExecutorService.submit`, `cancel(true)` **interrupts** the
worker, the JDK closes its socket, and the exchange ends within milliseconds (§3). A `ThreadPoolBulkhead`
hands back a `CompletableFuture`, and `CompletableFuture.cancel(mayInterruptIfRunning)` ignores its argument
by specification — it never interrupts the thread running the task. The pool thread would keep blocking in
the socket read until the transport's `responseTimeout`, holding its bulkhead slot and its pool connection
for up to 400 ms (the TimeLimiter's margin) longer than necessary, and under a stalled sidecar every slot
would fill with calls nobody waits for any more. The one property the blocking design is built around — the
caller's deadline frees the backend-side resources too — would be lost.

**2. Queueing would silently spend the budget.** A `ThreadPoolBulkhead` admits a call by queueing it; the
default queue holds 100 calls, and a queued call waits with its TimeLimiter already running. The ladder's
accounting assumes the opposite: the reference bulkheads have `max-wait-duration: 0`, the pool has a 200 ms
lease timeout, and the TimeLimiter's 400 ms margin covers exactly those pre-response phases. A queue in front
of them is a third waiting room the margin does not cover, and a call would arrive at the socket with its
budget partly gone — the `TimeoutException` then fires for a backend that was never slow. The semaphore with a
zero wait rejects at once, which is the behaviour the mesh ladder was derived for.

**3. The meter would answer a different question.** `resilience4j.bulkhead.available.concurrent.calls` on a
semaphore counts calls in flight *at the backend* — the number the pool is sized against and the one `decisions.md`
(D1) assigns to the bulkheads ("a pool partition and a backstop"). A `ThreadPoolBulkhead` exposes thread-pool and
queue depth under `resilience4j.bulkhead.thread.pool.*`: how busy the pool threads are, which includes the
queued calls that are not at the backend at all. Dashboards and the pool invariant
`sum(permits) <= max-connections` are written against the first meaning.

**4. Platform threads, and a pool to size.** The `ThreadPoolBulkhead` has no thread-factory setting; its
`ThreadPoolExecutor` runs platform threads. Four aliases × 25 permits is 100 platform threads parked in socket
reads, each with a megabyte of stack reserved, plus `keep-alive` tuning. A virtual thread parks for a few
hundred bytes of heap and needs no sizing, so the executor can afford to be unbounded — and *being* unbounded
is what makes it honest: it adds no limit of its own, so the only concurrency limits in the call path are the
ones that are configured, named and metered (the bulkheads and the pool). A bounded executor would be a
second, undeclared bulkhead that rejects with a different exception, counts in no meter, and would have to
be kept in step with the four bulkheads by hand.

**5. One hand-off, not two.** Resilience4j's own composition order (§1 of the class KDoc) nests the bulkhead
*inside* the TimeLimiter. The blocking TimeLimiter already needs a `Future`, hence already a hand-off; a
`ThreadPoolBulkhead` would add a second one, and the TimeLimiter would then have to be driven through
`decorateCompletionStage`, which needs a `ScheduledExecutorService` for the timeout — a third executor to own,
size and shut down. The decorator makes exactly one hand-off and declares it.

**6. Parity with the twin.** The reactive module must use the semaphore (a `ThreadPoolBulkhead` cannot wrap a
`Mono`), so the two modules share the `resilience4j.bulkhead.instances.<alias>` configuration, the meter
names, the shared customizer that installs the ignore verdict (`veredium-core`) and the sizing invariant. A `ThreadPoolBulkhead` lives in its own registry
(`resilience4j.thread-pool-bulkhead.*`) with its own config surface; the two stacks would be tuned differently
for the same backends.

**7. Context propagation is already covered.** The one thing a `ThreadPoolBulkhead` offers on top — a
`ContextPropagator` to carry thread-locals onto its threads — the decorator does itself for the two values
that matter (MDC and the current observation, §5.2), without a dependency on Resilience4j's mechanism.

**"Unbounded" in practice.** The number of live workers equals the number of calls in flight, plus — for an
instant — the ones a full bulkhead is about to reject. Both are bounded: the first by the bulkheads (100 in
total), the second by the number of callers, each of which is itself a thread blocked in `future.get`. The
executor's lack of a bound is therefore not a lack of a limit; it is the refusal to add a redundant one.

**When a `ThreadPoolBulkhead` would be the right tool**: a caller that does not want to block at all and
consumes the `CompletionStage` asynchronously, on a stack WITHOUT virtual threads. With them, that caller uses
`getAsync`/`postAsync` (`decorateAsync` underneath, §5): the wait moves to a second virtual thread and the
worker stays interruptible, which a `ThreadPoolBulkhead`'s future never is.

---

## 5. Thread handling

### 5.1 The worker executor

`BlockingResilienceDecorator` owns `Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name(prefix, 0).factory())`
with the prefix the host chose - `restclient-call-` here, set by `RestClientConfiguration` - one **virtual**
thread per call, started on submit, gone when the call ends. Consequences:

- **Nothing to size.** A parked virtual thread costs a few hundred bytes of heap and no OS thread. There is no
  pool that could become a second, undeclared bulkhead; the number of live workers equals the number of calls
  in flight plus the rejected ones in the instant between submit and permit refusal, and the bulkheads cap it.
- **Not injectable, on purpose.** A constructor parameter of type `ExecutorService` would let Spring inject a
  host's own executor bean — which the decorator would then shut down at `@PreDestroy` although it does not
  own it, and whose bound (if a platform pool) would cap concurrency below what the bulkheads declare. Tests
  construct the decorator directly and accept the executor it creates.
- **`@PreDestroy` is `shutdownNow`.** A thread-per-task executor holds no idle threads, so there is nothing to
  drain; `shutdownNow` only interrupts workers of calls still in flight, whose callers are by then gone or
  going. Spring destroys dependents before their dependencies, and the factory depends on the decorator, so the
  factory's drain (§6) runs first.

### 5.2 What crosses the hand-off, and what does not

The worker starts with an empty MDC and no current observation. `BlockingResilienceDecorator.propagate` copies
two things from the caller at decoration time and installs them on the worker for the duration of the call:

| Carried across | How | Why it matters |
|---|---|---|
| the caller's **MDC map** | `MDC.getCopyOfContextMap()` on the caller, `setContextMap` on the worker, restored in `finally` | legatium captures the MDC on the worker when it wires the exchange; without the copy, the one key that joins the outbound event to the inbound request (a correlation id) would be missing from every event this module produces. On the reactive stack that was a documented gap a host had to close with a `ThreadLocalAccessor`; here it is a module guarantee. |
| the caller's **current observation** | `observationRegistry.currentObservation` on the caller, `openScope()` on the worker, closed in `finally` | Spring opens the `http.client.requests` observation on the worker; with the caller's scope reopened there it nests under the caller's span instead of starting a detached root. With a tracing bridge, the scope also re-populates the MDC trace keys. Only when the host has an `ObservationRegistry` bean; otherwise nothing is propagated and nothing is lost. |

Everything else thread-bound on the caller — a `ThreadLocal` of the host's own, a `ScopedValue`, Spring's
`RequestContextHolder`, a transaction — does **not** cross. If an interceptor a host adds to the
`RestClient.Builder` reads such state, it will read the worker's empty one. Register a `ThreadLocalAccessor`
and put the value in the MDC, or pass it explicitly through the request (an attribute, a header).

### 5.3 The caller's side

The caller blocks in `future.get`. Three ways out:

| Event | What the caller gets | What happens to the worker |
|---|---|---|
| worker completes | the value, or the worker's exception **unwrapped** — the TimeLimiter rethrows the `ExecutionException`'s cause, so a `BulkheadFullException`, an `HttpClientErrorException.NotFound`, a `ResourceAccessException` arrive as themselves, and the breaker's predicates see the real types | done |
| TimeLimiter budget expires | `TimeoutException` (named after the instance) | cancelled → interrupted → socket closed (§3) |
| caller is **interrupted** — its own deadline, a cancelled scope, a shutting-down executor | `InterruptedException`, rethrown as such (the interrupt flag is consumed by the throw, as the JDK convention has it) | cancelled by the decorator itself: Resilience4j cancels the future on *its* timeout only, so without this a worker would run on, holding a permit, for a result nobody waits for |

The third row is the blocking counterpart of a subscriber cancelling, and `ResilientRestClient` logs it as such:
a DEBUG `Cancelled request to backend …` line, not a WARN — a cancellation is the caller's business, arrives in
floods under load, and must not drown the lines that say something about this module's health. The operator
signal is the exchange event the worker still writes (`failure`, §3).

An `Error` thrown on the worker is rethrown as itself and recorded by nothing: the breaker counts
`Exception`s only.

### 5.4 Virtual threads and pinning

The worker is a virtual thread doing blocking socket I/O. On JDK 21 (the target) the JDK's socket
implementation parks a virtual thread without pinning its carrier, and Apache HttpClient 5's pool uses
`ReentrantLock`. Should a carrier ever pin — a `synchronized` block around blocking I/O somewhere in a host
interceptor is the usual cause — the symptom is throughput collapsing under load while CPU stays idle; run
with `-Djdk.tracePinnedThreads=full` to find it. JDK 24 (JEP 491) removes pinning on `synchronized`
altogether, so the concern is bounded to the Java 21 line.

---

## 6. Shutdown

`ResilientRestClientFactory.shutdown()` (`@PreDestroy`) closes every engine in use against one deadline; for the
Apache engine, in order:

1. stop the pool's eviction scheduler;
2. close every per-endpoint `CloseableHttpClient` (the manager is shared, so this closes nothing underneath);
3. **wait for the pool to drain**: poll `leased == 0` with `LockSupport.parkNanos` every 50 ms, for at most
   `poolDisposeTimeout` = the longest `responseTimeout` of any endpoint + connect + pendingAcquire + 1 s —
   derived, so it cannot drift below the ladder as endpoints are retuned. No in-flight call can outlive its
   own response timeout, so waiting longer could not let anything finish that was not going to;
4. close the manager (`CloseMode.GRACEFUL`). Expiry of the wait is an *outcome*, logged as one WARN naming the
   engine (`HTTP engine 'apache' did not drain within … - closing it with N exchange(s) still in flight`), never
   an exception out of a destroy callback. Both client stacks write this line, through `veredium-core`'s
   `EngineDrain`.

Apache's own `close` discards leased connections on the spot — the drain wait exists because Apache has no
"dispose when drained". The other engines drain the same way before they are closed: Reactor Netty's pool waits
for its leased connections and Jetty's client for its active and queued requests (the same WARN line, naming
`reactor-netty` or `jetty`), the JDK client through its own `shutdown` + `awaitTermination` (the same line for
`jdk`, without a count, then `shutdownNow`). After the factory, Spring destroys the decorator, whose `shutdownNow` interrupts any
worker still running; those exchanges end as `Closed by interrupt`.

---

## 7. The failure matrix

What each bound produces, for the caller, the breaker, the exchange event and the meters:

| What happened | Caller sees | Breaker verdict | Exchange event (`adapter_outcome`) | Meter |
|---|---|---|---|---|
| breaker open | `CallNotPermittedException`, no worker started | — (not a call) | none | `resilience4j.circuitbreaker.not.permitted.calls` |
| bulkhead full | `BulkheadFullException` | **ignored** | none | `resilience4j.bulkhead.available.concurrent.calls == 0` |
| pool exhausted for 200 ms | `ResourceAccessException` over `ConnectionRequestTimeoutException` | failure | `timeout` (WARN) — matched by class name since legatium 1.2.1 (before: `failure` at ERROR, the type being a bare `InterruptedIOException`) | `restclient.pool.pending > 0` |
| connect > 100 ms | `ResourceAccessException` over `ConnectTimeoutException` | failure | `timeout` (WARN) | `http.client.requests{exception=ResourceAccessException}` |
| no response head within `responseTimeout` | `ResourceAccessException` over `SocketTimeoutException` | failure | `timeout` (WARN) | as above |
| TimeLimiter budget expired | `TimeoutException` | failure | `cancelled` (WARN), written by the cancelled worker — on Jetty `failure` (ERROR) when the body was being read | `resilience4j.timelimiter.calls{kind=timeout}` |
| caller interrupted | `InterruptedException` | **ignored** | `cancelled` (WARN), by the cancelled worker — the same Jetty exception | — |
| 5xx, 408, 429 | `HttpServerErrorException.*` / `HttpClientErrorException.*` | failure | `failure` / `rejected` (WARN) | `http.client.requests{status}` |
| benign 4xx | `HttpClientErrorException.*` | **success** | `rejected` (INFO) | as above |
| 2xx, body past the limit | `ResponseBodyLimitExceededException` | **ignored** | `success` (INFO) | the WARN limit line; no size sample |
| 2xx without a body (204, empty 200) | `EmptyResponseBodyException` (`veredium-core`), naming the backend and the expected type | not seen (outside the decoration) | `success` (INFO) | the WARN error line; size sample `0` |
| 2xx, body does not fit the DTO / violates its schema | `DatabindException` / `ConstraintViolationException` | not seen (outside the decoration) | `success` (INFO) | `restclient.response.body.size` sampled |

> **Note on the interrupted caller.** `CircuitBreaker.decorateCallable` catches `Exception`, and
> `InterruptedException` is one, so without help the breaker would record a caller's interruption as a failure
> of the backend — a backend that was not even given the time to answer. `InterruptedException` is therefore
> the third entry of `RestClientVerdicts.clientSideFailure`, and the circuit-breaker customizer
> keeps it off every mesh alias's breaker; the reactive twin needs no such entry because a cancelled
> subscription carries no error signal at all.

---

## 8. Tuning

- **Only the anchor is tuned.** Measure p99 of a single sidecar attempt, add a margin, set
  `individual-request-timeout-ms` for the endpoint. Every other rung follows. Do not set
  `resilience4j.timelimiter.instances.<alias>.timeout-duration` to "fix" a timeout — mesh overwrites it at
  startup, and the YAML value only governs until it does.
- **The TimeLimiter firing is a diagnosis, not a tuning target.** `calls{kind=timeout}` rising means the
  pre-response phases are eating the margin: the pool is saturated (`restclient.pool.pending`), or the sidecar
  stopped accepting connections. Widen nothing; look there.
- **Bulkheads and pool move together**, and only together (§4).
- **A caller's own budget belongs outside.** An adapter publishes `timeLimiterTimeout` and `responseTimeout`
  (`BaseAdapter`, `veredium-core`) so a caller can compose an end-to-end budget from them; a caller that needs less than the
  budget interrupts its waiting thread and gets the cancellation semantics of §5.3.

---

## 9. The other engines

`app.restclient.engine` ([the guide's "Engines"](GUIDE.md#engines)) replaces the transport underneath the
exchange and nothing above it: the caller still waits at most the TimeLimiter budget, the permit is still taken on
the worker, the ladder is still derived as in §2. What changes is how each rung lands on the engine, and how an
interrupted worker gets out.

**The response-timeout rung.** The rung is the same number on every engine and stays below the TimeLimiter budget
by construction; what it bounds differs:

| Engine | The rung bounds | Pre-response phases |
|---|---|---|
| `apache` | every single read (the socket timeout) - a dripping body passes it, the TimeLimiter catches it (§3.2) | lease ≤ `pending-acquire-timeout-ms`, connect ≤ `connect-timeout-ms` |
| `jdk` | head and body together, from sending the request until the body is closed | connect ≤ `connect-timeout-ms`; no lease - the JDK pool never makes a caller wait |
| `jetty` | the whole request, from queueing for a connection to the end of the body; the same value bounds silence on the connection | connect ≤ `connect-timeout-ms`; a queued request waits within the rung, not within `pending-acquire-timeout-ms` |
| `reactor-netty` | the longest silence between reads after the request was sent | lease ≤ `pending-acquire-timeout-ms`, connect ≤ `connect-timeout-ms` |

On `jdk` and `jetty` a dripping body therefore ends at the rung - before the TimeLimiter - as a transport timeout
(`HttpTimeoutException`, `TimeoutException`) rather than as the `TimeoutException` of §3.2. On `jetty` the
TimeLimiter margin's share for the lease is unused, because the queue wait is inside the rung.

**The interrupted worker.** The guarantee of §3 - the worker ends within milliseconds of the budget, its permit and
its connection with it - holds on every engine, by its own route (the table in the guide's "Abandoned exchanges"):
the JDK client cancels its exchange future or fails its body read; Jetty's request is aborted by the module because
Spring only stops waiting for it; Reactor Netty's head wait cancels its subscription, and its body reads run on a
short-lived virtual thread so that the interrupt reaches them - Spring's Reactor body stream ignores an interrupt
and would otherwise keep the worker spinning until the next chunk. `RestClientEngineIntegrationTest` pins the head
and the body phase, the caller's interrupt and the cancelled future for each engine.

**Bulkheads and the pool.** The sizing invariant of §4 (`sum(max-concurrent-calls) <= max-connections`) is checked
against `app.mesh.defaults.transport.max-connections` whatever the engine. On `apache`, `jetty` and `reactor-netty`
that number is the pool's limit; on `jdk` nothing enforces it - the JDK pool is unbounded, so the bulkheads are the
only concurrency limit in front of the sidecar.

**Threads.** The worker stays a virtual thread on every engine. The engines bring their own threads besides: none
for Apache; the JDK client's selector thread and a virtual-thread executor owned by the engine; Jetty's thread
pool and scheduler; Reactor Netty's event loops (`restclient-reactor-netty-*`) and the virtual body-read threads.
All are created only for an engine in use and stopped by the factory's shutdown (§6), which drains every engine in
use against one deadline.

**Related:** [metrics guide](metrics.md) · [what differs from the reactive twin](restclient-vs-webclient.md) ·
[mesh ladder derivation (veredium-core)](../../veredium-core/docs/mesh-timeout.md).
