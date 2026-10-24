# Logging Guide: Reactive WebClient

> **Ownership & baseline.** Maintained by **Dirk Haase**. Reviewed against the code on 2026-10-24.
> Where this text and the code disagree, the code
> wins.

This guide describes how the outbound HTTP calls of `veredium-webclient` are logged today - the exchange event
written by `legatium-webclient-logging` (§0) and the constraints of the deployment it serves (§1).

> **The earlier designs are a record, not this page.** The module's original implementation and the exchange
> diary that replaced it are both gone from the tree. Their write-up - why one event per exchange, why structured
> fields, why the emission must not be queued, and where each fell short - is kept in
> [the logging design record](logging-history.md), outside the published guides. A section number of §2 to §10
> on this page means that record, which keeps the guide's original numbering.

---

## 0. Superseded — the diary was replaced by legatium (2026-09-18)

> **The diary is not in the tree.** The design in §4–§8 of the [logging design record](logging-history.md) is
> gone. `ExchangeDiaryLogging.kt` and `ExchangeDiary` were deleted; `ResilientWebClient` creates no diary and
> captures no body; `ResilientWebClientFactory` attaches no filter. (`ResilientWebClient`
> takes an MDC snapshot for its OWN transport lines only - the error, cancellation and closing lines, written on
> the event loop under the caller's keys; see `webclient-config-guide.md` §4.1. The exchange event does not use
> it.)
> The exchange event is written by the library
> [`legatium-webclient-logging`](https://github.com/Inqudium/legatium) (1.3.0), whose auto-configuration hangs
> its filter on Boot's `WebClient.Builder` bean — the bean the factory clones per backend.
>
> **What the library does that the diary did**, in the diary's own terms: one event per exchange (§6.1) on the
> logger `adapter-http-exchange`; structured `adapter_*` fields (§6.2), fourteen of them, mapped by the
> library's own component template; ordering by construction (§6.3); the caller's context restored around
> the emission (§6.4) — from the **Reactor Context** through registered `ThreadLocalAccessor`s (legatium
> ADR-0010), not from an MDC snapshot (§4.4); one level check before any assembly (§6.5); a fail-open guard
> with a meter instead of a swallowed exception (§6.6, §6.8); and body capture bounded by its own
> `max-body-bytes` (16 KB, `on-failure`) instead of by `maxInMemorySize` (§4.6).
>
> **What changed in meaning.** A 4xx answer is `rejected` with its status (INFO; WARN for 401, 403, 408 and 429
> — legatium ADR-0012, since 1.2.0), not a `failure` at INFO (§4.7); a
> 5xx is WARN, not ERROR; a TimeLimiter timeout and a caller's cancel are both `cancelled`; a breaker
> short-circuit or a full bulkhead produces **no** event, because no exchange happened — those verdicts stay on
> `ResilientWebClient`'s transport lines, which carry `adapter_name` and `adapter_url_template` under the
> library's own field names (`AdapterLogFields.kt` and the module's ELK template are gone with the diary;
> `adapter_target_name` is written by nothing any more). The backend name is `adapter_name` on the event, set through the
> request attribute `ClientRequestLoggingFilter.ADAPTER_NAME_ATTRIBUTE`; since 1.2.0 the message of a named
> client carries it in place of the URL (`Adapter http exchange GET involved-party -> 200 [...]`, ADR-0009
> amended). The level of `adapter-http-exchange` is a **filter** (§4.8's "one switch" is gone: WARN keeps the
> failures instead of switching the feature off).
>
> **Why.** The diary was the module-local generalisation the library became; keeping both meant a
> parallel implementation of the same filter, masking, fingerprint and `traceparent` parser (§4.7), maintained
> in one place and copied into another. The library carries them for the RestClient twin as well, with tests
> that pin the twins in lockstep.
>
> §1 describes the module as it is. §2–§3 of the record were history already; §4–§8 are now history too: the
> reasoning in them — why one event, why structured fields, why the emission must not be queued, why the bound
> on a body must be a parameter — is what the library implements, and the record keeps it as the account of how
> the requirements were arrived at. The current configuration is in
> [`webclient-config-guide.md`](webclient-config-guide.md) §4.1.

---

## 1. Context and Scope

### 1.1 What the module does

`ResilientWebClientFactory` hands the host one `ResilientWebClient` per mesh endpoint (`app.mesh.endpoints[]`),
each calling a local sidecar process that routes the request to its backend system. The host's own code - an
adapter class of its own, a service - describes a call with `get` or `post` and receives a cold `Mono`; a
coroutine caller bridges with `awaitSingle()`.

Under the hood every client is a clone of Boot's `WebClient.Builder` bean on the backend's HTTP engine - Reactor
Netty by default; `app.webclient.engine` can put a backend on the JDK client, Jetty or Apache HttpClient 5
instead ([configuration guide](webclient-config-guide.md) §1.6) - and applies the Resilience4j primitives as
reactive OPERATORS — `CircuitBreaker( TimeLimiter( Bulkhead( call ) ) )` — rather than as annotations.

What is logged, and by whom:

- **The exchange event** - one per HTTP exchange, on the logger `adapter-http-exchange` - is written by
  `legatium-webclient-logging`, whose filter sits on Boot's builder bean and so on every clone (§0).
- **The transport lines** - the per-call DEBUG lines, the error line with the exception class, the cancellation
  line and the buffer-limit line - are written by `ResilientWebClient` itself, under the caller's MDC and with
  the library's `adapter_name` and `adapter_url_template` fields, so they join the exchange event on sight.

### 1.2 The loopback constraint

All WebClient calls go to the sidecar over the loopback interface
(`localhost`). The TCP handshake is sub-millisecond, the network never
partitions, and bandwidth is effectively unbounded. The only variable in
the request lifecycle is the time the sidecar (and the system behind it)
takes to produce a response.

This affects logging in two ways:

- Per-request HTTP latency is dominated by the backend, so request rates can
  be high even when individual requests are slow.
- Blocking I/O on the Netty event loop is *especially* costly: with only a
  handful of event loops serving hundreds of multiplexed requests, blocking
  on a synchronous log write stalls many concurrent flows at once.

### 1.3 The appender constraint — and its expiry

For most of this guide's history the constraint read: *Logback's
`AsyncAppender` is not available in the deployment environment; log calls go
directly to a synchronous appender that performs blocking I/O on the thread
that calls `LOGGER.info(...)`.* That single sentence is what shaped the whole
design — the application had to provide the asynchrony itself, because the
logging library would not.

**That constraint does not hold for this module.** It leaves the queueing to
the host's appender: an asynchronous appender (Logback's `AsyncAppender` or a
drop-in replacement) queues events and hands them on on its own worker, so the
module keeps no application-side queue of its own; §4.3 of the
[record](logging-history.md) tells of the queue the superseded design had and
why it went. One event per exchange, MDC restoration and level resolution
before the event is built - now legatium's - did not follow from this
constraint.

What did NOT change is the reason the design cares about the calling thread at
all: emission happens on a Netty event loop, and the appender's queue offer is
where any remaining cost sits. The constraint moved one layer down; it did not
disappear.
