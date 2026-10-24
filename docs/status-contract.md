# The status contract

Only a 2xx answer with a body gives the caller a value; every other answer ends as an error. The circuit breaker
judges only whether the backend answered, never whether the answer suited the adapter.

The status contract fixes, for both client stacks, what becomes of an HTTP answer: a value for the caller or an
error, and how the breaker counts it. It holds alike for `ResilientRestClient` (blocking) and `ResilientWebClient`
(reactive), on all eight engines.

Three rules carry it:

- **Only a 2xx is a body.** Since 2026-10-24 this covers 1xx and 3xx as well; before, they were decoded as a body.
- **A value or an error.** `get`/`post` return exactly one `T` or fail - never `null`, never an empty `Mono`. An
  empty 2xx body becomes an `EmptyResponseBodyException`.
- **Answered means healthy.** A benign 4xx counts as a success for the breaker; decoding, validation and
  empty-body failures arise after the resilience layer and do not count there.

## The contract at a glance

Every row holds for both stacks; where the types differ, the blocking one stands before the slash, the reactive one
after it.

| Answer | Caller gets | Exception (blocking / reactive) | Breaker | legatium event |
|---|---|---|---|---|
| 2xx with a body that fits the type | the value | - | success | `success` (INFO) |
| 2xx without a body (204, empty 200) | an error | `EmptyResponseBodyException` / the same | success | `success` (INFO) |
| 2xx, body does not fit the type | an error | Jackson parse failure / the same | success | `success` (INFO) |
| 2xx, body violates its Bean Validation (`fail-on-violation`) | an error | `ConstraintViolationException` / the same | success | `success` (INFO) |
| 2xx, body past `max-in-memory-size` | an error | `ResponseBodyLimitExceededException` / `DataBufferLimitException` | ignored | `success` (INFO) |
| 1xx, 3xx (no redirect is followed) | an error | `RestClientResponseException` / `WebClientResponseException` | failure | by its status; not verified for 3xx |
| benign 4xx (400, 404, 409, ...) | an error | `HttpClientErrorException.<Status>` / `WebClientResponseException.<Status>` | **success** | `rejected` (INFO; WARN for 401, 403) |
| 408, 429 | an error | as a 4xx | failure | `rejected` (WARN) |
| 5xx | an error | `HttpServerErrorException.<Status>` / `WebClientResponseException.<Status>` | failure | `failure` (WARN) |

The error bodies of a 4xx or 5xx ride on the exception; the blocking client cuts them at `max-in-memory-size`
rather than refusing them, so a 5xx stays a 5xx for the breaker whatever size its error page has. Transport failures
(timeouts, dropped connections) and cancellations by the caller are not part of the status contract; they are
described in the blocking module's `timeouts-bulkheads-threads.md` and in [HTTP engine behaviour](engine-behaviour.md).

## Where the decision is taken

The status decision is taken inside the resilience layer; decoding and validation come after it. That is why the
breaker sees the status but never the content.

```text
                 +-------------- resilience layer ---------------+
                 |        bulkhead . time limiter . breaker      |
                 |                                               |
  get / post --->|  HTTP exchange --->  only 2xx? ---- yes ------+---> after the layer:
                 |  (body read under         |                   |      empty? decode, validate
                 |   the limit)              no                  |        |               |
                 |                           v                   |      fits        does not fit
                 |                 error carrying its status     |        v               v
                 |                 (1xx, 3xx, 4xx, 5xx)          |    the value     error; the breaker
                 |                                               |                  already counted a
                 |  the breaker counts here:                     |                  success
                 |  2xx and benign 4xx = success,                |
                 |  everything else = failure                    |
                 +-----------------------------------------------+
```

On the blocking stack, `ResilientRestClient.exchange` takes the decision on the worker thread of the
`BlockingResilienceDecorator`; on the reactive stack it sits in `retrieve().onStatus(...)` inside the decorated
`Mono`. The empty-body check, `objectMapper.readValue` and the validation run behind `resilience.decorate` on both
stacks.

## The special cases

### 204 and the empty 200

An empty 2xx body fails as an `EmptyResponseBodyException` naming the backend and the expected type; the breaker
counts a success. On the blocking stack `decode` sees zero bytes and throws; on the reactive stack `bodyToMono`
completes empty and `switchIfEmpty` turns that into the same error. Without the rule it would be a Jackson failure on
the blocking stack and, on the reactive one, an empty `Mono` that surfaces only in the caller's `awaitSingle()` as a
`NoSuchElementException` naming nothing. The `*.response.body.size` summary records a 0.

### 1xx and 3xx

None of the eight engines follows a redirect, so a 3xx is never the answer the adapter asked for. Since 2026-10-24 it
fails as the stack's response exception with its status, and the breaker counts a failure. On the blocking stack
`exchange` tests `!status.is2xxSuccessful`; on the reactive stack an `onStatus` handler adds what `retrieve()` alone
does not do. A final 1xx answer hardly occurs in practice; it falls under the same rule only so that the rule reads
"only 2xx".

### Benign 4xx versus 408, 429 and 5xx

A 4xx proves that the backend is alive and answered promptly; it counts as a success, even though the caller sees an
exception. 408 and 429 are the exception: they are operational (a timeout, an overload) and count as a failure, like
a 5xx. The line is drawn in `BreakerVerdicts` (`veredium-core`) and holds for both stacks.

### A body past the limit

A body past `max-in-memory-size` is a refusal of the client, not a fault of the backend: the breaker ignores it. On
the blocking stack the engine aborts the exchange instead of reading the rest; the error line names the limit.

### Parse and validation failures

A body that does not fit its type, or violates its Bean Validation, is a contract problem between adapter and
backend. Both failures arise after the resilience layer, so the breaker counts the exchange as a success. With
`validation.fail-on-violation: false` a violation is only reported at WARN and the value is returned.

## Why the contract is cut this way

The breaker is to judge the health of the backend, the caller the content of the answer - so the contract keeps the
two questions apart.

- **Answered means healthy** (`veredium-restclient/docs/decisions.md`, D2). A breaker that ignores benign 4xx fills
  its window, on a 4xx-heavy backend, with the few genuine faults and opens at a nominal 100 % failure rate. A 4xx
  counted as a success keeps the window honest.
- **What the client refuses does not count.** A body past the limit, a full bulkhead, a cancellation by the caller
  say nothing about the backend; the breaker ignores them.
- **Contract failures sit behind the breaker.** Decoding and validation run after the resilience layer, so a schema
  problem never opens the circuit against a healthy backend.
- **Only a 2xx is a body.** An answer the adapter never wanted must not slip through as a value. A 3xx decoded as a
  body ended in a misleading empty-body or parse failure - or, with JSON that happened to fit, in a wrong value and no
  error.
- **A value or an error.** The API returns `T`, not `T?`; an empty result would be an answer without an error line
  and without a name.
- **Both stacks alike** (ADR-0001). A host that runs both modules in one context (ADR-0003) sees the same verdict
  and the same meaning for the same answer.

## The change of 2026-10-24

Until 2026-10-24, 1xx and 3xx were a body; since then they are errors carrying their status, which the breaker
counts as a failure. It resolves finding 11 of `docs/assessment/DEFECT_ANALYSIS-2026-10-24T10-08-59.md`; the
CHANGELOG records it under "Changed - Client API", and tests pin it on both stacks and every engine.

What changes for a host:

- **Callers:** whoever handled an `EmptyResponseBodyException` or a parse failure for a 3xx now gets a
  `RestClientResponseException` or `WebClientResponseException` with the status. A `catch` of those types now
  catches a 3xx as well.
- **Breaker:** when 3xx answers pile up, the breaker opens - a misrouted or redirecting path shows in monitoring
  instead of passing as healthy.
- **Logs:** the client's error line names the status. How legatium classifies the exchange event of a 3xx has not
  been verified.

| Option | For | Against | Status |
|---|---|---|---|
| 1xx/3xx as an error, the breaker counts a failure | a clear message; a misconfiguration shows in the breaker | a change of behaviour; many 3xx open the breaker | chosen |
| 1xx/3xx as an error, the breaker ignores it | a clear message, no effect on the breaker | a misconfiguration stays invisible to the breaker; needs an addition to the verdicts | rejected |
| the old contract, only pinned by a test | no change of behaviour | misleading errors; the breaker counts a success | rejected |

## Limits and references

The API has no call without a return value. Where a 204 is the expected answer (a DELETE, a "command accepted"), the
adapter has to catch the `EmptyResponseBodyException` and read it as a success - the way the example adapters read a
404 as `null`. A variant without a body (say, `postForNoContent`) would be a possible extension; none is planned.

Open question: how does legatium classify the exchange event of a 3xx (`success` or `rejected`)?

| What | Where |
|---|---|
| The status decision, blocking | `ResilientRestClient.exchange` / `statusException` (`veredium-restclient`) |
| The status decision, reactive | `ResilientWebClient`, `retrieve().onStatus(...)` (`veredium-webclient`) |
| The breaker verdicts | `BreakerVerdicts`, `MeshResilienceCustomizers.kt` (`veredium-core`) |
| The empty body | `EmptyResponseBodyException` (`veredium-core`) |
| Tests, 3xx | "should fail a redirect as a response exception with its status and count it as a failure" (both stacks, every engine) |
| Tests, empty body | "should fail a 2xx without a body with the named empty-body exception, not charging the breaker" (blocking); "should fail a 204 answer with EmptyResponseBodyException after the breaker recorded a success" (reactive) |
| Tests, 4xx/5xx | "should surface a non-2xx response as ..." (both stacks); `*VerdictsTest` |
| Guides | `restclient-vs-webclient.md`, `timeouts-bulkheads-threads.md` (the classification table), `webclient-config-guide.md` |
