# rest-client — decisions this module relies on

> **Ownership & baseline.** Maintained by **Dirk Haase**. Verified against the code at commit `cd5bceb4`
> (2026-10-19). Where this text and the code disagree, the code wins.

`rest-client` has no `docs/adr/` of its own. Its own decisions are recorded where they are argued — the POM
comment (a self-contained blocking twin, no reactive dependency), `restclient-vs-webclient.md` (what maps to
the twin and what deliberately differs, including §8 on the customizer wiring), `timeouts-bulkheads-threads.md`
(the hand-off, the ladder, the bulkhead over an unbounded executor) and the KDoc of the classes concerned. Three
decisions, however, were made elsewhere and are only *applied* here; the shipped `application.yml` and the
guides cite them by the labels below so that a reader of this module does not have to know the other modules'
numbering, and so that the module stays readable in a host that has none of them.

| Label | Decision | Where it was made | What this module takes from it |
|---|---|---|---|
| **D1** | Bulkheads are a **pool partition and a backstop**, not a load-steering mechanism; callers bound their own concurrency before reaching them, and a `BulkheadFullException` on a known path is a signal that a caller's bound is mis-sized. Breakers built outside the module carry their own ignore list. | [tool-box, `backfill/docs/adr/006-backfill-rate-pacing-and-concurrency-admission.md`](https://github.com/dhaase/tool-box/blob/main/backfill/docs/adr/006-backfill-rate-pacing-and-concurrency-admission.md), sections D3 and D4 (the project this module was ported from) | The bulkhead block of `application.yml` (`max-concurrent-calls: 25`, `max-wait-duration: 0`, the sum-against-the-pool invariant pinned by `ResilienceConfigContractTest`); `timeouts-bulkheads-threads.md` §4. |
| **D2** | **Answered means healthy**: a benign 4xx is recorded as a *success* on the circuit breaker, not ignored, because an ignored class empties the sliding window on a 4xx-dominated backend (measured on 2026-07-31: 41,903 filtered-out benign 4xx left five 504s as the whole window, opening the breaker at a nominal 100 %). | [tool-box, `sync-bridge/docs/adr/032-circuitbreaker-answered-means-healthy.md`](https://github.com/dhaase/tool-box/blob/main/sync-bridge/docs/adr/032-circuitbreaker-answered-means-healthy.md) (the project this module was ported from) | The record predicate `circuitBreakerCustomizer` installs on every mesh alias (`resilience/MeshResilienceCustomizers.kt`), driven by `HttpExchangeErrorPolicy`. |
| **D3** | The exchange event's **body logging is a mode, not a switch** (`never` / `on-failure` / `always`), and a 4xx answer is logged as `rejected` at INFO except for 401/403/408/429. | legatium, ADR-0006 and ADR-0012 — the documentation of the external artifact `eu.inqudium:legatium-restclient-logging` (`docs/adr/` of the legatium project) | The `adapter-logging` block of `application.yml`; the severity split in `HttpExchangeErrorPolicy`, which deliberately does not reach into the library's classification. |

When one of the source decisions changes, this table is where the change lands first; the YAML comments and
the guides point here rather than at the source documents.

**Related:** [what differs from the reactive twin](restclient-vs-webclient.md) ·
[timeouts, bulkheads and threads](timeouts-bulkheads-threads.md) · [metrics](metrics.md)
