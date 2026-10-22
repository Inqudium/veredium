# HTTP engine behaviour

Each client module lets a host choose the HTTP engine underneath (`app.restclient.engine`,
`app.webclient.engine`): Apache HttpClient 5, the JDK's `java.net.http` client, Jetty or Reactor Netty. Spring's
adapters make the four look alike at the API. Below the API, each one has defaults and habits of its own, some of
them surprising, and Spring's adapters add a few more.

This page collects the ones that matter for veredium: what the engine does, what that would mean for a call
through the mesh, and what the module does about it. Each entry was found by a test or by reading the library's
source. The code lists the workaround and, where it rests on a library's behaviour, an `Assumption:` comment, so
`grep -rn "Assumption:" */src/main` is the checklist for a dependency bump. The test named in an entry is the
one that turns red if the behaviour changes.

Checked against the versions the Spring Boot BOM resolves today:

| Library | Version |
|---|---|
| Spring Framework (`spring-web`) | 7.0.9 |
| Apache HttpClient 5 / HttpCore 5 | 5.6.4 / 5.4.3 |
| Jetty client | 12.1.12 |
| Reactor Netty / Reactor Pool | 1.3.7 / 1.2.7 |
| `java.net.http` | JDK 21+ (the build runs on 25 and 26) |

## One setting, different meanings

Some settings are mapped onto every engine, but an engine reads its knob in its own way. The module guides have
the full mapping tables; this table lists only the settings where the difference changes the outcome.

| Setting | Apache | JDK | Jetty | Reactor Netty |
|---|---|---|---|---|
| response timeout (ladder rung) | socket timeout of **each read** | blocking: one deadline over head **and body**; reactive: head only | blocking: the request's **total** timeout, queueing and connect included; reactive: per-request idle timeout | longest **silence** between reads |
| `max-connections` | classic: total cap and per route; async: per route | **no bound** at all | per destination | per remote address **and client configuration**: one pool per backend in `veredium-webclient`, one shared pool in `veredium-restclient` |
| `pending-acquire-timeout-ms` | classic: applied; async: **not applied** (see below) | nothing waits for a connection | no time bound; a queued request waits within its timeout | applied |
| `pool.max-idle-time: 0` | every idle connection closed at each sweep | not applied | **no idle timeout** | **every idle connection evicted at once**: no keep-alive |
| closing a response before its body was read to the end | Spring's adapter **reads the rest** | blocking: Spring's adapter **reads the rest** | the request stays in flight | Spring's adapter **reads the rest** |

What the module does:

- `pool.max-idle-time` must be positive on both stacks. A zero has no meaning the engines agree on, so the
  binding refuses it and names the key.
- Every engine on the blocking side drops a response that was not read to the end instead of reading the rest:
  a body refused at the buffer limit releases its worker, its bulkhead permit and its connection at once. Pinned
  per engine by `RestClientEngineIntegrationTest` ("should refuse a dripping body past the buffer limit at once"):
  against a backend that keeps dripping, Apache took 30 s and ended at the TimeLimiter before the fix.
- The startup guard checks the bulkheads against each pool the client factories report
  (`MeshConnectionPools`): one per engine, one per backend for Reactor Netty in the reactive module, none for the
  JDK client.
- Each engine in use writes one startup line saying what it applies and what it does not.

## Apache HttpClient 5 (classic, `veredium-restclient`)

- **Closing a response reads the rest of its body.** Spring's `HttpComponentsClientHttpResponse.close()` consumes
  the entity, and Apache's content streams (`ContentLengthInputStream`, `ChunkedInputStream`) read the rest of the
  message in their own `close`, so the connection can be reused. For a large or slow body, the worker reads for as
  long as the peer takes. Past the budget, a client-side refusal (buffer limit, which the breaker ignores) becomes
  a `TimeoutException` the breaker records. *Module:* the request factory captures the Apache request, and a
  response closed before its end cancels it first (`HttpUriRequestBase.cancel()` → `InternalExecRuntime.cancel()`
  → the connection is discarded). See `ApacheEngineTransport`, "A body not read to its end".
- **Pool limits fall back silently.** The per-route default is five connections, whatever the total. A
  `maxConnTotal` of zero or less is ignored, and the pool runs on its default of 25. *Module:* per route = total
  (every endpoint behind the sidecar is one route); `max-connections` must be at least 1 at binding.
- **On by default, off here:** automatic retry of a failed idempotent request, redirects and the cookie store. A
  retry would spend the mesh ladder's budget twice behind the TimeLimiter's back. Content compression stays on
  (`Accept-Encoding: gzip, x-gzip, deflate`).
- **`closeIdle(0)` closes every idle connection** at each eviction sweep, which is one more reason the binding
  refuses a zero idle time.
- **Interrupts.** On the decorator's virtual-thread workers, a blocking socket read reacts to an interrupt by
  closing the socket: the read fails at once with `SocketException: Closed by interrupt`, and Apache discards the
  connection. A pool lease and a connect are interruptible too.
- **Pool meters cannot be deregistered.** The `ConnPoolMeters` binder (httpclient5-observation) reads the pool off a
  client *builder* and hands back nothing to deregister with. *Module:* it binds once, over a throwaway builder,
  reads off the registry what it added, and removes those meters on shutdown.
- **`evictIdleConnections` on the builder would start one thread per client**, i.e. one per endpoint, all for the
  same shared pool. *Module:* one eviction schedule per pool.

## Apache HttpClient 5 (async, `veredium-webclient`)

- **Spring's connector drops the exchange's `Future`.** Cancelling the subscription (TimeLimiter, caller) would
  leave the exchange running until the response or the response timeout. *Module:* `AbortableAsyncClient`
  records each exchange's `Future` and cancels it. `AbortableExchange` handles an abort that arrives before the
  `Future` does; `AbortableExchangeTest` pins both orders.
- **The pending-acquire timeout is never enforced by itself.** The async pool (`StrictConnPool`) checks a waiting
  lease's deadline only on the next lease or release. `validatePendingRequests()`, which would check it on its
  own, is called nowhere in httpclient5. *Module:* the startup line lists `pending-acquire-timeout-ms` under "does
  NOT apply"; the TimeLimiter bounds the wait. (The classic pool of the blocking module applies it, as its
  `max-connections` test shows.)
- **Timeouts fire late.** Apache checks socket and connect timeouts when its selector wakes up, every second by
  default. That is later than the headroom between the response timeout and the TimeLimiter. *Module:* the select
  interval is half that headroom.
- **Shutdown with exchanges in flight.** The graceful reactor shutdown and a hard close both close the
  connections of running exchanges without completing their callbacks, so the caller's `Mono` would wait for its
  TimeLimiter. Closing a reactor while its workers are still shutting down closes their selector under them, which
  Apache logs at ERROR as a `ClosedSelectorException`. *Module:* in-flight exchanges are cancelled on release (one
  WARN with their number), and the reactor is closed hard only if it does not end by itself.
- **`IdleConnectionEvictor.shutdown()` only interrupts its thread.** `awaitTermination` with a zero timeout joins
  forever. *Module:* bounded wait after the shutdown.
- **HTTP/2 negotiation** is forced off (`HttpVersionPolicy.FORCE_HTTP_1`), as on Reactor Netty.

## JDK `java.net.http`

- **`Host` is a restricted header.**
  - Blocking: Spring's request drops it silently, and the JDK sends the URI's authority instead.
  - Reactive: the JDK refuses the header outright.
  - Behind the sidecar, either way would route by `localhost:<port>`. The only switch is the JVM-wide
    `-Djdk.httpclient.allowRestrictedHeaders=host`, which the JDK (and Spring) read **once**, when their HTTP
    classes initialise.

  *Module:* the factory refuses to start with this engine where it matters and names the flag. It never sets
  the flag itself: that would change every `java.net.http` client of the process.
  `JdkEngineRestrictedHostHeaderTest` runs in a JVM of its own.
- **No connection bound, no pool statistics, no per-client pool knobs.** The pool settings exist only as JVM-wide
  system properties (`jdk.httpclient.connectionPoolSize`, `jdk.httpclient.keepalive.timeout`, …). *Module:* none
  is set; the startup line says what does not apply. Size the bulkheads knowing that nothing bounds the
  connections behind them.
- **Behaviour with only JVM-wide switches:** a refused connect is retried once (within the same connect timeout),
  and an idempotent GET whose pooled connection the peer had closed is re-sent on a fresh one.
- **Defaults switched off per client:** the `h2c` upgrade offer on every plaintext request (pinned to HTTP/1.1),
  redirects, and the default proxy selector, which honours the `http.proxyHost` family of system properties.
- **Compression is Spring's, not the JDK's.** The JDK client neither asks for nor decodes compressed bodies. On
  the blocking side Spring's `JdkClientHttpRequestFactory` does both by default (`Accept-Encoding: gzip, deflate`,
  decoded in the adapter); the reactive `JdkClientHttpConnector` does neither, so on `veredium-webclient` this
  engine runs uncompressed (its startup line lists gzip under "does NOT apply").
- **Closing a response reads the rest of its body (blocking).** Spring's `JdkClientHttpResponse.close()` drains
  the stream (`StreamUtils.drain`) up to the read deadline. *Module:* the response's abort closes the JDK's body
  stream first, which cancels the exchange and drops its connection.
- **The response timeout means two different things.** On the blocking side, Spring's read timeout is a deadline
  from sending until the body is closed (head and body together). On the reactive side, `HttpRequest.timeout`
  bounds the wait for the response head only.
- **`shutdownNow()` returns before the client has terminated.** Its threads end asynchronously. *Module:* a
  bounded wait for `isTerminated`.
- **Reactive bodies arrive on `ForkJoinPool.commonPool`**, the pool `sendAsync` hands its completions to, not on a
  thread of the client's own (`WebClientEngineIsolationTest` reads it there).

## Jetty client 12

- **Spring adds headers instead of setting them**, next to Jetty's own. A peer would see two `User-Agent`
  headers. *Module:* Jetty's own `User-Agent` is removed.
- **Spring only stops waiting (blocking).** On an interrupt or a timeout, Spring's Jetty request stops waiting
  for the response, but the Jetty request stays in flight and holds its connection until the response arrives or
  its total timeout expires. *Module:* the request factory captures the Jetty request (`HttpClient.newRequest(URI)`,
  on the calling thread) and aborts it when the exchange fails, a body read fails, or the response is closed early.
- **Spring's read timeout becomes the request's total timeout (blocking):** queueing for a connection, connect,
  head and body together. *Module:* the request's idle timeout is set to the same value as well, so a silent
  connection fails at the rung, as on Apache.
- **Every request goes into the destination's queue first**, including the one about to get a connection, and
  stays there until it is on one. A queue capacity equal to "how many may wait" rejects a burst on a cold pool at
  that number. *Module (reactive):* capacity = `max-connections` + `pool.pending-acquire-max-count`; a count of
  `0` is still refused, because Jetty cannot express "nobody waits". Blocking: Jetty's default of 1024.
- **No time bound for a queued request** other than the request's own timeout.
- **`idleTimeout = 0` means no idle timeout**; idle connections close on their own timers (there is no eviction
  interval). The pool's `maxDuration` is checked when a connection is **leased**, not in the background.
- **`stop()` ignores an interrupt**: it waits for Jetty's own threads, whose `QueuedThreadPool` is **non-daemon**
  and can keep a JVM alive. *Module:* an interrupted shutdown thread starts the stop on a thread of its own and
  leaves it to complete; an engine created before a failed construction is stopped before the failure is rethrown.
- **No Micrometer binder** for the pool.

## Reactor Netty

- **Global resources by default.** Without its own resources, a client runs on the JVM-wide `HttpResources`,
  shared with every other Reactor Netty client of the host (and managed by Boot's `ReactorResourceFactory` for
  the host's own). Even with its own loops (`runOn`), the default resolver still comes from `HttpResources`.
  `.metrics(true)` with the default registrar registers in Micrometer's **global** registry. *Module:* own
  `LoopResources`, own `ConnectionProvider`, an explicit resolver; on the blocking side a registrar that
  registers the pool gauges in the host's registry. Pinned by "should register Reactor Netty's pool gauges in the
  host registry and keep off its global resources".
- **One sub-pool per remote address *and client configuration*** (`PoolKey(remoteAddress, config.channelHash())`).
  Two `HttpClient`s with different settings, for example a different `responseTimeout`, do not share connections.
  The reactive module builds one client per backend, so `max-connections` applies per backend there. The blocking
  module shares one client and sets the timeout per request, so its pool is shared.
- **`maxIdleTime(0)` evicts every idle connection**: the eviction predicate is `idleTime >= maxIdleTime`, so
  nothing is ever reused. `maxLifeTime(0)` means "disabled" (reactor-pool 1.2.7); `evictInBackground(0)` means
  "off".
- **One automatic retry** of a request whose pooled connection was closed while sending. *Module:* off
  (`disableRetry`). The pending-acquire queue defaults to twice `max-connections`.
- **Spring's body stream ignores an interrupt (blocking).** `SubscriberInputStream` parks in a loop that does
  not react to the interrupt, so an interrupted worker would spin until the next chunk, the end of the body or the
  response timeout. Closing the response drains the rest of the body. *Module:* each body read runs on a
  short-lived virtual thread while the worker waits for it interruptibly; on an interrupt, or a close before the
  end, the stream is closed instead.
- **Disposal.** `disposeLater()` on a pool built without `disposeTimeout` is not a graceful shutdown: it closes
  the idle connections and leaves an in-flight call to finish on its own. Disposing the event loops closes every
  channel on them. `block(Duration)` on a disposal reports its expiry as an `IllegalStateException`. *Module:* the
  loops go after the pool; an expiry is a WARN line, not an exception out of a destroy callback.

## Reactor (the reactive stack, every engine)

- **Errors after a cancellation go to `onErrorDropped`.** The JDK, Jetty and Apache adapters may report an
  exchange's failure after its subscription was cancelled. Reactor logs it at ERROR as a dropped error, as if it
  had been lost. *Module:* only errors dropped after this exchange was cancelled are logged at DEBUG ("ended after
  cancellation"); any other dropped error goes to the caller's own hook or to Reactor's global handling.

## Test infrastructure

The engines' behaviour shows only against a backend that behaves badly in a controlled way. Three test tools
behave differently from what one would expect:

- **WireMock's `withChunkedDribbleDelay` does not drip.** The body goes into the server's output buffer, and the
  head reaches the wire only once that buffer is full. A "dripping" body is often a delayed one. The tests that
  need a body that is still arriving use a raw-socket `DrippingBackend` instead.
- **WireMock's fixed delays occupy a container thread** even after the client went away. Abandoned exchanges with
  long delays pile up across a test class, so the engine tests run WireMock with asynchronous responses.
- **A connect timeout needs a listening socket that does not answer.** A `ServerSocket` with a backlog of 1 whose
  accept queue has been filled makes Linux drop further SYNs, so the connect hangs until the client gives up
  (`BlackholeBackend` in the blocking module, `UnansweredConnectBackend` in the reactive one, which skips the
  case where the platform does not drop them).
