# HTTP engine behaviour

Each client module lets a host choose the HTTP engine underneath (`app.restclient.engine`,
`app.webclient.engine`): Apache HttpClient 5, the JDK's `java.net.http` client, Jetty or Reactor Netty. Spring's
adapters make the four look alike at the API. Below the API, each engine has defaults and habits of its own, and
Spring's adapters add a few more.

This page goes topic by topic. Each topic starts with the behaviour a call through the mesh needs, which is the
behaviour the module accepts as correct. Then, one engine at a time, it gives the engine's default with the version
it was checked against, and what the engine does under veredium. Every statement about a default was read in the
library's source or seen in a test. In the code, a workaround that rests on a library's behaviour carries an
`Assumption:` comment, so `grep -rn "Assumption:" */src/main` is the checklist for a dependency bump. The tests
named below turn red if the behaviour changes.

Versions, as the Spring Boot BOM resolves them today:

| Engine | Library | Version |
|---|---|---|
| Apache, classic (`veredium-restclient`) | `httpclient5` / `httpcore5` / `httpclient5-observation` (pool meters) | 5.6.4 / 5.4.3 / 5.6.4 |
| Apache, async (`veredium-webclient`) | `httpclient5` / `httpcore5` / `httpcore5-reactive` (Spring's reactive adapter) | 5.6.4 / 5.4.3 / 5.4.3 |
| JDK | `java.net.http` | JDK 21+ (the build runs on 25 and 26) |
| Jetty | Jetty client / jetty-io | 12.1.12 |
| Reactor Netty | Reactor Netty / Reactor Pool | 1.3.7 / 1.2.7 |
| (all) | Spring Framework `spring-web`, the adapters over the engines | 7.0.9 |

"Blocking" means `veredium-restclient` (Spring's `ClientHttpRequestFactory` adapters), "reactive" means
`veredium-webclient` (Spring's `ClientHttpConnector` adapters).

## 1. Connect timeout

**Expected.** A connect gives up after `app.mesh.defaults.transport.connect-timeout-ms` (100 ms by default). The
TimeLimiter's margin over the response timeout is computed from this value; a connect that takes longer than the
margin assumes makes the mesh ladder wrong.

### Apache HttpClient 5

**Default (httpclient5 5.6.4, httpcore5 5.4.3).** `ConnectionConfig` connect timeout: 3 minutes.

**With veredium.** `ConnectionConfig.connectTimeout` = `connect-timeout-ms`, on both stacks.

### JDK `java.net.http`

**Default (JDK 21+).** No connect timeout; the operating system's applies. A refused connect is retried once,
within the same timeout, and only a JVM-wide property switches that off.

**With veredium.** `HttpClient.Builder.connectTimeout` = `connect-timeout-ms`, on both stacks. The retry stays: it
has no per-client switch.

### Jetty

**Default (12.1.12).** `ClientConnector` connect timeout: 5 seconds.

**With veredium.** `HttpClient.connectTimeout` = `connect-timeout-ms`, on both stacks.

### Reactor Netty

**Default (1.3.7).** Netty's channel default for `CONNECT_TIMEOUT_MILLIS`: 30 seconds.

**With veredium.** `ChannelOption.CONNECT_TIMEOUT_MILLIS` = `connect-timeout-ms`, on both stacks. The binding
bounds the value at `Int.MAX_VALUE`, the option's type.

Pinned per engine by "should give up a connect at the connect timeout" (`RestClientEngineIntegrationTest`) and its
reactive twin, against a socket whose full accept queue makes the kernel drop the connect.

## 2. Response timeout

**Expected.** Each endpoint has its own response timeout, the transport rung of the mesh ladder
(`individual-request-timeout-ms` × attempts + margins). It sits above the sidecar's global timeout and below the
endpoint's TimeLimiter budget. It ends a call whose backend went silent, and so frees the worker before the
TimeLimiter has to. Two backends on the same engine keep their own values.

### Apache HttpClient 5

**Default (httpclient5 5.6.4, httpcore5 5.4.3).**
- Blocking (classic): no response timeout of its own; reads use the socket timeout, 3 minutes by default
  (`SocketConfig`), applied to **each read**.
- Reactive (async): socket timeout 0 (none). Apache checks socket and connect timeouts only when its selector
  wakes up, every second by default (`IOReactorConfig.selectInterval`).

**With veredium.** The endpoint's response timeout is the request's `RequestConfig.responseTimeout`, per read: per
endpoint client on the blocking side, per connector on the reactive side. Reactive: the select interval is half
the fixed TimeLimiter headroom (50 ms), so the timeout fires inside it.

### JDK `java.net.http`

**Default (JDK 21+).** No timeout (`HttpRequest.timeout` unset).

**With veredium.**
- Blocking: Spring's read timeout, a deadline from sending until the response body is closed: head **and body**.
- Reactive: `HttpRequest.timeout`, which bounds the wait for the response **head** only.

### Jetty

**Default (12.1.12).** No request timeout; the connection idle timeout is 30 seconds (`ClientConnector`).

**With veredium.**
- Blocking: Spring's read timeout becomes the request's **total** timeout (queueing for a connection, connect,
  head and body). The request's idle timeout is set to the same value, so a silent connection in use fails at the
  rung, as on Apache.
- Reactive: the request's idle timeout, the same reading as Reactor Netty's.

### Reactor Netty

**Default (1.3.7).** No response timeout.

**With veredium.** `HttpClient.responseTimeout`: the longest silence between reads until the response is
complete.

Pinned by "should give each backend on a shared engine its own response timeout" and "should fail with the
transport's response timeout when it is the tighter bound" (blocking), and the reactive response-timeout case with
two backends per engine.

## 3. How many connections

**Expected.** At most `app.mesh.defaults.transport.max-connections` (100) connections to the sidecar. The
bulkheads are sized against that bound: `sum(max-concurrent-calls)` over the backends on one pool should not exceed
it, or the excess queues for a connection instead of being shed with a named `BulkheadFullException`. The startup
guard warns about such a configuration. Since each engine has its own pool, it checks each pool that a client
factory reports (`MeshConnectionPools`).

### Apache HttpClient 5

**Default (httpclient5 5.6.4, httpcore5 5.4.3).** Pool of 25 connections in total and **5 per route**, on both stacks. A `maxConnTotal` of zero
or less is ignored, and the pool keeps its default.

**With veredium.**
- Blocking: `maxConnTotal` and `maxConnPerRoute` = `max-connections`. Every endpoint behind the sidecar is one
  route, so per route = total.
- Reactive: `maxConnPerRoute` = `max-connections`, with no total cap across routes (like Reactor Netty).
- The binding requires at least 1. One pool per engine for the guard.

### JDK `java.net.http`

**Default (JDK 21+).** **No bound**: a connection is opened per concurrent exchange. The pool can only be tuned
JVM-wide (`jdk.httpclient.connectionPoolSize`, …), which changes every `java.net.http` client of the process.

**With veredium.** Not applied: no property is set, and the startup line says so. The guard does not check this
engine. Size the bulkheads knowing that nothing bounds the connections behind them.

### Jetty

**Default (12.1.12).** 64 connections per destination.

**With veredium.** `maxConnectionsPerDestination` and the pool's size = `max-connections`. Every backend behind the
sidecar is one destination; one pool per engine for the guard.

### Reactor Netty

**Default (1.3.7).** `max(available processors, 8) × 2` connections **per pool key**. The key is the remote address
**and the client configuration** (`PoolKey(remoteAddress, config.channelHash())`): two `HttpClient`s with
different settings, a different `responseTimeout` for instance, never share connections.

**With veredium.** `ConnectionProvider.maxConnections` = `max-connections`.
- Blocking: one `HttpClient` for all backends; the timeout is set per request. The pool is shared, so one pool
  for the guard.
- Reactive: one client per backend, hence one sub-pool per backend. `max-connections` applies **per backend**,
  and the guard gets one pool per backend.

Pinned by "should bound the connections by max-connections where the engine applies it" (blocking: two backends,
one connection) and the reactive "Pool limits on every engine" cases. The pool reports to the guard are pinned by
the `connectionPools()` cases of `RestClientEngineIntegrationTest` and `WebClientEngineConfigTest`.

## 4. Waiting for a connection

**Expected.** A call that finds every connection in use waits at most
`app.mesh.defaults.transport.pending-acquire-timeout-ms` (200 ms), and the TimeLimiter's margin assumes exactly
that. Ideally, though, nobody waits: the bulkheads shed load before the pool's queue builds up.

### Apache HttpClient 5

**Default (httpclient5 5.6.4, httpcore5 5.4.3).** Connection-request timeout: 3 minutes. The pending queue is bounded in time only, not in
count.
- Blocking (classic pool): the timeout is enforced.
- Reactive (async pool, `StrictConnPool`): a waiting lease's deadline is checked only when the next lease or
  release happens. `validatePendingRequests()`, which would check it on its own, is called nowhere in httpclient5,
  so on an idle pool a waiting call is never timed out.

**With veredium.**
- Blocking: `RequestConfig.connectionRequestTimeout` = `pending-acquire-timeout-ms`.
- Reactive: set as well, but the startup line lists it under "does NOT apply"; the TimeLimiter bounds the wait.

### JDK `java.net.http`

**Default (JDK 21+).** Nothing waits: there is no bound to wait for (§3).

**With veredium.** Not applied, and stated on the startup line.

### Jetty

**Default (12.1.12).** Requests queue per destination, at most 1024, with no time bound of their own. Every request
enters the queue first, including the one about to get a connection, and stays there until it is on one.

**With veredium.**
- Blocking: the request's total timeout (§2) bounds the wait; the queue keeps Jetty's 1024.
- Reactive: queue capacity = `max-connections` + `pool.pending-acquire-max-count`. A capacity equal to the count
  alone would reject a burst on a cold pool at half the concurrency Reactor Netty accepts. `-1` stays unbounded. A
  count of `0` is refused at startup: Jetty cannot express "nobody waits".

### Reactor Netty

**Default (1.3.7).** Pending-acquire timeout 45 seconds; the queue holds at most twice the connection count.

**With veredium.** `pendingAcquireTimeout` = `pending-acquire-timeout-ms`.
- Reactive: the queue is `pool.pending-acquire-max-count` (100).
- Blocking: the queue keeps Reactor Netty's default of 2 × `max-connections`, because `app.restclient` has no key
  for it (Apache has no counterpart).

## 5. Idle and old connections

**Expected.** A pooled connection is closed once it has been idle for `pool.max-idle-time` (30 s), and once it is
older than `pool.max-life-time` (5 min; `0` = unbounded), checked every `pool.evict-in-background` (30 s; `0` =
off). A zero idle time has no reading the engines agree on, so the binding refuses it on both stacks and names the
key.

### Apache HttpClient 5

**Default (httpclient5 5.6.4, httpcore5 5.4.3).**
- No eviction unless configured.
- `closeIdle(0)` closes **every** idle connection.
- `HttpClientBuilder.evictIdleConnections` starts a thread **per client**: here that would be one per endpoint,
  all for one shared pool.
- Time to live is unbounded.
- `IdleConnectionEvictor.shutdown()` only interrupts its thread, and `awaitTermination` with a zero timeout joins
  forever.

**With veredium.** One eviction schedule per pool (`closeExpired` + `closeIdle(max-idle-time)` every
`evict-in-background`); `ConnectionConfig.timeToLive` = `max-life-time`. Reactive: an `IdleConnectionEvictor`,
awaited for a bounded time on shutdown.

### JDK `java.net.http`

**Default (JDK 21+).** The keep-alive timeout is the JVM-wide `jdk.httpclient.keepalive.timeout`; there is no
per-client knob and no lifetime bound.

**With veredium.** Not applied, and stated on the startup line.

### Jetty

**Default (12.1.12).**
- Connection idle timeout 30 s; `idleTimeout = 0` means **no** idle timeout.
- Idle connections close on their own timers; there is no eviction interval.
- The pool's `maxDuration` is checked when a connection is **leased**, not in the background.

**With veredium.** `HttpClient.idleTimeout` = `max-idle-time`; the pool's `maxDuration` = `max-life-time`.
`evict-in-background` does not apply.

### Reactor Netty

**Default (1.3.7).** No idle limit, no lifetime limit (`-1`), no background eviction. `maxIdleTime(0)` evicts
**every** idle connection: the predicate is `idleTime >= maxIdleTime`, so nothing is ever reused. `maxLifeTime(0)`
means "disabled" (Reactor Pool 1.2.7).

**With veredium.** `maxIdleTime`, `maxLifeTime` and `evictInBackground` from `app.*.pool`.

## 6. Retries, redirects, cookies, protocol, proxy

**Expected.** One request on the wire per attempt. The sidecar owns retries: an in-app second attempt would spend
the ladder's budget twice behind the TimeLimiter's back. There are no redirects and no cookies. HTTP/1.1 goes
straight to the sidecar on loopback, with no proxy.

### Apache HttpClient 5

**Default (httpclient5 5.6.4, httpcore5 5.4.3).** A failed idempotent request is retried automatically, redirects are followed and cookies are
stored. The async client may negotiate HTTP/2.

**With veredium.** `disableAutomaticRetries`, `disableRedirectHandling`, `disableCookieManagement`. Reactive:
`HttpVersionPolicy.FORCE_HTTP_1`.

### JDK `java.net.http`

**Default (JDK 21+).**
- HTTP/2 by default, so every plaintext request offers an `h2c` upgrade.
- Redirects are not followed.
- The default proxy selector honours the `http.proxyHost` family of system properties.
- An idempotent GET whose pooled connection the peer had closed is re-sent on a fresh one. Only a JVM-wide
  property changes that.

**With veredium.** `HTTP_1_1`, `Redirect.NEVER` (explicit), `NO_PROXY` (blocking). The re-send stays: it has no
per-client switch.

### Jetty

**Default (12.1.12).** Redirects are followed and cookies are stored.

**With veredium.** `isFollowRedirects = false`; an empty cookie store.

### Reactor Netty

**Default (1.3.7).** A request whose pooled connection was closed while it was being sent is retried once. There
are no redirects and no cookie store.

**With veredium.** `disableRetry(true)`.

## 7. Request headers

**Expected.** Every request carries exactly one `Host` (the upstream from the triplet, which the sidecar routes on)
and exactly one `User-Agent` (`veredium/<version>`, or the host's `user-agent`).

### Apache HttpClient 5

**Default (httpclient5 5.6.4, httpcore5 5.4.3).** An explicit `Host` is sent as given. Without one, Apache sends `Apache-HttpClient/<version>`
as `User-Agent`.

**With veredium.** The module's headers replace the engine's.

### JDK `java.net.http`

**Default (JDK 21+).** `Host` is a **restricted** header:
- Blocking: Spring's request drops it silently, and the JDK sends the URI's authority instead.
- Reactive: the JDK refuses it.
- Behind the sidecar, either way the call would be routed by `localhost:<port>`.
- The only switch is the JVM-wide `-Djdk.httpclient.allowRestrictedHeaders=host`. The JDK and Spring read it
  **once**, when their HTTP classes initialise.

**With veredium.** The factory refuses to start where the JDK would send the wrong host, and names the flag:
- Blocking: compared per backend, against the base URL's authority.
- Reactive: always, unless the flag is set; the check asks the JDK itself.

The module never sets the flag: it would change every `java.net.http` client of the process.
`JdkEngineRestrictedHostHeaderTest` runs in a JVM of its own.

### Jetty

**Default (12.1.12).** Jetty sets its own `User-Agent`, and Spring **adds** the client's header next to it instead
of replacing it, so a peer sees two.

**With veredium.** Jetty's own `User-Agent` is removed (`userAgentField = null`).

### Reactor Netty

**Default (1.3.7).** An explicit `Host` is sent as given; the default `User-Agent` is Reactor Netty's.

**With veredium.** The module's headers replace the engine's.

Pinned by "should deserialise a GET response and send the module's headers once each" and the Host cases of both
engine tests.

## 8. Compression

**Expected.** The client offers gzip and decodes it, so the sidecar may compress. The buffer limit counts the
**decoded** bytes, on every engine.

### Apache HttpClient 5

**Default (httpclient5 5.6.4, httpcore5 5.4.3).**
- Blocking: compression is on (`Accept-Encoding: gzip, x-gzip, deflate`), and bodies are decoded transparently.
- Reactive: asks for gzip and decodes it.

**With veredium.** Left on.

### JDK `java.net.http`

**Default (JDK 21+).** The JDK client neither asks for nor decodes compressed bodies.
- Blocking: Spring's `JdkClientHttpRequestFactory` does both by default (`Accept-Encoding: gzip, deflate`,
  decoded in the adapter).
- Reactive: Spring's `JdkClientHttpConnector` does neither.

**With veredium.** Blocking: Spring's default stands. Reactive: the engine runs uncompressed, and its startup line
lists gzip under "does NOT apply".

### Jetty

**Default (12.1.12).** Asks for gzip and decodes it.

**With veredium.** Left on.

### Reactor Netty

**Default (1.3.7).** Off.

**With veredium.** `compress(true)`.

Pinned on the reactive side by "should ask for gzip and decode a gzip answer where the startup line claims it".

## 9. Abandoning an exchange

**Expected.** When the TimeLimiter expires, the caller is interrupted, or a future or subscription is cancelled,
the exchange underneath ends **at once**. The worker stops, the bulkhead permit comes back and the connection is
released. This applies while waiting for the response head and while the body is still arriving. A failure the
engine reports after that is the expected end of an aborted exchange, not an error.

### Apache HttpClient 5

**Default (httpclient5 5.6.4, httpcore5 5.4.3).**
- Blocking: on the decorator's virtual-thread workers, a blocking socket read reacts to an interrupt by closing the
  socket. The read fails at once with `SocketException: Closed by interrupt`, and Apache discards the connection.
  A pool lease and a connect are interruptible too.
- Reactive: Spring's `HttpComponentsClientHttpConnector` **drops the exchange's `Future`**. A subscription
  cancelled while waiting for the response therefore leaves the exchange running until the response or the
  response timeout. A body cancelled before its end (a codec refusal, a TimeLimiter abort) releases its
  connection without help.

**With veredium.**
- Blocking: nothing to add.
- Reactive: `AbortableAsyncClient` records each exchange's `Future` and cancels it when the subscription is
  cancelled while waiting for the response. `AbortableExchange` handles an abort that arrives before the
  `Future`; `AbortableExchangeTest` pins both orders. The body phase needs nothing: the dripping-body cases of
  `WebClientEngineIntegrationTest` stay green without a body decorator, which is why there is none.

### JDK `java.net.http`

**Default (JDK 21+).**
- Blocking: waiting for the head, Spring cancels the JDK's response future on an interrupt, which aborts the
  exchange. Reading the body, the JDK's stream closes itself and fails the read.
- Reactive: cancelling the subscription cancels the client's future, which completes it at once. Whether the
  exchange's own failure follows **after** the cancellation depends on timing: sometimes it does, sometimes not.
- Reactive, once the head arrived: cancelling the **body** subscription only unregisters the subscriber
  (`HttpBodySubscriberWrapper.onCancel`); the client reads the rest of the body on, for as long as the peer sends
  it. The JDK client has no public way to abort an exchange whose head arrived.

**With veredium.** Nothing to add on the blocking side. Reactive: see "Errors after a cancellation" below. A body
cancelled after the head (a codec refusal, a TimeLimiter abort) keeps its connection until the peer stops: the
startup line says so; there is no fix short of a connector of the module's own.

### Jetty

**Default (12.1.12).**
- Blocking: Spring's request only **stops waiting**. The Jetty request stays in flight and holds its connection
  until the response arrives or its total timeout expires.
- Reactive: the adapter aborts the request on cancel (`abortOnCancel`) and reports the failure afterwards - while
  waiting for the head. Cancelling the **body** only stops its delivery: Jetty goes on receiving the rest on the
  connection, for as long as the peer sends it.

**With veredium.** Blocking: the request factory captures the Jetty request where Spring creates it
(`HttpClient.newRequest(URI)`, on the calling thread). It aborts the request when the exchange fails, when a body
read fails, and when the response is closed early. Reactive: a body cancelled before its end aborts the native
request (`AbortJettyOnBodyCancel`). Pinned per engine by the reactive "should refuse a dripping body past the
codec limit at once, dropping the connection"; before the fix the Jetty connection stayed open.

### Reactor Netty

**Default (1.3.7).**
- Blocking, waiting for the head: Reactor's `block()` cancels the subscription on an interrupt, which closes the
  connection.
- Blocking, reading the body: Spring's `SubscriberInputStream` parks in a loop that **ignores the interrupt**.
  The worker would spin until the next chunk, the end of the body or the response timeout, and keep its permit
  meanwhile.
- Reactive: a cancelled subscription closes the connection.

**With veredium.** Blocking: each body read runs on a short-lived virtual thread while the worker waits for it
interruptibly. On an interrupt the worker closes Spring's stream, which cancels the inbound, and fails the read
with an `InterruptedIOException`. The wait for the head is handed off the same way; on an interrupt the worker
interrupts that thread, whose `block()` cancels the subscription as before (see §12 for why).

### Errors after a cancellation (reactive, every engine)

**Default (Reactor).** An error that an adapter reports after the subscription was cancelled reaches Reactor's
`onErrorDropped` hook. Reactor logs it at ERROR as if it had been lost.

**With veredium.** Only errors dropped after **this** exchange was cancelled are logged at DEBUG ("ended after
cancellation"). Any other dropped error goes on to the caller's own hook or to Reactor's global handling.

Pinned by the "Abandoned exchanges" cases of `RestClientEngineIntegrationTest`, per engine. They release the head
wait, the body read, the interrupted caller and the cancelled future, and check that the permit and the
connection come back. The reactive abort case of `WebClientEngineIntegrationTest` covers the other stack.

## 10. A body not read to its end

**Expected.** A response that is closed before its body was read to the end gives its connection up at once.
The usual case is a body refused at the buffer limit. Reading the rest of a body nobody wants holds the worker,
its permit and its connection for as long as the peer takes. Past the TimeLimiter budget, the client-side
refusal, which the breaker ignores, would turn into a `TimeoutException`, which it records.

### Apache HttpClient 5

**Default (httpclient5 5.6.4, httpcore5 5.4.3).** Blocking: Spring's `HttpComponentsClientHttpResponse.close()` consumes the entity, and
Apache's content streams (`ContentLengthInputStream`, `ChunkedInputStream`) **read the rest** of the message in
their `close`, to keep the connection. The source says so: "this assumes that closing the stream will consume the
remainder" (`ResponseEntityProxy`).

**With veredium.**
- Blocking: the request factory captures the Apache request. A response closed before its end cancels the request
  first (`HttpUriRequestBase.cancel()` → `InternalExecRuntime.cancel()`), which discards the connection.
- Reactive: a cancelled body `Flux` cancels the exchange's `Future` (§9).

### JDK `java.net.http`

**Default (JDK 21+).** Blocking: Spring's `JdkClientHttpResponse.close()` **drains** the stream
(`StreamUtils.drain`), up to the read deadline.

**With veredium.** Blocking: a response closed before its end closes the JDK's body stream first, which cancels
the exchange and drops its connection.

### Jetty

**Default (12.1.12).** Blocking: the Jetty request stays in flight, as in §9.

**With veredium.** Blocking: the request is aborted.

### Reactor Netty

**Default (1.3.7).** Blocking: closing Spring's response **drains** the rest of the body.

**With veredium.** Blocking: a body not read to its end is closed before the response, which cancels the inbound.

The four blocking engines share one decorator (`AbortingRequest` / `AbortingResponse` in `EngineTransport.kt`), or
the Reactor Netty hand-off stream. Pinned per engine by "should refuse a dripping body past the buffer limit at
once, without reading the rest". The backend sends the head and 4 KiB of a declared 1 MiB, then one byte every
200 ms. Before the fix, Apache took 30 s and ended at the TimeLimiter, and the JDK client took the full read
deadline.

## 11. Resources and meters

**Expected.** The engine's pool, threads and meters belong to the module's factory and to nothing else. They are
not shared with the host's other clients, not registered in a JVM-global place, and gone after shutdown.

### Apache HttpClient 5

**Default (httpclient5 5.6.4, httpcore5 5.4.3).** Pool meters come from the `ConnPoolMeters` binder (httpclient5-observation). It reads the
pool off a client **builder** and hands back nothing to deregister with.

**With veredium.** The binder runs once per pool, over a throwaway builder, in the host's registry
(`restclient.pool.*`). What it added is read off the registry and removed on shutdown.

### JDK `java.net.http`

**Default (JDK 21+).** Each client runs on an executor of its own, a cached platform-thread pool. Reactive bodies
arrive on the client's own worker threads (`HttpClient-<n>-Worker-<m>`) or on `ForkJoinPool.commonPool`, where
`sendAsync` hands completions - which of the two varies from call to call. There are no pool statistics.

**With veredium.** Blocking: a virtual-thread executor owned by the transport, which also serves as Spring's
body writer. There are no pool meters, and the startup line says so.

### Jetty

**Default (12.1.12).** Each client starts its own `QueuedThreadPool`, whose threads are **non-daemon** and can keep
a JVM alive. There is no Micrometer binder.

**With veredium.** One client per engine, started with the factory and stopped with it. If a construction fails
part-way, the engines already created are stopped before the failure is rethrown. Reactive I/O threads are named
`veredium-webclient-jetty-*`.

### Reactor Netty

**Default (1.3.7).** Without resources of its own, a client runs on the JVM-wide `HttpResources`. Those are shared
with every other Reactor Netty client of the host, and Boot's `ReactorResourceFactory` manages them for the host's
own clients. Even with own loops (`runOn`), the default resolver still comes from `HttpResources`. With the default
registrar, `.metrics(true)` registers in Micrometer's **global** registry.

**With veredium.**
- Both stacks: own `LoopResources`, own `ConnectionProvider`, and an explicit `DefaultAddressResolverGroup`.
- Blocking: a registrar that puts the pool gauges in the **host's** registry and removes them on disposal.

Pinned by "should register Reactor Netty's pool gauges in the host registry and keep off its global resources" (a
before/after comparison of the JVM-wide state) and "should take the engine's pool gauges out of the host registry
on shutdown".

## 12. Shutdown

**Expected.**
- On context shutdown, every engine first lets its in-flight exchanges finish, up to **one shared deadline**: the
  longest response timeout + connect + pending-acquire + 1 s.
- Then it closes. An engine that did not drain in time is reported in one WARN line and closed anyway, never as an
  exception out of the destroy callback.
- An engine whose release fails is reported and does not keep the others open.
- An interrupted shutdown thread stops waiting at once and keeps its flag.
- A client requested after shutdown is refused.

### Apache HttpClient 5

**Default (httpclient5 5.6.4, httpcore5 5.4.3).**
- Blocking: `close` discards leased connections on the spot. There is no "dispose when drained".
- Reactive: the graceful reactor shutdown lets an exchange already on a connection finish, but opens **no new
  connection**: a call still waiting for one (max-connections reached, a connect not yet made) fails at once with
  an `IOReactorShutdownException`. A hard close ends the exchanges on their connections **without completing
  their callbacks**, so a caller's `Mono` waits for its TimeLimiter.
- Reactive: closing a reactor whose workers are still shutting down closes their selector under them. Apache logs
  that at ERROR as a `ClosedSelectorException`.

**With veredium.**
- Blocking: the module waits for the leased count to reach zero, polling every 50 ms up to the deadline, then
  closes the pool.
- Reactive: the module waits for its exchanges in flight (those waiting for a connection included) to reach zero,
  polling every 50 ms up to the deadline, and only then shuts the reactor down. What is still in flight after the
  reactor shutdown is cancelled (one WARN with their number). The reactor is closed hard
  only if it does not end by itself. The multi-core reactor reports `SHUT_DOWN` only once it is closed, so the
  module cannot tell whether every worker already ended. A worker that has not finds its selector closed and
  reports a `ClosedSelectorException`; the module's own reactor exception callback logs that one at DEBUG during
  the release, and every other reactor exception at ERROR, as Apache's default callback would.

### JDK `java.net.http`

**Default (JDK 21+).** `shutdown()` lets in-flight exchanges finish, and `awaitTermination` waits for them.
`shutdownNow()` returns **before** the client has terminated; its threads end asynchronously.

**With veredium.** `shutdown` + `awaitTermination` up to the deadline, then `shutdownNow` with a WARN and a bounded
wait for termination.

### Jetty

**Default (12.1.12).** `stop()` aborts the exchanges still running and waits for Jetty's own threads, **regardless
of an interrupt**.

**With veredium.**
- Both stacks: the module waits for active and queued requests to reach zero, up to the deadline, then stops; what
  is still running then is aborted, with one WARN. From an interrupted thread, the wait ends at once and the stop
  runs on a thread of its own and is left to complete.

### Reactor Netty

**Default (1.3.7).**
- `disposeLater()` on a pool built without `disposeTimeout` is not a graceful shutdown: it closes the idle
  connections, leaves in-flight calls to finish on theirs, and fails every call still waiting for a connection at
  once (`PoolShutdownException`).
- With `disposeTimeout` it is: the pool refuses new acquires, serves the waiting ones, and completes once every
  connection came back, or fails at the timeout, which Reactor Netty logs at ERROR.
- Disposing the event loops closes every channel on them.
- Spring's wait for the head is a `block()` without a time bound, and the response timeout is a timer on the
  event loops. Once the loops are disposed, an exchange whose end they did not signal any more holds its worker
  until the TimeLimiter budget. Seen once on CI, after a shutdown past its deadline; not reproduced locally.
- `block(Duration)` reports an expiry as an `IllegalStateException`.

**With veredium.**
- Blocking: waits for the acquired count to reach zero, then disposes pool and loops. An expiry is a WARN line,
  not an exception; the hand-off executor is shut down whatever happens. Its `shutdownNow` interrupts every
  head wait and body read still running on it, so a call the loops no longer end fails at once with an
  `IOException` instead of waiting for its budget (pinned by `ReactorNettyHandOffTest`).
- Reactive: the pool is built with `disposeTimeout` = the drain bound, so its disposal waits for the calls in
  flight, those waiting for a connection included; the factory waits for it up to the shared deadline, with a WARN
  past it. The loops go after the pool, once no call can still need them.

Pinned per engine by "should let an in-flight exchange finish before closing the engine", "should let a call still
waiting for a connection finish before closing the engine", "should report an engine that does not drain by the
deadline and close it anyway", "should stop waiting when the shutdown thread is interrupted and leave the flag set"
and "should refuse a client requested after shutdown" (blocking), and by the reactive "Draining shutdown" (the same
two cases on every engine), "Release against the shutdown deadline" and "Interrupted shutdown" cases. `EngineTransportLifecycleTest`
and `EngineTransportsTest` pin the isolation of a failing release.

## Test infrastructure

The engines show their behaviour only against a backend that misbehaves in a controlled way. Three test tools
behave differently from what one would expect:

- **WireMock's `withChunkedDribbleDelay` does not drip.** The body goes into the server's output buffer, and the
  head reaches the wire only once that buffer is full, so a "dripping" body is often just a delayed one. The tests
  that need a body still arriving use a raw-socket `DrippingBackend` instead.
- **WireMock's fixed delays occupy a container thread** even after the client went away. Abandoned exchanges with
  long delays pile up across a test class, which is why the engine tests run WireMock with asynchronous
  responses.
- **A connect timeout needs a listening socket that does not answer.** A `ServerSocket` with a backlog of 1 whose
  accept queue has been filled makes Linux drop further SYNs, so the connect hangs until the client gives up.
  Blocking: `BlackholeBackend`. Reactive: `UnansweredConnectBackend`, which skips the case where the platform does
  not drop the SYNs.
