# veredium-restclient guide

> The long-form guide of the module: architecture, integration, configuration. It is
> rendered on the documentation site by the Docs workflow
> (`.github/scripts/generate-guide-pages.py` copies it under `docs/guides/` with
> rewritten links), so relative links from here must point at files that exist.

## Scope

`veredium-restclient` is the blocking client of the Veredium project: Spring's `RestClient` over Apache HttpClient 5 (or, selectable per backend, the JDK client, Jetty or Reactor Netty - see [Engines](#engines)): one resilience-wrapped client per mesh endpoint, the timeout ladder derived from one measured anchor, the three Resilience4j primitives installed through the library's own customizers, a bounded body read and the exchange event of `legatium-restclient-logging`.

The module ships the client INFRASTRUCTURE only. A host declares its own adapters on top: one
`@Component` per backend that asks the factory for the client of its mesh endpoint alias and
describes the request; the mesh endpoints, the Resilience4j instances and the exchange-logging
surface are configured in the host's `application.yml`. `src/test/resources/application.yml`
of this module is the reference configuration the contract tests pin - a library ships no
`application.yml` of its own.

## Prerequisites

- Java 21 or newer at runtime (the build itself needs JDK 24+, see the
  [README](../../README.md)).
- Spring Boot 4.1, Resilience4j 2.4 (`resilience4j-spring-boot4`), legatium 1.3.

## Dependency

```xml
<dependency>
    <groupId>eu.inqudium</groupId>
    <artifactId>veredium-restclient</artifactId>
    <version>...</version>
</dependency>
```

The dependency is all the wiring there is: the module registers a Spring Boot auto-configuration,
`VerediumRestClientAutoConfiguration`, which brings the client factory, the mesh wiring of `veredium-core` and the startup
lines. Its beans back off for a host's own bean of the same type.

**Both client modules in one host.** A host that depends on `veredium-webclient` as well runs both stacks in one context. Each
mesh endpoint then names the stack that serves it, `app.mesh.endpoints[].stack: restclient` for this module; an
endpoint without the key fails the start with a message that names it. Each factory serves only its own stack's
endpoints and refuses the other's, naming the owner. Each stack holds its own pools, so
`app.mesh.defaults.transport.max-connections` applies per stack and the sidecar can see twice as many connections. A
host with one client module never sets `stack`, and neither does one that excludes the other auto-configuration
(`@SpringBootApplication(exclude = [VerediumWebClientAutoConfiguration::class])`)
([ADR-0003](../../docs/adr/ADR-0003-each-mesh-endpoint-is-owned-by-one-client-stack.md)).

## Engines

The HTTP engine underneath the clients is selectable - for all backends, and per backend. What each engine does on
its own, and what the module does about it, is collected in
[HTTP engine behaviour](../../docs/engine-behaviour.md):

```yaml
app:
  restclient:
    engine:
      default: apache                                   # apache | jdk | jetty | reactor-netty
      endpoints:
        apis-example-com-GET-v6-involved-parties: jetty   # this backend only; the key is its mesh alias
```

| Value | Spring request factory | Engine | Dependency the host adds |
|---|---|---|---|
| `apache` (default) | `HttpComponentsClientHttpRequestFactory` | Apache HttpClient 5 | none - a regular dependency of this module |
| `jdk` | `JdkClientHttpRequestFactory` | `java.net.http.HttpClient` | none - part of the JDK (`java.net.http`) |
| `jetty` | `JettyClientHttpRequestFactory` | Jetty `HttpClient` | `org.eclipse.jetty:jetty-client` |
| `reactor-netty` | `ReactorClientHttpRequestFactory` | Reactor Netty `HttpClient` | `io.projectreactor.netty:reactor-netty-http` |

`jetty-client` and `reactor-netty-http` are *optional* dependencies of this module: a consumer gets neither
transitively - no Jetty and no reactive artifact on its classpath - until it selects the engine and adds the
dependency (version from the Spring Boot BOM). `apache` is the default: a host that does not set the property runs
every backend on Apache HttpClient 5.

### Isolation: this module's clients, and no other client of the host

The property changes the engine of the clients `ResilientRestClientFactory` builds, and of nothing else:

- the factory never reads or writes Boot's own switch, `spring.http.clients.imperative.factory`, nor any other
  `spring.http.clients.*` setting;
- it registers no bean a host's wiring could pick up - no `ClientHttpRequestFactory`, no engine client or pool, no
  `ClientHttpRequestFactoryBuilder`, no `HttpClientSettings`;
- it keeps cloning Boot's `RestClient.Builder` bean and sets the request factory on the clone only. A `RestClient`
  the host builds from the same bean keeps the engine Boot gave it;
- every engine's resources - pool, client, threads - are created and closed by the factory. The Reactor Netty
  engine runs on its own `ConnectionProvider` and event loops, never on Reactor Netty's global `HttpResources`, and
  its pool gauges go into the host's `MeterRegistry`, never into Micrometer's global one. No JVM-wide system
  property is set (the JDK client's pool and Host-header knobs exist only as such properties - see below).

*Why not Boot's switch:* `spring.http.clients.imperative.factory` selects the engine of every `RestClient` and
`RestTemplate` Boot builds in the host - the host's own clients included, and with them their timeouts, their pools
and their behaviour under failure. A library that set it would change clients it does not own. The reverse holds
too: a host that sets Boot's switch for its own clients leaves this module's clients on `app.restclient.engine`.
`RestClientEngineIsolationTest` pins both directions in a real context, reading the host client's engine off the
wire (its default `User-Agent`).

### Fail fast

The start fails with a message naming the key when

- an `app.restclient.engine.endpoints` key is not the alias of an `app.mesh.endpoints[]` entry (a typo would
  otherwise leave the intended backend on the default engine without a word);
- a selected engine's library is not on the classpath - the message names the Maven coordinates above;
- `jdk` is selected for a backend whose Host header the JDK client would replace (next section).

### Host header

Every engine sends the explicit `Host` header the factory derives from the triplet - except the JDK client, for
which `Host` is a *restricted* header: Spring's request drops it and the JDK sends the authority of the request URI
instead (`localhost:<port>` behind the sidecar), unless the JVM runs with
`-Djdk.httpclient.allowRestrictedHeaders=host`. Behind a sidecar that routes on Host, every call would go to the
wrong upstream. The factory therefore refuses `jdk` for a backend whose Host differs from the authority of its base
URL. The ways out: another engine for that backend; a `base-url` whose authority is the Host (a direct call without
a sidecar); or the system property - a JVM-wide setting that changes every `java.net.http` client of the process,
which is why veredium never sets it and leaves the decision to the host. (Jetty *adds* a header Spring sets rather
than replacing its own, so the Jetty engine removes Jetty's default `User-Agent`; a peer sees exactly one.)

### What each engine applies

The mesh ladder and `app.restclient.pool` were written for Apache's pool. Every engine applies them as far as it
has the knob; where it has none, the setting does not apply - stated once per engine in use at startup, never
ignored silently:

| Setting | `apache` | `jdk` | `jetty` | `reactor-netty` |
|---|---|---|---|---|
| `transport.connect-timeout-ms` | `connectTimeout` | `connectTimeout` | `connectTimeout` | `CONNECT_TIMEOUT_MILLIS` |
| the endpoint's response timeout (ladder rung) | socket timeout of every read | one deadline over head and body (send until the body is closed) | the request's total timeout (queueing, connect, head and body) and its idle timeout | the longest silence between reads |
| `transport.max-connections` | `maxConnTotal` and `maxConnPerRoute` | **does not apply** - the JDK pool is unbounded | `maxConnectionsPerDestination` | `maxConnections` per remote address |
| `transport.pending-acquire-timeout-ms` | `connectionRequestTimeout` | **does not apply** - nothing waits for a connection | **does not apply** - a queued request waits within its total timeout; the queue is bounded at 1024 per destination | `pendingAcquireTimeout`; the queue is bounded at 2 × `max-connections` |
| `pool.max-idle-time` | `closeIdle` on the eviction schedule | **does not apply** | `HttpClient.idleTimeout` | `maxIdleTime` |
| `pool.max-life-time` | `timeToLive` | **does not apply** | the pool's `maxDuration`, checked when a connection is leased | `maxLifeTime` |
| `pool.evict-in-background` | the factory's eviction schedule | **does not apply** | **does not apply** - Jetty closes an idle connection when its idle timeout passes | `evictInBackground` |
| pool meters | `restclient.pool.{leased,available,pending}` | none | none | `reactor.netty.connection.provider.*{name=restclient-reactor-netty}` |
| response timeout surfaces as (under `ResourceAccessException`) | `SocketTimeoutException` | `HttpTimeoutException` | `TimeoutException` | `ReadTimeoutException` |

`pool.max-idle-time` must be positive: a zero would mean "evict every idle connection at once" to Reactor Netty
(no keep-alive), "no idle timeout" to Jetty and "close every idle connection at each sweep" to Apache, so the binding
refuses it. Zero keeps its meaning for `pool.max-life-time` (unbounded) and `pool.evict-in-background` (off).

Every one of those four is classified as `timeout` by legatium's exchange event. The JDK's pool knobs exist only
as JVM-wide system properties (`jdk.httpclient.keepalive.timeout`, `jdk.httpclient.connectionPoolSize`), which
the module does not set. With `jdk`, size the bulkheads knowing that no pool bounds the connections behind them.

Retries, redirects and cookies are off on every engine as on Apache - with one exception the JDK client has no
per-client switch for: it retries a refused connect once (within the same connect timeout) and re-sends an
idempotent GET whose pooled connection the peer had closed (`jdk.httpclient.disableRetryConnect` and friends are
JVM-wide).

The startup lines, next to the Bean Validation summary:

```
HTTP engines: default=apache, per backend [a-stable=apache, b-trial=jdk]
HTTP engine 'apache' for a-stable: applies connect-timeout-ms=100 (ConnectionConfig.connectTimeout), ...; does not apply -; pool meters: ...; cancellation: ...
HTTP engine 'jdk' for b-trial: applies connect-timeout-ms=100 (HttpClient.connectTimeout), ...; does not apply max-connections=100 (the JDK pool is unbounded), ...
```

### Abandoned exchanges

The blocking decorator interrupts the worker when the TimeLimiter budget expires, when the caller is interrupted
and when an asynchronous future is cancelled ([timeouts, bulkheads and threads](timeouts-bulkheads-threads.md)
§3). On every engine the worker then ends at once, releasing its bulkhead permit, and the connection is released
with it - by four different routes:

| Engine | Waiting for the head | Reading the body |
|---|---|---|
| `apache` | the JDK closes the socket on interrupt; Apache discards the connection | the same |
| `jdk` | Spring cancels the JDK's exchange future, which aborts the exchange | the JDK's body stream closes itself and fails the read |
| `jetty` | Spring stops waiting; the module **aborts the Jetty request**, which Spring leaves in flight | the read fails on interrupt; the module aborts the request |
| `reactor-netty` | Reactor's `block()` cancels the subscription, which closes the connection | Spring's body stream ignores an interrupt, so the module **hands each read to a short-lived virtual thread** and waits for it interruptibly; on interrupt it closes the stream |

The two module-side measures close gaps of Spring's request factories (spring-web 7.0): without them an abandoned
Jetty exchange keeps its connection until its total timeout, and an abandoned Reactor Netty body read keeps its
worker spinning - and its permit - until the next chunk or the response timeout. A response closed before its body
was read to the end - a body refused at the buffer limit - is cancelled (Apache), has its body stream closed (JDK,
Reactor Netty) or is aborted (Jetty) instead of drained. Spring's Apache and JDK responses would otherwise read the
rest of the body on close, to keep the connection: for a large or slow body that holds the worker, its permit and
its connection, and past the TimeLimiter budget turns the client-side refusal into a timeout the breaker records.
`RestClientEngineIntegrationTest` pins all four cases per engine, and the refused body per engine with a body that
drips far longer than the call may take. The exchange event of an abandoned call is WARN `cancelled` on every
engine: legatium reads the worker's interrupt, and on Jetty the module restores the interrupt flag that Jetty's body
stream clears ([HTTP engine behaviour](../../docs/engine-behaviour.md), "Abandoning an exchange").

### Shutdown

`shutdown()` closes every engine in use, each draining its in-flight exchanges until one shared deadline (the
longest response timeout + connect + pending-acquire + 1 s): the Apache pool and the Reactor Netty pool wait for
their leased connections, the Jetty client for its active and queued requests, the JDK client through its own
`shutdown`/`awaitTermination`. An engine that does not drain in time is reported in one WARN line and closed
anyway; an engine whose shutdown fails is reported as well and does not keep the others open. The Apache pool's
gauges leave the host's registry with the pool. A client requested after `shutdown()` is refused with an
`IllegalStateException` instead of being built on a closed engine.

## Guides

- [Timeouts, bulkheads and threads - how a blocking call is bounded](timeouts-bulkheads-threads.md)
- [Metrics](metrics.md)
- [What maps to the reactive sibling and what differs](restclient-vs-webclient.md)
- [Decisions this module relies on](decisions.md)
- [The mesh timeout ladder](../../veredium-core/docs/mesh-timeout.md) (veredium-core)
- [HTTP engine behaviour - what each engine does on its own](../../docs/engine-behaviour.md) (both clients)

## Provenance

The code was ported on 2026-10-19 from the `rest-client` module of
[dhaase/tool-box](https://github.com/dhaase/tool-box), where it fronted four concrete backends
behind a service-mesh sidecar. Organisation-specific names were replaced (`example.com` hosts,
`X-Access-Token`), the concrete adapters and DTOs stayed behind. The guides below still describe
that origin where they explain a number; the code wins where the two disagree.
