# veredium-webclient guide

> The long-form guide of the module: architecture, integration, configuration. It is
> rendered on the documentation site by the Docs workflow
> (`.github/scripts/generate-guide-pages.py` copies it under `docs/guides/` with
> rewritten links), so relative links from here must point at files that exist.

## Scope

`veredium-webclient` is the reactive client of the Veredium project: Spring's `WebClient` over Reactor Netty: one resilience-wrapped client per mesh endpoint, the timeout ladder derived from one measured anchor, the three Resilience4j primitives installed through the library's own customizers (shared with the blocking client in `veredium-core`), the codec buffer limit and the exchange event of `legatium-webclient-logging`.

The module ships the client INFRASTRUCTURE only. A host declares its own adapters on top: one
`@Component` per backend that asks the factory for the client of its mesh endpoint alias and
describes the request; the mesh endpoints, the Resilience4j instances and the exchange-logging
surface are configured in the host's `application.yml`. `src/test/resources/application.yml`
of this module is the reference configuration the contract tests pin - a library ships no
`application.yml` of its own.

**The HTTP engine.** Reactor Netty by default; `app.webclient.engine` moves all backends or single ones to the
JDK's `java.net.http.HttpClient`, Jetty or Apache HttpClient 5 - scoped to this module, so the host's other
WebClients keep the connector Boot gave them. What each engine applies of the mesh transport and pool
settings, which meters it has and what it needs: [configuration guide](webclient-config-guide.md) §1.6.

**The response contract.** A client's `Mono<T>` emits exactly one value or an error: a 2xx answer without a
body (204, empty 200) fails with `EmptyResponseBodyException` rather than completing the `Mono` empty; see the
[configuration guide](webclient-config-guide.md) §3.4. Only a 2xx is a body: a 1xx or 3xx fails as a
`WebClientResponseException` with its status. What every status becomes on both stacks, and how the breaker counts
it: [Status contract](../../docs/status-contract.md).

## Prerequisites

- Java 21 or newer at runtime (the build itself needs JDK 24+, see the
  [README](../../README.md)).
- Spring Boot 4.1, Resilience4j 2.4 (`resilience4j-spring-boot4`), legatium 1.3.

## Dependency

```xml
<dependency>
    <groupId>eu.inqudium</groupId>
    <artifactId>veredium-webclient</artifactId>
    <version>...</version>
</dependency>
```

The dependency is all the wiring there is: the module registers a Spring Boot auto-configuration,
`VerediumWebClientAutoConfiguration`, which brings the client factory, the mesh wiring of `veredium-core` and the startup
lines. Its beans back off for a host's own bean of the same type.

**Both client modules in one host.** A host that depends on `veredium-restclient` as well runs both stacks in one context. Each
mesh endpoint then names the stack that serves it, `app.mesh.endpoints[].stack: webclient` for this module; an
endpoint without the key fails the start with a message that names it. Each factory serves only its own stack's
endpoints and refuses the other's, naming the owner. Each stack holds its own pools, so
`app.mesh.defaults.transport.max-connections` applies per stack and the sidecar can see twice as many connections. A
host with one client module never sets `stack`, and neither does one that excludes the other auto-configuration
(`@SpringBootApplication(exclude = [VerediumRestClientAutoConfiguration::class])`)
([ADR-0003](../../docs/adr/ADR-0003-each-mesh-endpoint-is-owned-by-one-client-stack.md)).

The non-default engines need their library on the host's classpath - the module declares them `optional`,
versions from the Spring Boot BOM. Only the one selected is required:

```xml
<!-- app.webclient.engine: jetty -->
<dependency>
    <groupId>org.eclipse.jetty</groupId>
    <artifactId>jetty-client</artifactId>
</dependency>
<dependency>
    <groupId>org.eclipse.jetty</groupId>
    <artifactId>jetty-reactive-httpclient</artifactId>
</dependency>

<!-- app.webclient.engine: apache -->
<dependency>
    <groupId>org.apache.httpcomponents.client5</groupId>
    <artifactId>httpclient5</artifactId>
</dependency>
<dependency>
    <groupId>org.apache.httpcomponents.core5</groupId>
    <artifactId>httpcore5-reactive</artifactId>
</dependency>
```

`jdk` needs no dependency, but the JVM flag `-Djdk.httpclient.allowRestrictedHeaders=host` (the clients send
the mesh Host header; the configuration guide §1.6 explains why the module does not set it).

## Engines

The HTTP engine underneath the clients is selectable, for all backends and per backend:

```yaml
app:
  webclient:
    engine:
      default: reactor-netty                            # reactor-netty | jdk | jetty | apache
      endpoints:
        apis-example-com-GET-v6-involved-parties: jetty   # this backend only; the key is its mesh alias
```

| Value | Spring connector | Dependency the host adds |
|---|---|---|
| `reactor-netty` (default) | `ReactorClientHttpConnector` | none |
| `jdk` | `JdkClientHttpConnector` | none, but the JVM flag above |
| `jetty` | `JettyClientHttpConnector` | `jetty-client`, `jetty-reactive-httpclient` |
| `apache` | `HttpComponentsClientHttpConnector` | `httpclient5`, `httpcore5-reactive` |

- **Scope.** The setting reaches only the clients of this module. Boot's global `spring.http.clients.*`
  switches are neither read nor written, the host's own `WebClient.Builder` bean keeps the connector Boot gave
  it, and no process-global resource is used. Even the default engine runs on event loops and a resolver of
  its own, not on Reactor Netty's global `HttpResources`.
- **What applies.** The mesh transport settings (connect, response timeout, max connections, pending-acquire)
  and `app.webclient.pool.*` are applied as far as each engine supports them. One startup line per engine in use
  names what does not apply. The per-request and pool meters exist on `reactor-netty` only.
- **Fail fast.** The start fails, naming the key, for an override key that is no mesh alias, for a missing engine
  library (with its coordinates), and for `jdk` without the Host-header flag.
- **Cancellation.** A TimeLimiter expiry or a cancelled subscription aborts the exchange on every engine while
  it waits for the response head. A body cancelled after the head arrived drops the connection on every engine
  but `jdk`, whose client reads such a body to its end (its startup line says so).

Every detail, with the mapping table per engine: [configuration guide](webclient-config-guide.md) §1.6.

## Guides

- [The mesh timeout ladder](../../veredium-core/docs/mesh-timeout.md) (veredium-core)
- [Configuration guide](webclient-config-guide.md)
- [Metrics](metrics.md)
- [Logging guide](logging-guide.md)
- [HTTP engine behaviour - what each engine does on its own](../../docs/engine-behaviour.md) (both clients)

## Provenance

The code was ported on 2026-10-19 from the `web-client` module of
[dhaase/tool-box](https://github.com/dhaase/tool-box), where it fronted four concrete backends
behind a service-mesh sidecar. Organisation-specific names were replaced (`example.com` hosts,
`X-Access-Token`), the concrete adapters and DTOs stayed behind. The guides below still describe
that origin where they explain a number; the code wins where the two disagree.
