# veredium-restclient guide

> The long-form guide of the module: architecture, integration, configuration. It is
> rendered on the documentation site by the Docs workflow
> (`.github/scripts/generate-guide-pages.py` copies it under `docs/guides/` with
> rewritten links), so relative links from here must point at files that exist.

## Scope

`veredium-restclient` is the blocking client of the Veredium project: Spring's `RestClient` over Apache HttpClient 5: one resilience-wrapped client per mesh endpoint, the timeout ladder derived from one measured anchor, the three Resilience4j primitives installed through the library's own customizers, a bounded body read and the exchange event of `legatium-restclient-logging`.

The module ships the client INFRASTRUCTURE only. A host declares its own adapters on top: one
`@Component` per backend that asks the factory for the client of its mesh endpoint alias and
describes the request; the mesh endpoints, the Resilience4j instances and the exchange-logging
surface are configured in the host's `application.yml`. `src/test/resources/application.yml`
of this module is the reference configuration the contract tests pin - a library ships no
`application.yml` of its own.

## Prerequisites

- Java 21 or newer at runtime (the build itself needs JDK 24+, see the
  [README](../../README.md)).
- Spring Boot 4.1, Resilience4j 2.4 (`resilience4j-spring-boot4`), legatium 1.2.

## Dependency

```xml
<dependency>
    <groupId>eu.inqudium</groupId>
    <artifactId>veredium-restclient</artifactId>
    <version>...</version>
</dependency>
```

## Guides

- [Timeouts, bulkheads and threads - how a blocking call is bounded](timeouts-bulkheads-threads.md)
- [Metrics](metrics.md)
- [What maps to the reactive sibling and what differs](restclient-vs-webclient.md)
- [Decisions this module relies on](decisions.md)
- [The mesh timeout ladder](../../veredium-core/docs/mesh-timeout.md) (veredium-core)

## Provenance

The code was ported on 2026-10-19 from the `rest-client` module of
[dhaase/tool-box](https://github.com/dhaase/tool-box), where it fronted four concrete backends
behind a service-mesh sidecar. Organisation-specific names were replaced (`example.com` hosts,
`X-Access-Token`), the concrete adapters and DTOs stayed behind. The guides below still describe
that origin where they explain a number; the code wins where the two disagree.
