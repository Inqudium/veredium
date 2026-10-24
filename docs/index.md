![Veredium banner](logo/veredium-banner.svg)

# Veredium

Veredium is a **web client implementation** — named after the Roman *veredus*,
the post-horse of the *cursus publicus*: the mount that carries the envoy to
the foreign party and back. Its sibling [Legatium](https://github.com/Inqudium/legatium)
records what the envoy's call came to; Veredium is the ride itself.

| Module | Role | Root package |
|---|---|---|
| `veredium-core` | the stack-neutral core both clients share: mesh timeout ladder, breaker verdicts, Resilience4j wiring | `eu.inqudium.veredium.core` |
| `veredium-restclient` | the blocking client: Spring `RestClient` over a selectable HTTP engine (default Apache HttpClient 5; [ADR-0004](https://github.com/Inqudium/veredium/blob/main/docs/adr/ADR-0004-the-http-engine-is-the-hosts-choice.md)) | `eu.inqudium.veredium.restclient` |
| `veredium-webclient` | the reactive client: Spring `WebClient` over a selectable HTTP engine (default Reactor Netty; [ADR-0004](https://github.com/Inqudium/veredium/blob/main/docs/adr/ADR-0004-the-http-engine-is-the-hosts-choice.md)) | `eu.inqudium.veredium.webclient` |

## Quick start

[![veredium-core on Maven Central](https://img.shields.io/maven-central/v/eu.inqudium/veredium-core.svg?label=veredium-core)](https://central.sonatype.com/artifact/eu.inqudium/veredium-core)

A host declares the client module of the stack it calls with - `veredium-restclient`
for the blocking client, `veredium-webclient` for the reactive one, both for a host that
runs the two side by side ([ADR-0003](https://github.com/Inqudium/veredium/blob/main/docs/adr/ADR-0003-each-mesh-endpoint-is-owned-by-one-client-stack.md)). Each brings
`veredium-core` along. The badge shows the current release on Maven Central — use that
version where the snippet says `...`:

```xml
<dependency>
    <groupId>eu.inqudium</groupId>
    <artifactId>veredium-restclient</artifactId>
    <version>...</version>
</dependency>
```

## Documentation

- **[Core guide](https://github.com/Inqudium/veredium/blob/main/veredium-core/docs/GUIDE.md)** —
  the long-form guide of the module: architecture, integration, configuration.
- **[Status contract](status-contract.md)** —
  what each HTTP status becomes on both client stacks: a value or an error,
  and how the circuit breaker counts it.
- **[Test evidence](https://inqudium.github.io/veredium/tests/test-evidence/)** —
  the generated inventory of the test suite: every test sentence plus its
  rationale, grouped by module and component.
- **[Coverage report](https://inqudium.github.io/veredium/coverage/)** —
  the cross-module JaCoCo report of the run that built this site.
- **[API reference](https://inqudium.github.io/veredium/api/veredium-core/)** —
  generated with Dokka.

## Project

- [README](https://github.com/Inqudium/veredium#readme) — the full project
  story and the naming.
- [Contributing](https://github.com/Inqudium/veredium/blob/main/CONTRIBUTING.md)
- [Changelog](https://github.com/Inqudium/veredium/blob/main/CHANGELOG.md)
- [License (Apache 2.0)](https://github.com/Inqudium/veredium/blob/main/LICENSE)
