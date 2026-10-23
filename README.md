<p align="center">
  <img src="docs/logo/veredium-banner.svg" alt="Veredium — the mount that carries the envoy" width="640">
</p>

[![Maven Central](https://img.shields.io/maven-central/v/eu.inqudium/veredium-core.svg?label=Maven%20Central)](https://central.sonatype.com/namespace/eu.inqudium)
[![CI](https://github.com/Inqudium/veredium/actions/workflows/ci.yml/badge.svg)](https://github.com/Inqudium/veredium/actions/workflows/ci.yml)
[![Coverage](https://inqudium.github.io/veredium/coverage/badge.svg)](https://inqudium.github.io/veredium/coverage/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-21-orange)](https://openjdk.org/projects/jdk/21/)
[![Kotlin](https://img.shields.io/badge/Kotlin-2.4.21-7F52FF?logo=kotlin&logoColor=white)](https://kotlinlang.org/)
[![Last commit](https://img.shields.io/github/last-commit/Inqudium/veredium)](https://github.com/Inqudium/veredium/commits/main)
[![Issues](https://img.shields.io/github/issues/Inqudium/veredium)](https://github.com/Inqudium/veredium/issues)
[![Docs](https://img.shields.io/badge/docs-inqudium.github.io-8E2C21)](https://inqudium.github.io/veredium/)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/Inqudium/veredium/badge)](https://scorecard.dev/viewer/?uri=github.com/Inqudium/veredium)

Veredium is a web client implementation — named after the Roman *veredus*, the post-horse of
the *cursus publicus*: the mount that carries the envoy to the foreign party and back. Its
sibling [Legatium](https://github.com/Inqudium/legatium) records what the envoy's call came to;
Veredium is the ride itself.

> The project is at its beginning: this repository holds the build, the workflows and the
> documentation site, set up from the sibling projects, and the first module. The client
> itself follows.

## Quickstart

[![veredium-core on Maven Central](https://img.shields.io/maven-central/v/eu.inqudium/veredium-core.svg?label=veredium-core)](https://central.sonatype.com/artifact/eu.inqudium/veredium-core)

The badge shows the current release on Maven Central — use that version where the snippet
says `...`:

```xml
<dependency>
    <groupId>eu.inqudium</groupId>
    <artifactId>veredium-core</artifactId>
    <version>...</version>
</dependency>
```

## About the name

Veredium derives from *veredus*, the post-horse of the *cursus publicus*, the Roman state
courier service: the fast horse a rider took from one station (*mutatio*) to the next, changed
there for a fresh one, and rode on. A web client is exactly that: not the message and not the
envoy, but the mount that carries them to the other side — and the one that has to be sound
for the journey to end well.

The form follows the naming of chemical elements, like its siblings
[**Limesium**](https://github.com/Inqudium/limesium) (the watched frontier, where the inbound
crossings are recorded), [**Legatium**](https://github.com/Inqudium/legatium) (the envoy, whose
outbound calls are recorded) and [**Tabellarium**](https://github.com/Inqudium/tabellarium)
(the letter-carrier, who ships the log events). All are published under the `eu.inqudium`
group, the fictional periodic table of **Inqudium**.

| Module | Role | Root package |
|---|---|---|
| [`veredium-core`](veredium-core/README.md) | the stack-neutral core both clients share: mesh timeout ladder, breaker verdicts, Resilience4j wiring | `eu.inqudium.veredium.core` |
| [`veredium-restclient`](veredium-restclient/README.md) | the blocking client: Spring `RestClient` over Apache HttpClient 5, mesh timeout ladder, Resilience4j | `eu.inqudium.veredium.restclient` |
| [`veredium-webclient`](veredium-webclient/README.md) | the reactive client: Spring `WebClient` over Reactor Netty, mesh timeout ladder, Resilience4j | `eu.inqudium.veredium.webclient` |

[`veredium-examples`](veredium-examples/README.md) shows a host using veredium: one Spring Boot application per
calling style of the two clients (blocking, asynchronous, reactive), one that runs both clients in one context, and
one that bounds a native Apache HttpClient 4 call with the core's `BlockingResilienceDecorator`, each run by the build's tests. It is never
published, and neither are the other two build-only modules,
[`veredium-test-support`](veredium-test-support/README.md) and [`veredium-coverage`](veredium-coverage/README.md).

## Documentation

- **[Core guide](veredium-core/docs/GUIDE.md)** — `veredium-core`: what both clients share, and [the mesh timeout ladder](veredium-core/docs/mesh-timeout.md).
- **[Blocking client guide](veredium-restclient/docs/GUIDE.md)** — `veredium-restclient`: the hand-off, the ladder, the bulkheads, metrics, what differs from the reactive sibling.
- **[Reactive client guide](veredium-webclient/docs/GUIDE.md)** — `veredium-webclient`: configuration, metrics, logging.
- **[Test evidence](https://inqudium.github.io/veredium/tests/test-evidence/)** — the generated
  inventory of the test suite: every test sentence plus its rationale.
- **[Coverage report](https://inqudium.github.io/veredium/coverage/)** — the cross-module JaCoCo
  report of the run that built the site (a class counts as covered when any module's tests run it).
- **[API reference](https://inqudium.github.io/veredium/api/veredium-core/)** — generated with
  Dokka.
- **[Architecture decision records](docs/adr/README.md)** — the decisions, numbered and never
  renumbered.

### Compatibility

Each Veredium release is built and tested against one Spring Boot line, one Kotlin line and one
Java target; the table is the history of those lines, newest first. The Java column is the
bytecode target the artifacts run on - the build itself needs JDK 24+.

| Veredium | Spring Boot | Kotlin | Java |
|---|---|---|---|
| (unreleased) | 4.1.x | 2.4.x | 21 |

## Build

```
mvn verify
```

Maven multi-module build (group `eu.inqudium`), Java 21, Kotlin, Spring Boot parent.

### Reproducible builds

The same source and version produce the same bytes on any machine: the root
POM sets `project.build.outputTimestamp` (bumped in every release commit), so
the jar and source archivers write that instant as every zip entry's
timestamp, sort the entries and normalize their permissions, and the manifests
carry no build user, build JDK or Maven version (`Build-Jdk-Spec` is omitted on
purpose - CI builds on JDK 25, a maintainer on whatever 24+ is installed). The
javadoc jar is rendered by Dokka but packaged by the jar plugin, because
Dokka's own `javadocJar` goal writes the build time and JDK into the archive.
Consequently the jars the Release workflow attaches to the GitHub release
(built on the tag by GitHub Actions, attested with SLSA provenance) and the
jars deployed to Maven Central from a local checkout of the same tag are
byte-identical. To check a build, run `mvn -DskipTests package` twice, or once
on the tag, and compare `sha256sum veredium-core/target/veredium-core-<version>.jar`
with the release asset.

## Contributing

Contributions are welcome — please read [CONTRIBUTING.md](CONTRIBUTING.md) first.
The [Code of Conduct](CODE_OF_CONDUCT.md) applies to all project spaces, and
security issues should be reported privately as described in [SECURITY.md](SECURITY.md).

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
