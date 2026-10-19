# veredium-core

The core module of [Veredium](../README.md): the stack-neutral part both client modules share - the mesh
timeout ladder (`app.mesh`), the breaker verdicts and the Resilience4j customizer wiring. A host gets it
transitively with `veredium-restclient` or `veredium-webclient`.

## Usage

```xml
<dependency>
    <groupId>eu.inqudium</groupId>
    <artifactId>veredium-core</artifactId>
    <version>...</version>
</dependency>
```

The long-form guide is [docs/GUIDE.md](docs/GUIDE.md).
