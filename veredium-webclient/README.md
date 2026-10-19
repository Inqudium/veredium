# veredium-webclient

Veredium, the reactive client: Spring's `WebClient` over Reactor Netty: one resilience-wrapped client per mesh endpoint, the timeout ladder derived from one measured anchor, the three Resilience4j primitives amended at startup, the codec buffer limit and the exchange event of `legatium-webclient-logging`.

## Usage

```xml
<dependency>
    <groupId>eu.inqudium</groupId>
    <artifactId>veredium-webclient</artifactId>
    <version>...</version>
</dependency>
```

The long-form guide is [docs/GUIDE.md](docs/GUIDE.md).
