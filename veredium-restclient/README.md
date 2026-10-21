# veredium-restclient

Veredium, the blocking client: Spring's `RestClient` over Apache HttpClient 5 (or the JDK client, Jetty or Reactor Netty, per `app.restclient.engine` - for this module's clients only, see the guide's "Engines"): one resilience-wrapped client per mesh endpoint, the timeout ladder derived from one measured anchor, the three Resilience4j primitives installed through the library's own customizers, a bounded body read and the exchange event of `legatium-restclient-logging`.

## Usage

```xml
<dependency>
    <groupId>eu.inqudium</groupId>
    <artifactId>veredium-restclient</artifactId>
    <version>...</version>
</dependency>
```

The long-form guide is [docs/GUIDE.md](docs/GUIDE.md).
