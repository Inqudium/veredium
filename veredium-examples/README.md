# veredium-examples

Worked examples of a host using [Veredium](../README.md): one Spring Boot application per calling style of the two
client modules - blocking, asynchronous, reactive - each with an adapter over two backends and the configuration
it needs, and one that bounds a blocking call of its own with the core's `BlockingResilienceDecorator`. A
build-only module: it is never published, and its tests run every example against a stand-in sidecar, so an
example that no longer works fails the build.

## What it shows

The client examples call the same two backends behind the local sidecar: a customer directory (GET by id) and an
order search (POST with a JSON body). The decorator example reads the same customer directory with a native client.
All of them are configured once in [`application.yml`](src/main/resources/application.yml).

| File | Shows |
|---|---|
| [`application.yml`](src/main/resources/application.yml) | The whole configuration of a host: the mesh endpoints with their triplets and aliases, the Resilience4j instance every alias needs, legatium's exchange log, the engine knob. |
| [`Customers.kt`](src/main/kotlin/eu/inqudium/veredium/examples/Customers.kt) | The payloads, and the aliases as constants: one string is the client, its Resilience4j instances and its metrics. |
| [`BlockingExampleApplication`](src/main/kotlin/eu/inqudium/veredium/examples/blocking/BlockingExampleApplication.kt) | The wiring of the blocking client: none beyond the dependency - the module's auto-configuration brings it. The one line it needs here excludes the reactive client's auto-configuration, which this module also has on its classpath. |
| [`BlockingCustomerAdapter`](src/main/kotlin/eu/inqudium/veredium/examples/blocking/BlockingCustomerAdapter.kt) | `ResilientRestClient` called blocking: a GET and a POST, a 404 turned into `null`, and the two budgets a `BaseAdapter` publishes. |
| [`AsyncExampleApplication`](src/main/kotlin/eu/inqudium/veredium/examples/async/AsyncExampleApplication.kt) | The same client, the same wiring - called without blocking. |
| [`AsyncCustomerAdapter`](src/main/kotlin/eu/inqudium/veredium/examples/async/AsyncCustomerAdapter.kt) | `getAsync`/`postAsync`: every outcome on a `CompletableFuture`, a 404 mapped to `null` on the future, two calls in flight with `thenCombine`, and a cancel handed on to the calls a derived future came from. |
| [`ReactiveExampleApplication`](src/main/kotlin/eu/inqudium/veredium/examples/reactive/ReactiveExampleApplication.kt) | The same wiring for the reactive client. |
| [`ReactiveCustomerAdapter`](src/main/kotlin/eu/inqudium/veredium/examples/reactive/ReactiveCustomerAdapter.kt) | `ResilientWebClient`: the same adapter on `Mono`s, a 404 turned into an empty `Mono`, `Mono.zip` for both calls, and a `suspend` function for coroutine code. |
| [`DecoratorExampleApplication`](src/main/kotlin/eu/inqudium/veredium/examples/decorator/DecoratorExampleApplication.kt) | A host without a client module: it excludes both clients' auto-configurations, which this module has on its classpath and a real host of the decorator alone would not. |
| [`DecoratorConfiguration`](src/main/kotlin/eu/inqudium/veredium/examples/decorator/DecoratorConfiguration.kt) | The wiring a host declares itself for `BlockingResilienceDecorator`: the bean over the auto-configured registries, the breaker customizer that ignores `SELF_INFLICTED_FAILURES` and the backend's own "unknown customer" answer, and a native Apache HttpClient 4 with its bounds, no retries and no redirects. |
| [`DecoratorCustomerAdapter`](src/main/kotlin/eu/inqudium/veredium/examples/decorator/DecoratorCustomerAdapter.kt) | `decorate` and `decorateAsync` by instance name around a HttpClient 4 exchange that sets its own Host header, a 404 that the breaker ignores, and the two `BaseAdapter` budgets from the decorator and the client. |

The tests under [`src/test/kotlin`](src/test/kotlin/eu/inqudium/veredium/examples) start each application as a
host starts it (`@SpringBootTest`, through the one test annotation `ExampleContext`, which also injects the beans
through the test's constructor). A WireMock server plays the sidecar (`Sidecar`), and every property that names the
sidecar is pointed at it in one place. Its stubs answer only the Host header the
triplet names, so a passing test also shows the routing. The decorator example's tests also show the layered
bounds: against a silent backend HttpClient 4's socket timeout trips first; against a body that drips in, each read
inside the socket timeout, the decorator's budget releases the caller and its interrupt ends the worker's read.

## Running them

```
mvn -pl veredium-examples -am verify
```

`-am` builds the client modules first. The applications have a `main` as well, but on their own they only start
and wait for nothing. They are meant to be read and copied, not run.

## Why a module of its own

Examples need a place where they compile against the published API, run against it, and still never ship. None of
the existing modules is that place:

- **Not the library modules' tests.** Their tests use factories and contexts built by hand, which pin one behaviour
  each. A host never builds those. It adds a dependency and writes YAML, and that is what an example has to show. The
  client modules' tests also cannot see each other, and the point of these examples is the same adapter on both
  stacks, side by side.
- **Not the guides.** A snippet in Markdown is never compiled. When the API changes, the snippet goes stale and
  nothing fails. Here the compiler and the tests catch it.
- **Not a client module's main code.** Example applications and adapters in a library jar would ship to every host
  that depends on it.

A module that depends on both clients the way a host does has none of these problems. Its one dependency of its
own, Apache HttpClient 4 for the decorator example, stays in this module too. It is kept out of every
published artifact the same way as [`veredium-coverage`](../veredium-coverage/README.md) and
[`veredium-test-support`](../veredium-test-support/README.md):

- `maven.deploy.skip`: nothing goes to GitHub Packages. `cyclonedx`'s `skipNotDeployed` (on by default) therefore
  also leaves it out of the aggregate SBOM.
- `excludeArtifacts` in the parent's release profile: it stays out of the Maven Central bundle.
- `dokka.skip` and a Dokka plugin declared with `skip`: the API reference has no page for it.
- Not a dependency of `veredium-coverage`: example code does not count towards the library's coverage.

## Consequences

- **Both stacks are on one classpath.** A real host usually picks one, and the two cannot share a context yet
  ([ADR-0003](../docs/adr/ADR-0003-each-mesh-endpoint-is-owned-by-one-client-stack.md)). Each client example therefore
  excludes the other stack's auto-configuration, and the decorator example both.
  `spring.main.web-application-type: none` keeps `spring-webflux`, which the reactive client brings, from starting
  a web server in any of them.
- **Four Boot contexts share one test JVM.** The tests keep every context off Micrometer's global registry
  (`management.metrics.use-global-registry=false`, set once in `ExampleContext`). A single host does not need that
  setting.
