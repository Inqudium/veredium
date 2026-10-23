# veredium-examples

Worked examples of a host using the two client modules of [Veredium](../README.md): one Spring Boot application
per calling style - blocking, asynchronous, reactive - each with an adapter over two backends and the configuration
it needs. A build-only module: it is never published, and its tests run every example against a stand-in sidecar,
so an example that no longer works fails the build.

## What it shows

Both examples call the same two backends behind the local sidecar: a customer directory (GET by id) and an order
search (POST with a JSON body). Both are configured once in
[`application.yml`](src/main/resources/application.yml).

| File | Shows |
|---|---|
| [`application.yml`](src/main/resources/application.yml) | The whole configuration of a host: the mesh endpoints with their triplets and aliases, the Resilience4j instance every alias needs, legatium's exchange log, the engine knob. |
| [`Customers.kt`](src/main/kotlin/eu/inqudium/veredium/examples/Customers.kt) | The payloads, and the aliases as constants: one string is the client, its Resilience4j instances and its metrics. |
| [`BlockingExampleApplication`](src/main/kotlin/eu/inqudium/veredium/examples/blocking/BlockingExampleApplication.kt) | The wiring of the blocking client: one package scan. |
| [`BlockingCustomerAdapter`](src/main/kotlin/eu/inqudium/veredium/examples/blocking/BlockingCustomerAdapter.kt) | `ResilientRestClient` called blocking: a GET and a POST, a 404 turned into `null`, and the two budgets a `BaseAdapter` publishes. |
| [`AsyncExampleApplication`](src/main/kotlin/eu/inqudium/veredium/examples/async/AsyncExampleApplication.kt) | The same client, the same wiring - called without blocking. |
| [`AsyncCustomerAdapter`](src/main/kotlin/eu/inqudium/veredium/examples/async/AsyncCustomerAdapter.kt) | `getAsync`/`postAsync`: every outcome on a `CompletableFuture`, a 404 mapped to `null` on the future, two calls in flight with `thenCombine`, and a cancel handed on to the calls a derived future came from. |
| [`ReactiveExampleApplication`](src/main/kotlin/eu/inqudium/veredium/examples/reactive/ReactiveExampleApplication.kt) | The same wiring for the reactive client. |
| [`ReactiveCustomerAdapter`](src/main/kotlin/eu/inqudium/veredium/examples/reactive/ReactiveCustomerAdapter.kt) | `ResilientWebClient`: the same adapter on `Mono`s, a 404 turned into an empty `Mono`, `Mono.zip` for both calls, and a `suspend` function for coroutine code. |

The tests under [`src/test/kotlin`](src/test/kotlin/eu/inqudium/veredium/examples) start each application as a
host starts it (`@SpringBootTest`). A WireMock server plays the sidecar. Its stubs answer only the Host header the
triplet names, so a passing test also shows the routing.

## Running them

```
mvn -pl veredium-examples -am verify
```

`-am` builds the client modules first. The applications have a `main` as well, but on their own they only start
and wait for nothing. They are meant to be read and copied, not run.

## Why a module of its own

Examples need a place where they compile against the published API, run against it, and still never ship. None of
the existing modules is that place:

- **Not the client modules' tests.** Their tests use factories and contexts built by hand, which pin one behaviour
  each. A host never builds those. It scans a package and writes YAML, and that is what an example has to show. The
  client modules' tests also cannot see each other, and the point of these examples is the same adapter on both
  stacks, side by side.
- **Not the guides.** A snippet in Markdown is never compiled. When the API changes, the snippet goes stale and
  nothing fails. Here the compiler and the tests catch it.
- **Not a client module's main code.** Example applications and adapters in a library jar would ship to every host,
  including a component scan of an `examples` package.

A module that depends on both clients the way a host does has none of these problems. It is kept out of every
published artifact the same way as [`veredium-coverage`](../veredium-coverage/README.md) and
[`veredium-test-support`](../veredium-test-support/README.md):

- `maven.deploy.skip`: nothing goes to GitHub Packages. `cyclonedx`'s `skipNotDeployed` (on by default) therefore
  also leaves it out of the aggregate SBOM.
- `excludeArtifacts` in the parent's release profile: it stays out of the Maven Central bundle.
- `dokka.skip` and a Dokka plugin declared with `skip`: the API reference has no page for it.
- Not a dependency of `veredium-coverage`: example code does not count towards the library's coverage.

## Consequences

- **Both stacks are on one classpath.** A real host usually picks one. Each example application therefore scans
  exactly one client package. `spring.main.web-application-type: none` keeps `spring-webflux`, which the reactive
  client brings, from starting a web server in any of them.
- **Three Boot contexts share one test JVM.** The tests keep every context off Micrometer's global registry
  (`management.metrics.use-global-registry=false`). A single host does not need that setting.
