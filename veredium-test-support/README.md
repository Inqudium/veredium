# veredium-test-support

The test helpers both client modules of [Veredium](../README.md) share. A build-only module: it is never
published, and no host ever sees it. `veredium-restclient` and `veredium-webclient` take it at **test scope**.

## What it holds

Everything a client module's tests need that does not depend on the client stack, in
`eu.inqudium.veredium.testsupport`:

| Helper | What for |
|---|---|
| `AnsweringBackend`, `DrippingBackend`, `SilentBackend`, `UnansweredConnectBackend` | Raw-socket HTTP peers on the loopback address. They show what WireMock cannot: connections rather than requests, a body still arriving, a peer that cuts the body off or never answers, a connect the kernel leaves hanging. |
| `CapturingAppender`, `capturing` | Thread-safe Logback capture with a bounded wait for the expected lines. It snapshots each event's MDC as it is appended. `capturing` attaches it for one block, opens the logger's level for it if asked, and afterwards detaches and stops it and restores the level. |
| `eventually`, `leapingClock`, `causeChain`, `causeNames` | Bounded waits instead of sleeps, a clock that jumps past any deadline, and cause-chain assertions for engine exceptions whose types are shaded or optional. |
| `WireMockArrivals` | One request listener per WireMock server, cleared before every test. WireMock's `resetAll()` does not remove a listener, so one added per test piles up. |
| `ConfigurationFiles` | Binding of the reference configuration (`application.yml`) and the test fixture (`application-test.yml`) the way Boot reads them, without a context. |
| `Greeting`, `LookupRequest`, `Unserialisable` | The payloads the client tests send and receive. |
| `CustomizedRegistries` | Resilience4j registries with the mesh customizers applied, fed with a stack's verdicts, so a test can run the customized instance without a Spring context. |

A helper that only one module needs stays in that module. So do the builders tied to one client stack: factories,
Spring contexts and mesh fixtures for that stack's types.

## Why a module of its own

A module's `src/test` is invisible to every other module. Two client modules that need the same helper have three
ways to get it:

1. **A copy in each module.** This is how the helpers started, and the copies drifted apart. One socket backend
   reported `localhost` and its twin the loopback address. One appender snapshotted the MDC and the other did not.
   One poller gave up after 2 s and another after 5 s, with a different deadline comparison. Each module also had
   its own YAML loader and its own registry helper. A fix to one copy never reached the other.
2. **A test-jar of `veredium-core`.** Both client modules already depend on the core. But the core is published
   to Maven Central, and the release attaches every artifact of a module, a test-jar included. Library consumers
   would receive test helpers they have no use for.
3. **Main code of a separate, unpublished module.** Helpers written as main code can be taken at test scope.
   Keeping the module out of every published artifact is a matter of configuration, the same way it is done for
   [`veredium-coverage`](../veredium-coverage/README.md):
   - `maven.deploy.skip`, so nothing goes to GitHub Packages, and `cyclonedx`'s `skipNotDeployed` (on by default) keeps it out of
     the aggregate SBOM;
   - an entry in `excludeArtifacts` of the release profile, so it stays out of the Central bundle;
   - Dokka declared with `skip`, so the API reference has no page for it.

This module is option 3.

## Consequences

- **`veredium-core`'s own tests cannot use it.** This module depends on the core, because `CustomizedRegistries`
  needs the mesh customizers. A test dependency back from the core would be a cycle. The core's tests therefore
  keep their own `CustomizedRegistries`; its KDoc says why.
- **It is part of the Dokka module list in CI.** The CI's KDoc check and the Docs workflow run `dokka:dokka` over
  an explicit `-pl` list. That list must contain this module: the client modules compile their tests during
  `package`, and on a fresh runner the module is in neither the reactor nor the local repository. An undeclared
  Dokka plugin fails a command-line run instead of skipping it, so the POM declares the plugin with `skip`.
- **The helpers have tests of their own.** `src/test/kotlin` pins what the suites rely on them for: a bounded wait
  that gives up, a cause-chain walk that stops at a self-cause, a capture that keeps only what it accepts and
  leaves no appender or level behind, an arrival listener that forgets the previous test's actions. The tests also give the module the test source root ktlint
  expects, so `mvn verify` stays free of warnings.

## Using it

```xml
<dependency>
    <groupId>eu.inqudium</groupId>
    <artifactId>veredium-test-support</artifactId>
    <version>${revision}</version>
    <scope>test</scope>
</dependency>
```

Only for modules inside this reactor. The artifact is not published, so a project outside it cannot resolve it.
