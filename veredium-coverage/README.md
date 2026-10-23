# veredium-coverage

The cross-module test coverage report of [Veredium](../README.md). A build-only module: it carries no code, is
never published, and exists only to run JaCoCo's `report-aggregate` once every other module has been tested.

## What it produces

`mvn clean verify` writes the merged report to `target/site/jacoco-aggregate/` (HTML, `jacoco.xml`,
`jacoco.csv`). That report is the number the project publishes:

- the coverage badge (`.github/scripts/generate-coverage-badge.py`);
- the coverage line of the CI job summary;
- the `/coverage/` page of the documentation site.

Read it only from a `mvn clean verify`. A build without `clean` can count stale classes left in `target/classes`.

## Why a module of its own

Each library module already writes its own report to `target/site/jacoco/`. That report only counts what that
module's tests run. Much of `veredium-core` is exercised by the client modules' suites: the engine checks, the
drain, the mesh ladder as a client resolves it, the customized breakers. In the core's own report those lines
show as uncovered, although a test runs them on every build. The truthful number needs every module's execution
data (`jacoco.exec`) merged and applied to every module's classes. That is what `report-aggregate` does.

`report-aggregate` reports on the modules the reporting project **depends on**, and it has to run after all of
them have run their tests. Neither the existing modules nor the parent can be that project:

- **Not a library module.** `veredium-core` cannot depend on the client modules that depend on it, and a client
  module would only ever see itself and the core, never its twin. Any library module that listed all the others
  as dependencies would also ship those dependencies to every host.
- **Not the parent POM.** In the reactor the parent is built first, before any child has run a test, so it has
  no execution data to merge. Its dependencies would also be inherited by every module.

A separate module that depends on all library modules fixes both:

- The reactor orders it last, because it depends on every other module.
- `report-aggregate` knows exactly which modules to include.
- No library module gains a dependency.

The dependencies are compile scope only so that `report-aggregate` sees the modules. Nothing in this module uses
them.

## Kept out of every published artifact

- `maven.deploy.skip`: nothing goes to GitHub Packages. `cyclonedx`'s `skipNotDeployed` (on by default) therefore also leaves it
  out of the aggregate SBOM.
- `excludeArtifacts` in the parent's release profile: it stays out of the Maven Central bundle.
- `dokka.skip` and absence from the `-pl` lists of the Dokka steps: the API reference has no page for it.

The test helper module [`veredium-test-support`](../veredium-test-support/README.md) and the usage examples in
[`veredium-examples`](../veredium-examples/README.md) are kept out of publication the same way. Neither is part of
the aggregate: their classes are test infrastructure and example code, not library code.
