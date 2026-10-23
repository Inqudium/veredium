package eu.inqudium.veredium.restclient

import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.testsupport.ConfigurationFiles
import eu.inqudium.veredium.testsupport.ConfigurationFiles.FIXTURE
import eu.inqudium.veredium.testsupport.ConfigurationFiles.REFERENCE
import io.github.resilience4j.bulkhead.BulkheadConfig
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

/**
 * Contract test for the two INVARIANTS the `resilience4j.*` block of this module's configuration owes the mesh,
 * pinned in BOTH the reference configuration and the fixture (the rule in [ConfigurationFiles]): nobody tunes
 * an invariant by hand, so a drift in either file has to fail the build.
 *
 * 1. The bulkheads are a PARTITION of the shared connection pool: `sum(max-concurrent-calls)` over the mesh
 *    aliases must not exceed `app.mesh.defaults.transport.max-connections`. Apache's pool caps the total over
 *    every route, so bulkheads that together admit more calls than it has connections move the queueing from
 *    the bulkhead - an immediate, named `BulkheadFullException`, ignored by the breaker - into the pool, where a
 *    pending-acquire timeout is a FAILURE on the breaker of whichever backend is waiting at that moment. That
 *    is error propagation across backends through the pool. At runtime the core's `MeshResilienceStartupGuard`
 *    only WARNs about it (an over-committed pool can be a deliberate choice for a host); the configuration this
 *    module documents itself with must not make that choice by accident.
 * 2. Every mesh alias is declared on all three layers - `circuitbreaker`, `timelimiter` and `bulkhead`
 *    `instances`. An alias missing from a layer does not mean "off": the decorator applies all three
 *    unconditionally, and the undeclared one runs on Resilience4j's registry default instead of the named base
 *    config. The guard fails such a start; this test finds it in the file, before any context exists.
 *
 * The aliases are the DERIVED ones - `MeshProperties.resolveAll()` over the bound `app.mesh` block - not a list
 * restated here, so an endpoint added to the mesh without its resilience entries is caught too. The per-alias
 * bulkhead value is resolved the way Resilience4j's Boot properties resolve it: the instance's own
 * `max-concurrent-calls`, else its `base-config`'s, else the `default` config's, else the library default.
 *
 * No context, no I/O beyond the classpath: `YamlPropertySourceLoader` + `Binder` via [ConfigurationFiles].
 */
class ResilienceConfigContractTest {
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = [REFERENCE, FIXTURE])
    fun `should size the mesh bulkheads within the shared connection pool`(file: String) {
        // What is tested: the pool invariant of the class KDoc - the bulkhead permits of every mesh alias,
        //   resolved through base-config as Resilience4j resolves them, summed against the pool's
        //   max-connections, in the file named by the parameter.
        // Success criteria: the sum is at most max-connections; the per-alias resolution found a value for each
        //   of the file's aliases (a sanity bound, so an empty alias set cannot pass vacuously).
        // Why it matters: a broken invariant is invisible until the pool runs dry under load - and then it shows
        //   as breakers opening on HEALTHY backends whose calls happened to wait behind a slow one. The startup
        //   guard warns, but a WARN in a reference configuration is a recommendation followed by copy-and-paste.
        //
        // Given: the mesh as the file declares it, and the file's bulkhead block
        val mesh = ConfigurationFiles.bind(file, "app.mesh", MeshProperties::class.java)
        val aliases = mesh.resolveAll().values.map { it.alias }
        val bulkheads = ConfigurationFiles.bindMap(file, "resilience4j.bulkhead")
        assertThat(aliases).`as`("the mesh endpoints of %s", file).isNotEmpty()

        // When: each alias's permits, resolved like Resilience4j does, and their sum
        val permits = aliases.associateWith { maxConcurrentCalls(bulkheads, it) }
        val total = permits.values.sum()

        // Then
        assertThat(total)
            .`as`(
                "sum(resilience4j.bulkhead.instances.<alias>.max-concurrent-calls) in %s (%s) against " +
                    "app.mesh.defaults.transport.max-connections",
                file,
                permits,
            ).isLessThanOrEqualTo(mesh.defaults.transport.maxConnections)
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = [REFERENCE, FIXTURE])
    fun `should declare every mesh alias on all three resilience layers`(file: String) {
        // What is tested: the second invariant of the class KDoc - for every derived mesh alias, an
        //   `instances.<alias>` entry under resilience4j.circuitbreaker, .timelimiter and .bulkhead.
        // Success criteria: the set of aliases missing from each layer is empty, and the declared instances are
        //   not empty either (a layer that binds nothing at all must not satisfy the check).
        // Why it matters: an undeclared layer silently runs that backend on Resilience4j's registry defaults -
        //   values nobody chose - and the mesh customizer's verdicts land on an instance built without the
        //   named base config. The startup guard refuses such a context; this catches the same drift as a
        //   failing test at the file, where it is cheapest to fix.
        //
        // Given
        val aliases =
            ConfigurationFiles
                .bind(file, "app.mesh", MeshProperties::class.java)
                .resolveAll()
                .values
                .map { it.alias }

        // When: the instance names each layer declares
        val declared = LAYERS.associateWith { layer -> instanceNames(ConfigurationFiles.bindMap(file, "resilience4j.$layer")) }

        // Then
        LAYERS.forEach { layer ->
            assertThat(declared.getValue(layer)).`as`("resilience4j.%s.instances in %s", layer, file).isNotEmpty()
            assertThat(aliases.filterNot { it in declared.getValue(layer) })
                .`as`("mesh aliases without a resilience4j.%s.instances entry in %s", layer, file)
                .isEmpty()
        }
    }

    /**
     * The `max-concurrent-calls` Resilience4j's Boot properties give [alias]: its own value, else its
     * `base-config`'s, else the `default` config's, else [BulkheadConfig]'s default.
     */
    private fun maxConcurrentCalls(
        bulkhead: Map<String, String>,
        alias: String,
    ): Int {
        val own = bulkhead["instances.$alias.max-concurrent-calls"]
        val baseConfig = bulkhead["instances.$alias.base-config"] ?: "default"
        val inherited = bulkhead["configs.$baseConfig.max-concurrent-calls"]
        return (own ?: inherited)?.toInt() ?: BulkheadConfig.ofDefaults().maxConcurrentCalls
    }

    /** The `<name>` of every `instances.<name>.<key>` entry of a flattened layer block. */
    private fun instanceNames(layer: Map<String, String>): Set<String> =
        layer.keys
            .filter { it.startsWith("instances.") }
            .map { it.removePrefix("instances.").substringBefore('.') }
            .toSet()

    private companion object {
        /** The three `resilience4j.<layer>` blocks the decorator reads an instance from on every call. */
        val LAYERS = listOf("circuitbreaker", "timelimiter", "bulkhead")
    }
}
