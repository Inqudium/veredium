package eu.inqudium.veredium.webclient

import eu.inqudium.veredium.core.mesh.MeshProperties
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.springboot.bulkhead.autoconfigure.BulkheadAutoConfiguration
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource
import org.springframework.boot.env.YamlPropertySourceLoader
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.core.env.EnumerablePropertySource
import org.springframework.core.env.MapPropertySource
import org.springframework.core.io.ClassPathResource

/**
 * The contract between the `app.mesh` block and the `resilience4j.*` block of this module's two configuration
 * files - the reference configuration (`src/test/resources/application.yml`) and the test fixture
 * (`application-test.yml`) - checked statically, from the files alone.
 *
 * 1. What is tested: that every mesh alias (derived from its triplet exactly as at runtime, by binding
 *    `app.mesh` into [MeshProperties]) is declared under `resilience4j.circuitbreaker.instances`,
 *    `resilience4j.timelimiter.instances` AND `resilience4j.bulkhead.instances`; and that the bulkheads of those
 *    aliases together admit no more concurrent calls than the shared pool has connections -
 *    `sum(max-concurrent-calls) <= app.mesh.defaults.transport.max-connections`.
 * 2. Success criteria: for both files the check reports no problem; and the check itself is shown to bite - the
 *    reference configuration with one bulkhead raised by a single permit is reported, naming the sum and the
 *    pool size.
 * 3. Why it matters: both rules are invisible when broken. An undeclared alias still gets an instance - on the
 *    registry default, silently - and the core's `MeshResilienceStartupGuard` refuses that at startup; this test
 *    moves the refusal to the build of the file that documents the rule. An over-committed pool starts fine:
 *    the excess queues for a connection instead of being shed by its bulkhead, and the pending-acquire timeout
 *    that follows is a FAILURE on the breaker of whichever backend is waiting, not necessarily the one holding
 *    the connections. At runtime the startup guard only WARNs about it (a deliberate over-commit can be right
 *    for a host); for the configuration this module publishes as its reference it is a hard rule.
 *
 * The per-alias `max-concurrent-calls` is not read off the YAML by hand: the `resilience4j.bulkhead` keys are
 * handed to Resilience4j's own Boot auto-configuration and the value is read off the instance it builds, so a
 * `base-config`, a `configs.default` or an instance override resolves exactly as it would in the running
 * application. The sum runs over ALL mesh aliases against the one pool size - exact for the blocking stack,
 * conservative here (Reactor Netty caps per remote address, so an endpoint with its own `base-url` gets its own
 * sub-pool), and therefore the safe direction for a reference.
 *
 * Determinism: classpath files and a Spring context that configures and calls nothing.
 */
class ResilienceConfigContractTest {
    @ParameterizedTest
    @ValueSource(strings = [REFERENCE, FIXTURE])
    fun `should declare every mesh alias under all three Resilience4j instance trees`(file: String) {
        // What is tested: the declaration half of the contract for one file - each derived alias appears as an
        //   instance key under circuitbreaker, timelimiter and bulkhead.
        // Success criteria: the list of undeclared (layer, alias) pairs is empty, and the file declares four
        //   aliases, so an empty list is not the result of a mesh block that bound to nothing.
        // Why it matters: an alias missing from one tree runs that primitive on Resilience4j's registry defaults
        //   while the others look configured - the startup guard catches it in a host, this catches it here,
        //   where the file is edited.
        //
        // Given
        val properties = load(file)

        // When
        val aliases = meshAliases(properties)
        val undeclared = undeclaredAliases(properties, aliases)

        // Then
        assertThat(aliases).`as`("mesh aliases derived from %s", file).hasSize(4)
        assertThat(undeclared).`as`("aliases %s does not declare", file).isEmpty()
    }

    @ParameterizedTest
    @ValueSource(strings = [REFERENCE, FIXTURE])
    fun `should keep the mesh bulkheads within the shared connection pool`(file: String) {
        // What is tested: the sizing half for one file - sum(max-concurrent-calls) over the mesh aliases, as
        //   Resilience4j resolves each instance, against app.mesh.defaults.transport.max-connections.
        // Success criteria: no over-commit is reported; the file's four aliases at 25 permits each fill the
        //   100-connection pool exactly.
        // Why it matters: see the class KDoc - an over-committed pool moves the queueing out of the bulkhead and
        //   charges its timeouts to whichever backend happens to wait.
        //
        // Given
        val properties = load(file)

        // When
        val overCommit = poolOverCommit(properties)

        // Then
        assertThat(overCommit).`as`("bulkhead sizing of %s", file).isNull()
    }

    @Test
    fun `should report a bulkhead raised past the pool, naming the sum and the pool size`() {
        // What is tested: that the sizing check above can fail at all - the reference configuration with ONE
        //   instance raised from 25 to 26 permits.
        // Success criteria: the check reports the over-commit with the sum (101) and the pool size (100).
        // Why it matters: a check that reads the wrong keys (a renamed layer, a misspelt prefix, a bulkhead the
        //   registry built from defaults) passes vacuously on every file. The one-permit margin also pins that
        //   the comparison is `<=` and not `<`, since the reference sits exactly on the limit.
        //
        // Given: the reference configuration, one instance raised by one permit
        val reference = load(REFERENCE)
        val alias = meshAliases(reference).first()
        val raised = reference + ("resilience4j.bulkhead.instances.$alias.max-concurrent-calls" to "26")

        // When
        val overCommit = poolOverCommit(raised)

        // Then
        assertThat(overCommit).isNotNull().contains("sum=101", "max-connections=100", "$alias=26")
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /** The flattened keys of [file], the way Boot's `YamlPropertySourceLoader` produces them. */
    private fun load(file: String): Map<String, Any> =
        YamlPropertySourceLoader()
            .load(file, ClassPathResource(file))
            .filterIsInstance<EnumerablePropertySource<*>>()
            .flatMap { source -> source.propertyNames.map { it to source.getProperty(it)!! } }
            .toMap()

    /** `app.mesh` bound into [MeshProperties] - the bound object, whose code defaults fill what the file omits. */
    private fun mesh(properties: Map<String, Any>): MeshProperties =
        Binder(MapConfigurationPropertySource(properties))
            .bind(MeshProperties.PREFIX, MeshProperties::class.java)
            .orElseThrow { AssertionError("app.mesh did not bind") }

    /** The derived alias of every mesh endpoint, in declaration order. */
    private fun meshAliases(properties: Map<String, Any>): List<String> = mesh(properties).resolveAll().values.map { it.alias }

    /** Every (layer, alias) pair the file does not declare under `resilience4j.<layer>.instances.<alias>`. */
    private fun undeclaredAliases(
        properties: Map<String, Any>,
        aliases: List<String>,
    ): List<String> =
        LAYERS.flatMap { layer ->
            aliases
                .filter { alias -> properties.keys.none { it.startsWith("resilience4j.$layer.instances.$alias.") } }
                .map { alias -> "resilience4j.$layer.instances.$alias" }
        }

    /**
     * The over-commit of the pool as a message, or `null` when the mesh bulkheads fit. Each instance's
     * `max-concurrent-calls` comes from the registry Resilience4j's auto-configuration builds from the file's
     * `resilience4j.bulkhead` keys (class KDoc); undeclared aliases are the other test's business and fail it.
     */
    private fun poolOverCommit(properties: Map<String, Any>): String? {
        val mesh = mesh(properties)
        val aliases = mesh.resolveAll().values.map { it.alias }
        assertThat(undeclaredAliases(properties, aliases)).`as`("the sizing check needs every bulkhead declared").isEmpty()
        val maxConnections = mesh.defaults.transport.maxConnections
        var permits: Map<String, Int> = emptyMap()
        ApplicationContextRunner()
            .withInitializer { context ->
                val bulkheadKeys = properties.filterKeys { it.startsWith("resilience4j.bulkhead.") }
                context.environment.propertySources.addFirst(MapPropertySource("bulkhead-keys", bulkheadKeys))
            }.withConfiguration(AutoConfigurations.of(BulkheadAutoConfiguration::class.java))
            .run { context ->
                assertThat(context).hasNotFailed()
                val registry = context.getBean(BulkheadRegistry::class.java)
                permits = aliases.associateWith { registry.bulkhead(it).bulkheadConfig.maxConcurrentCalls }
            }
        val sum = permits.values.sum()
        return if (sum <= maxConnections) {
            null
        } else {
            "sum=$sum > max-connections=$maxConnections (" + permits.entries.joinToString(", ") { (alias, calls) -> "$alias=$calls" } + ")"
        }
    }

    private companion object {
        /** The reference configuration: the documented example a host copies from. */
        const val REFERENCE = "application.yml"

        /** The test-owned fixture the binding and contract tests of the logging blocks read. */
        const val FIXTURE = "application-test.yml"

        /** The three primitives [ReactiveResilienceDecorator] applies to every call. */
        val LAYERS = listOf("circuitbreaker", "timelimiter", "bulkhead")
    }
}
