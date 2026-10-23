package eu.inqudium.veredium.core.resilience

import eu.inqudium.veredium.core.mesh.MeshLadderLogger
import eu.inqudium.veredium.core.mesh.MeshProperties
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.common.circuitbreaker.configuration.CircuitBreakerConfigCustomizer
import io.github.resilience4j.common.timelimiter.configuration.CommonTimeLimiterConfigurationProperties
import io.github.resilience4j.common.timelimiter.configuration.TimeLimiterConfigCustomizer
import io.github.resilience4j.spring6.timelimiter.configure.TimeLimiterConfigurationProperties
import io.github.resilience4j.springboot.bulkhead.autoconfigure.BulkheadAutoConfiguration
import io.github.resilience4j.springboot.circuitbreaker.autoconfigure.CircuitBreakerAutoConfiguration
import io.github.resilience4j.springboot.timelimiter.autoconfigure.TimeLimiterAutoConfiguration
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.context.properties.source.ConfigurationPropertySources
import org.springframework.boot.env.YamlPropertySourceLoader
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.core.env.EnumerablePropertySource
import org.springframework.core.env.MapPropertySource
import org.springframework.core.io.ClassPathResource
import java.io.IOException
import java.util.function.Supplier

/**
 * Boots Resilience4j's own Boot auto-configuration on the `resilience4j.*` and `app.mesh.*` blocks of the test
 * fixture, with the shared mesh wiring the way a client stack imports it (`TestVerdictsConfig`: the verdicts
 * bean plus `MeshTimeoutConfig` - the per-alias customizer beans, the startup guard, the ladder logger) - and
 * pins both directions of the guard.
 *
 * 1. What is tested: that every instance the fixture declares is built WITH the mesh customizer's effect (the
 *    derived budget on each TimeLimiter, the two breaker verdicts on each CircuitBreaker), and that a
 *    configuration which does not declare an instance - or a registry the auto-configuration did not build -
 *    does not start.
 * 2. How it is deemed successful: the healthy context starts and every endpoint's instances show the
 *    customizer's effect; the two broken contexts fail with the guard's message naming the key respectively
 *    the unreached instance.
 * 3. Why it matters: a `*ConfigCustomizer` is consulted ONLY while Resilience4j's Boot configuration builds an
 *    `instances.<name>` entry. The reactive stack's configuration once declared a single TimeLimiter instance
 *    while mesh derives four; under the customizer mechanism three endpoints would then have run on the
 *    library default without a word. Only a real context shows that the keys, the derived aliases, the
 *    customizer names and the guard agree - and that the guard fails the start when they do not. Each client
 *    module boots its own fixture the same way (`MeshResilienceShippedConfigTest`), with its real verdicts.
 *
 * Determinism: a real context, no I/O beyond the classpath, no HTTP stack - nothing is called, only configured.
 */
class MeshResilienceContextTest {
    @Test
    fun `should start on the fixture with every mesh endpoint customized`() {
        // What is tested: the real Spring wiring on the test fixture - Resilience4j's Boot auto-configurations
        //   plus the mesh registrar - and what it leaves on every endpoint's TimeLimiter and CircuitBreaker.
        // Success criteria: the context starts; for each of the four endpoints there is a circuit-breaker and a
        //   time-limiter customizer bean named after the alias, the TimeLimiter carries the derived budget with
        //   cancelRunningFuture kept, and the breaker ignores client-side failures, does not record a 404 and
        //   keeps the YAML's failure-rate threshold of 50.
        // Why it matters: the unit tests prove each customizer in isolation; only a real context shows that
        //   Resilience4j actually consults them by bean name and that the YAML's own policy survives them.

        // Given / When: the context starts on the fixture's resilience4j + app.mesh blocks
        fixtureContext().run { context ->
            assertThat(context).hasNotFailed()
            val mesh = context.getBean(MeshProperties::class.java)
            val timeLimiters = context.getBean(TimeLimiterRegistry::class.java)
            val circuitBreakers = context.getBean(CircuitBreakerRegistry::class.java)
            val endpoints = mesh.resolveAll().values

            // Then: the fixture declares four endpoints (a sanity bound on the loop below), and the ladder logger
            // is wired by the import rather than by a scan of this package
            assertThat(endpoints).hasSize(4)
            assertThat(context).hasSingleBean(MeshLadderLogger::class.java)

            endpoints.forEach { endpoint ->
                // ... one customizer bean per alias and layer, named so Resilience4j consulted it
                assertThat(context).hasBean(MeshResilienceCustomizerRegistrar.CIRCUIT_BREAKER_BEAN_PREFIX + endpoint.alias)
                assertThat(context).hasBean(MeshResilienceCustomizerRegistrar.TIME_LIMITER_BEAN_PREFIX + endpoint.alias)
                assertThat(context.getBean(MeshResilienceCustomizerRegistrar.CIRCUIT_BREAKER_BEAN_PREFIX + endpoint.alias))
                    .isInstanceOf(CircuitBreakerConfigCustomizer::class.java)
                assertThat(context.getBean(MeshResilienceCustomizerRegistrar.TIME_LIMITER_BEAN_PREFIX + endpoint.alias))
                    .isInstanceOf(TimeLimiterConfigCustomizer::class.java)

                // ... each TimeLimiter carries the derived budget and keeps the YAML's cancelRunningFuture
                val timeLimiter = timeLimiters.timeLimiter(endpoint.alias).timeLimiterConfig
                assertThat(timeLimiter.timeoutDuration)
                    .`as`("TimeLimiter budget of %s", endpoint.alias)
                    .isEqualTo(endpoint.timeLimiterTimeout)
                assertThat(timeLimiter.shouldCancelRunningFuture()).`as`("cancelRunningFuture of %s", endpoint.alias).isTrue()

                // ... each CircuitBreaker carries both verdicts on top of the YAML's mesh-backend policy
                val circuitBreaker = circuitBreakers.circuitBreaker(endpoint.alias).circuitBreakerConfig
                assertThat(circuitBreaker.ignoreExceptionPredicate.test(TestBufferLimitException()))
                    .`as`("CircuitBreaker of %s must ignore a client-side failure", endpoint.alias)
                    .isTrue()
                assertThat(circuitBreaker.ignoreExceptionPredicate.test(TestVerdicts.bulkheadRejection())).isTrue()
                assertThat(circuitBreaker.recordExceptionPredicate.test(TestResponseException(404)))
                    .`as`("CircuitBreaker of %s must not record a benign 4xx", endpoint.alias)
                    .isFalse()
                assertThat(circuitBreaker.failureRateThreshold).`as`("mesh-backend policy of %s survives", endpoint.alias).isEqualTo(50f)
            }
        }
    }

    @Test
    fun `should start and keep a benign 4xx unrecorded when an instance lists a superclass of it in record-exceptions`() {
        // What is tested: the record verdict in the real wiring against an operator's record-exceptions class
        //   list - the fixture plus `record-exceptions: [java.lang.RuntimeException]` on one breaker instance, a
        //   superclass of the stack's 4xx exception.
        // Success criteria: the context STARTS (the guard finds the verdict in place); the instance does not
        //   record the benign 404, does record a listed 503, and does not record an IOException - which the
        //   registry default would, so that assertion is also the proof the list reached the instance.
        // Why it matters: Resilience4j's Boot configuration sets the list on the builder and its build() OR-s it
        //   back in past any predicate a customizer sets. Before the customizer cleared the list, this exact
        //   configuration failed the start with a diagnosis blaming a hand-built registry - only a real context
        //   shows that the property binding, the customizer and build() now compose the way the KDoc says.
        //
        // Given: the fixture, one breaker instance carrying a record-exceptions list
        val alias = firstAlias()
        val properties = fixtureProperties() + ("resilience4j.circuitbreaker.instances.$alias.record-exceptions[0]" to "java.lang.RuntimeException")

        // When
        fixtureContext(properties).run { context ->
            // Then
            assertThat(context).hasNotFailed()
            val config = context.getBean(CircuitBreakerRegistry::class.java).circuitBreaker(alias).circuitBreakerConfig
            assertThat(config.recordExceptionPredicate.test(TestResponseException(404))).`as`("benign 404").isFalse()
            assertThat(config.recordExceptionPredicate.test(TestResponseException(503))).`as`("listed 503").isTrue()
            assertThat(config.recordExceptionPredicate.test(IOException("outside the list"))).`as`("the list is in force").isFalse()
            assertThat(config.ignoreExceptionPredicate.test(TestVerdicts.bulkheadRejection())).isTrue()
        }
    }

    @Test
    fun `should refuse to start when an endpoint's time limiter is not declared`() {
        // What is tested: the silent case the guard exists for, in the real wiring - the fixture minus one
        //   TimeLimiter instance, as the reactive stack's configuration once shipped. Resilience4j still creates
        //   the instance for the customizer (on the registry default, not on mesh-backend), so nothing else
        //   would notice.
        // Success criteria: the context fails, and the failure names the key that is missing - and only it.
        // Why it matters: an undeclared instance runs on the registry default instead of the mesh-backend policy,
        //   with no error anywhere; refusing the start is the only point at which that drift is visible.
        //
        // Given: the fixture properties without one endpoint's time-limiter instance
        val missing = firstAlias()
        val withoutInstance = fixtureProperties().filterKeys { !it.startsWith("resilience4j.timelimiter.instances.$missing.") }
        assertThat(withoutInstance).`as`("the filter must have removed something").hasSizeLessThan(fixtureProperties().size)

        // When / Then
        fixtureContext(withoutInstance).run { context ->
            assertThat(context).hasFailed()
            assertThat(rootCauseOf(context.startupFailure))
                .hasMessageContaining("1 problem(s)")
                .hasMessageContaining("resilience4j.timelimiter.instances.$missing is not declared")
        }
    }

    @Test
    fun `should refuse to start when a registry was not built by the auto-configuration`() {
        // What is tested: the second silent case - a TimeLimiterRegistry bean a host built by hand. Every alias is
        //   declared and present on it, so the declaration check passes, but Resilience4j's Boot configuration
        //   never ran on that registry and the customizer beans were never consulted.
        // Success criteria: the context fails naming the time limiter as not reached.
        // Why it matters: a hand-built registry hands out instances without the derived budget, so every call
        //   would run on Resilience4j's default timeout while the declaration check reports all is well.
        //
        // Given: the fixture properties, the CB and bulkhead auto-configurations, and for the time limiter a
        // hand-made registry next to hand-filled properties that declare every alias
        val aliases = fixtureAliases()
        val handMade = TimeLimiterRegistry.ofDefaults().also { registry -> aliases.forEach { registry.timeLimiter(it) } }
        val declared =
            TimeLimiterConfigurationProperties().also { properties ->
                aliases.forEach { properties.instances[it] = CommonTimeLimiterConfigurationProperties.InstanceProperties() }
            }
        val runner =
            ApplicationContextRunner()
                .withInitializer { context ->
                    context.environment.propertySources.addFirst(MapPropertySource(FIXTURE_PROPERTY_SOURCE, fixtureProperties()))
                }.withConfiguration(
                    AutoConfigurations.of(CircuitBreakerAutoConfiguration::class.java, BulkheadAutoConfiguration::class.java),
                ).withBean(TimeLimiterRegistry::class.java, Supplier { handMade })
                .withBean(TimeLimiterConfigurationProperties::class.java, Supplier { declared })
                .withUserConfiguration(TestVerdictsConfig::class.java)

        // When / Then
        runner.run { context ->
            assertThat(context).hasFailed()
            assertThat(rootCauseOf(context.startupFailure))
                .hasMessageContaining("the timelimiter '${aliases.first()}'")
                .hasMessageContaining("did not reach it")
        }
    }

    /**
     * The context the way a host assembles its resilience side: Resilience4j's three auto-configurations
     * reading the fixture's `resilience4j.*` keys, and a stack's verdicts configuration, which imports the mesh
     * wiring - the `app.mesh.*` binding, the customizer registrar, the guard and the ladder logger. Nothing
     * else - no client, no factory, no pool.
     */
    private fun fixtureContext(properties: Map<String, Any> = fixtureProperties()): ApplicationContextRunner =
        ApplicationContextRunner()
            .withInitializer { context ->
                context.environment.propertySources.addFirst(MapPropertySource(FIXTURE_PROPERTY_SOURCE, properties))
            }.withConfiguration(
                AutoConfigurations.of(
                    TimeLimiterAutoConfiguration::class.java,
                    CircuitBreakerAutoConfiguration::class.java,
                    BulkheadAutoConfiguration::class.java,
                ),
            ).withUserConfiguration(TestVerdictsConfig::class.java)

    /**
     * The [FIXTURE_PREFIXES] keys of the fixture, flattened the way Boot would load them. Read from the file
     * rather than restated here: a copy would keep passing after the fixture changed, which is the one thing
     * this test exists to catch.
     */
    private fun fixtureProperties(): Map<String, Any> =
        YamlPropertySourceLoader()
            .load(FIXTURE, ClassPathResource(FIXTURE))
            .filterIsInstance<EnumerablePropertySource<*>>()
            .flatMap { source ->
                source.propertyNames
                    .filter { name -> FIXTURE_PREFIXES.any(name::startsWith) }
                    .map { it to source.getProperty(it)!! }
            }.toMap()
            .also { loaded ->
                // A prefix that silently matches nothing would start the context on Resilience4j's defaults and
                // turn the assertion into a statement about an empty registry.
                FIXTURE_PREFIXES.forEach { prefix ->
                    assertThat(loaded.keys)
                        .`as`("the fixture must carry '$prefix*' keys")
                        .anyMatch { it.startsWith(prefix) }
                }
            }

    /** The aliases of the fixture's endpoints, in file order. */
    private fun fixtureAliases(): List<String> {
        val sources = YamlPropertySourceLoader().load(FIXTURE, ClassPathResource(FIXTURE))
        return Binder(ConfigurationPropertySources.from(sources))
            .bind(MeshProperties.PREFIX, MeshProperties::class.java)
            .orElseThrow { AssertionError("${MeshProperties.PREFIX} did not bind from $FIXTURE") }
            .resolveAll()
            .values
            .map { it.alias }
    }

    /** The alias of the first endpoint, the one the negative test strips. */
    private fun firstAlias(): String = fixtureAliases().first()

    /** The innermost cause - the guard's own exception, however Spring wrapped it on the way out of refresh. */
    private fun rootCauseOf(failure: Throwable?): Throwable = generateSequence(failure) { it.cause.takeIf { cause -> cause !== it } }.last()

    private companion object {
        private const val FIXTURE = "application-test.yml"
        private val FIXTURE_PREFIXES = listOf("resilience4j.", "app.mesh.")
        private const val FIXTURE_PROPERTY_SOURCE = "fixture-application-test-yml"
    }
}
