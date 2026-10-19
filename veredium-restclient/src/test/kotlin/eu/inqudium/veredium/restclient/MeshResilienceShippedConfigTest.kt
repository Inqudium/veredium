package eu.inqudium.veredium.restclient

import eu.inqudium.veredium.restclient.ConfigurationFiles.SHIPPED
import eu.inqudium.veredium.restclient.mesh.MeshProperties
import eu.inqudium.veredium.restclient.mesh.MeshTimeoutConfig
import eu.inqudium.veredium.restclient.resilience.MeshResilienceCustomizerRegistrar
import io.github.resilience4j.bulkhead.Bulkhead
import io.github.resilience4j.bulkhead.BulkheadConfig
import io.github.resilience4j.bulkhead.BulkheadFullException
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
import org.springframework.boot.env.YamlPropertySourceLoader
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.core.env.EnumerablePropertySource
import org.springframework.core.env.MapPropertySource
import org.springframework.core.io.ClassPathResource
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.web.client.HttpClientErrorException
import java.util.function.Supplier

/**
 * Boots Resilience4j's own Boot auto-configuration on the SHIPPED `resilience4j.*` and `app.mesh.*` blocks of
 * `src/main/resources/application.yml`, with this module's mesh wiring (`MeshTimeoutConfig`: the per-alias
 * customizer beans and the startup guard) - and pins both directions of the guard.
 *
 * 1. What is tested: that every instance the shipped file declares is built WITH the mesh customizer's effect
 *    (the derived budget on each TimeLimiter, the two breaker verdicts on each CircuitBreaker), and that a
 *    configuration which does not declare an instance - or a registry the auto-configuration did not build -
 *    does not start.
 * 2. How it is deemed successful: the healthy context starts and every endpoint's instances show the
 *    customizer's effect; the two broken contexts fail with the guard's message naming the key respectively
 *    the unreached instance.
 * 3. Why it matters: a `*ConfigCustomizer` is consulted ONLY while Resilience4j's Boot configuration builds an
 *    `instances.<name>` entry. The shipped file once declared a single TimeLimiter instance while mesh derives
 *    four; under the customizer mechanism three endpoints would then have run on the library default without
 *    a word. [ResilienceConfigContractTest] pins the instance KEYS at build time; only a real context shows
 *    that the keys, the derived aliases, the customizer names and the guard agree - and that the guard fails
 *    the start when they do not.
 *
 * Determinism: a real context, no I/O beyond the classpath, no WireMock - nothing is called, only configured.
 */
class MeshResilienceShippedConfigTest {
    @Test
    fun `should start on the shipped configuration with every mesh endpoint customized`() {
        // Given / When: the context starts on the shipped resilience4j + app.mesh blocks
        shippedContext().run { context ->
            assertThat(context).hasNotFailed()
            val mesh = context.getBean(MeshProperties::class.java)
            val timeLimiters = context.getBean(TimeLimiterRegistry::class.java)
            val circuitBreakers = context.getBean(CircuitBreakerRegistry::class.java)
            val endpoints = mesh.resolveAll().values

            // Then: the file declares the four endpoints the adapters call (a sanity bound on the loop below)
            assertThat(endpoints).hasSize(4)

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

                // ... each CircuitBreaker carries both verdicts on top of the YAML's lap-backend policy
                val circuitBreaker = circuitBreakers.circuitBreaker(endpoint.alias).circuitBreakerConfig
                assertThat(circuitBreaker.ignoreExceptionPredicate.test(InterruptedException()))
                    .`as`("CircuitBreaker of %s must ignore a client-side failure", endpoint.alias)
                    .isTrue()
                assertThat(circuitBreaker.ignoreExceptionPredicate.test(bulkheadRejection())).isTrue()
                assertThat(circuitBreaker.recordExceptionPredicate.test(notFound()))
                    .`as`("CircuitBreaker of %s must not record a benign 4xx", endpoint.alias)
                    .isFalse()
                assertThat(circuitBreaker.failureRateThreshold).`as`("lap-backend policy of %s survives", endpoint.alias).isEqualTo(50f)
            }
        }
    }

    @Test
    fun `should refuse to start when an endpoint's time limiter is not declared`() {
        // What is tested: the silent case the guard exists for, in the real wiring - the shipped file minus one
        //   TimeLimiter instance, as the file once shipped. Resilience4j still creates the instance for the
        //   customizer (on the registry default, not on lap-backend), so nothing else would notice.
        // How success is judged: the context fails, and the failure names the key that is missing - and only it.
        //
        // Given: the shipped properties without one endpoint's time-limiter instance
        val missing = firstAlias()
        val withoutInstance = shippedProperties().filterKeys { !it.startsWith("resilience4j.timelimiter.instances.$missing.") }
        assertThat(withoutInstance).`as`("the filter must have removed something").hasSizeLessThan(shippedProperties().size)

        // When / Then
        shippedContext(withoutInstance).run { context ->
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
        // How success is judged: the context fails naming the time limiter as not reached.
        //
        // Given: the shipped properties, the CB and bulkhead auto-configurations, and for the time limiter a
        // hand-made registry next to hand-filled properties that declare every alias
        val aliases = shippedAliases()
        val handMade = TimeLimiterRegistry.ofDefaults().also { registry -> aliases.forEach { registry.timeLimiter(it) } }
        val declared =
            TimeLimiterConfigurationProperties().also { properties ->
                aliases.forEach { properties.instances[it] = CommonTimeLimiterConfigurationProperties.InstanceProperties() }
            }
        val runner =
            ApplicationContextRunner()
                .withInitializer { context ->
                    context.environment.propertySources.addFirst(MapPropertySource(SHIPPED_PROPERTY_SOURCE, shippedProperties()))
                }.withConfiguration(
                    AutoConfigurations.of(CircuitBreakerAutoConfiguration::class.java, BulkheadAutoConfiguration::class.java),
                ).withBean(TimeLimiterRegistry::class.java, Supplier { handMade })
                .withBean(TimeLimiterConfigurationProperties::class.java, Supplier { declared })
                .withUserConfiguration(MeshTimeoutConfig::class.java)

        // When / Then
        runner.run { context ->
            assertThat(context).hasFailed()
            assertThat(rootCauseOf(context.startupFailure))
                .hasMessageContaining("the timelimiter '${aliases.first()}'")
                .hasMessageContaining("did not reach it")
        }
    }

    /**
     * The context the way the host assembles its resilience side: Resilience4j's three auto-configurations
     * reading the shipped `resilience4j.*` keys, and the module's mesh configuration - the `app.mesh.*`
     * binding, the customizer registrar and the guard. Nothing else - no RestClient, no factory, no pool.
     */
    private fun shippedContext(properties: Map<String, Any> = shippedProperties()): ApplicationContextRunner =
        ApplicationContextRunner()
            .withInitializer { context ->
                context.environment.propertySources.addFirst(MapPropertySource(SHIPPED_PROPERTY_SOURCE, properties))
            }.withConfiguration(
                AutoConfigurations.of(
                    TimeLimiterAutoConfiguration::class.java,
                    CircuitBreakerAutoConfiguration::class.java,
                    BulkheadAutoConfiguration::class.java,
                ),
            ).withUserConfiguration(MeshTimeoutConfig::class.java)

    /**
     * The [SHIPPED_PREFIXES] keys of the shipped `application.yml`, flattened the way Boot would load them.
     * Read from the real file rather than restated here: a copy would keep passing after the shipped file
     * changed, which is the one thing this test exists to catch.
     */
    private fun shippedProperties(): Map<String, Any> =
        YamlPropertySourceLoader()
            .load(SHIPPED, ClassPathResource(SHIPPED))
            .filterIsInstance<EnumerablePropertySource<*>>()
            .flatMap { source ->
                source.propertyNames
                    .filter { name -> SHIPPED_PREFIXES.any(name::startsWith) }
                    .map { it to source.getProperty(it)!! }
            }.toMap()
            .also { loaded ->
                // A prefix that silently matches nothing would start the context on Resilience4j's defaults and
                // turn the assertion into a statement about an empty registry.
                SHIPPED_PREFIXES.forEach { prefix ->
                    assertThat(loaded.keys)
                        .`as`("shipped application.yml must carry '$prefix*' keys")
                        .anyMatch { it.startsWith(prefix) }
                }
            }

    /** The aliases of the shipped endpoints, in file order. */
    private fun shippedAliases(): List<String> =
        ConfigurationFiles
            .bind(SHIPPED, MeshProperties.PREFIX, MeshProperties::class.java)
            .resolveAll()
            .values
            .map { it.alias }

    /** The alias of the first shipped endpoint, the one the negative test strips. */
    private fun firstAlias(): String = shippedAliases().first()

    /** The innermost cause - the guard's own exception, however Spring wrapped it on the way out of refresh. */
    private fun rootCauseOf(failure: Throwable?): Throwable = generateSequence(failure) { it.cause.takeIf { cause -> cause !== it } }.last()

    private fun bulkheadRejection(): BulkheadFullException = BulkheadFullException.createBulkheadFullException(Bulkhead.of("probe", BulkheadConfig.ofDefaults()))

    private fun notFound(): HttpClientErrorException = HttpClientErrorException.create(HttpStatus.NOT_FOUND, "Not Found", HttpHeaders(), ByteArray(0), null)

    private companion object {
        private val SHIPPED_PREFIXES = listOf("resilience4j.", "app.mesh.")
        private const val SHIPPED_PROPERTY_SOURCE = "shipped-application-yml"
    }
}
