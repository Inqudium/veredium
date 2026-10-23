package eu.inqudium.veredium.webclient

import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.resilience.MeshResilienceStartupGuard
import eu.inqudium.veredium.testsupport.ConfigurationFiles
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.springboot.bulkhead.autoconfigure.BulkheadAutoConfiguration
import io.github.resilience4j.springboot.circuitbreaker.autoconfigure.CircuitBreakerAutoConfiguration
import io.github.resilience4j.springboot.timelimiter.autoconfigure.TimeLimiterAutoConfiguration
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.context.annotation.ImportCandidates
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.boot.webclient.autoconfigure.WebClientAutoConfiguration
import org.springframework.core.env.MapPropertySource
import org.springframework.core.io.buffer.DataBufferLimitException
import org.springframework.web.reactive.function.client.WebClientResponseException
import java.util.function.Supplier

/**
 * Boots Resilience4j's own Boot auto-configuration on the `resilience4j.*` and `app.mesh.*` blocks of this
 * module's reference configuration ([ConfigurationFiles.REFERENCE]) with THIS stack's wiring
 * ([VerediumWebClientAutoConfiguration], which imports the core's mesh configuration) - the one proof that the reference
 * blocks, the derived aliases, the reactive stack's verdicts and the core's guard agree. The file is a test
 * resource, not shipped (a library ships no `application.yml`). Compatibility: the class is named "Shipped"
 * all the same, because veredium-core, the ADRs and the blocking twin refer to it by that name.
 *
 * The context must start, and every instance the file declares must show the customizer's effect with the
 * verdicts this module ships: a 404 in the shape `WebClient` throws it is not recorded, the codec's limit
 * exception is ignored even behind its 200-reporting wrapper, the derived budget is on every TimeLimiter. The
 * mechanism - the per-alias customizers, the guard, both directions of failure - is pinned once in the core
 * (`MeshResilienceContextTest`) with a stand-in stack. A file that declared a single TimeLimiter instance while
 * mesh derives four would leave three endpoints on the library default without a word; the guard in this
 * context refuses that.
 *
 * Determinism: a real context, no I/O beyond the classpath, no WireMock - nothing is called, only configured.
 */
class MeshResilienceShippedConfigTest {
    @Test
    fun `should start on the reference configuration with every mesh endpoint customized`() {
        // What is tested: Resilience4j's own Boot auto-configuration plus this stack's wiring, booted on the
        //   reference configuration's resilience4j and app.mesh blocks.
        // Success criteria: the context starts (the core's startup guard did not refuse it); for each of the
        //   four endpoints the TimeLimiter carries the derived budget, and the CircuitBreaker ignores the
        //   codec limit behind its 200 wrapper and does not record a 404 in WebClient's shape.
        // Why it matters: it is the only test where the customizer mechanism meets the real auto-configuration
        //   AND this stack's exception vocabulary on the configuration the module documents - the static
        //   contract (ResilienceConfigContractTest) cannot see whether the customizers actually land.
        //
        // Given / When: the context starts on the reference resilience4j + app.mesh blocks
        referenceContext().run { context ->
            assertThat(context).hasNotFailed()
            assertThat(context).hasSingleBean(MeshResilienceStartupGuard::class.java)
            val endpoints = context.getBean(MeshProperties::class.java).resolveAll().values
            val timeLimiters = context.getBean(TimeLimiterRegistry::class.java)
            val circuitBreakers = context.getBean(CircuitBreakerRegistry::class.java)

            // Then: the file declares the four endpoints (a sanity bound on the loop below) ...
            assertThat(endpoints).hasSize(4)
            endpoints.forEach { endpoint ->
                // ... each TimeLimiter carries the derived budget ...
                assertThat(timeLimiters.timeLimiter(endpoint.alias).timeLimiterConfig.timeoutDuration)
                    .`as`("TimeLimiter budget of %s", endpoint.alias)
                    .isEqualTo(endpoint.timeLimiterTimeout)
                // ... and each CircuitBreaker carries both verdicts in this stack's exception vocabulary
                val config = circuitBreakers.circuitBreaker(endpoint.alias).circuitBreakerConfig
                assertThat(config.ignoreExceptionPredicate.test(wrappedBufferLimit()))
                    .`as`("CircuitBreaker of %s must ignore the codec limit behind its 200 wrapper", endpoint.alias)
                    .isTrue()
                assertThat(config.recordExceptionPredicate.test(WebClientResponseException(404, "Not Found", null, null, null)))
                    .`as`("CircuitBreaker of %s must not record a benign 4xx", endpoint.alias)
                    .isFalse()
            }
        }
    }

    @Test
    fun `should be registered as an auto-configuration, so the dependency alone wires the module`() {
        // What is tested: the module's AutoConfiguration.imports file.
        // Success criteria: Boot's own loader lists VerediumWebClientAutoConfiguration among the auto-configurations.
        // Why it matters: a host adds the dependency and nothing else; a missing or misspelt entry would leave it
        //   without a factory and fail at its first injection point, with nothing pointing at the file.
        //
        // Given / When
        val candidates = ImportCandidates.load(AutoConfiguration::class.java, javaClass.classLoader)

        // Then
        assertThat(candidates.candidates).contains(VerediumWebClientAutoConfiguration::class.java.name)
    }

    @Test
    fun `should back off for a host's own decorator`() {
        // What is tested: the auto-configuration in a context where the host declares its own
        //   ReactiveResilienceDecorator.
        // Success criteria: the context holds exactly the host's decorator, and the module's factory next to it.
        // Why it matters: a host that wraps or replaces the decorator must not end up with a second one next to its
        //   own, which the factory could then be wired with instead.
        //
        // Given
        val hostDecorator = ReactiveResilienceDecorator(CircuitBreakerRegistry.ofDefaults(), TimeLimiterRegistry.ofDefaults(), BulkheadRegistry.ofDefaults())

        // When
        referenceContext()
            .withBean("hostDecorator", ReactiveResilienceDecorator::class.java, Supplier { hostDecorator })
            .run { context ->
                // Then
                assertThat(context).hasNotFailed()
                assertThat(context).getBean(ReactiveResilienceDecorator::class.java).isSameAs(hostDecorator)
                assertThat(context).hasSingleBean(ResilientWebClientFactory::class.java)
            }
    }

    /**
     * Resilience4j's three auto-configurations on the reference keys, Boot's Jackson and `WebClient.Builder`, plus
     * [VerediumWebClientAutoConfiguration] - the core's mesh binding and [WebClientMeshRegistrar] with this stack's
     * verdicts, the decorator and the factory.
     */
    private fun referenceContext(): ApplicationContextRunner =
        ApplicationContextRunner()
            .withInitializer { context ->
                context.environment.propertySources.addFirst(
                    MapPropertySource("reference-application-yml", ConfigurationFiles.referenceProperties("resilience4j.", "app.mesh.")),
                )
            }.withConfiguration(
                AutoConfigurations.of(
                    TimeLimiterAutoConfiguration::class.java,
                    CircuitBreakerAutoConfiguration::class.java,
                    BulkheadAutoConfiguration::class.java,
                    JacksonAutoConfiguration::class.java,
                    WebClientAutoConfiguration::class.java,
                    VerediumWebClientAutoConfiguration::class.java,
                ),
            )

    /** The codec failure in the exact shape the client produces it: a 200-reporting response exception with the limit beneath. */
    private fun wrappedBufferLimit(): Throwable =
        WebClientResponseException(200, "OK", null, null, null)
            .initCause(DataBufferLimitException("Exceeded limit on max bytes to buffer : 8192"))
}
