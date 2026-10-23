package eu.inqudium.veredium.restclient

import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.resilience.BlockingResilienceDecorator
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
import org.springframework.boot.restclient.autoconfigure.RestClientAutoConfiguration
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.core.env.MapPropertySource
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.web.client.HttpClientErrorException
import java.util.function.Supplier

/**
 * Boots Resilience4j's own Boot auto-configuration on the `resilience4j.*` and `app.mesh.*` blocks of this
 * module's REFERENCE configuration (`src/test/resources/application.yml`, [ConfigurationFiles.REFERENCE] - a
 * test resource no consumer receives) with THIS stack's wiring ([VerediumRestClientAutoConfiguration], which
 * imports the core's mesh configuration) - the one proof that the documented blocks, the derived aliases, the blocking
 * stack's verdicts and the core's guard agree. (The "Shipped" in the name is the one the core's tests and
 * ADR-0001 refer to it by, in both stacks.)
 *
 * The test checks that the context starts and that every instance the file declares shows the customizer's effect
 * with the verdicts this module ships: a 404 in the shape `RestClient` throws it is not recorded, this module's
 * buffer-limit exception is ignored, the derived budget is on every TimeLimiter. The mechanism - customizer beans,
 * the guard, both directions of failure - is pinned once in the core (`MeshResilienceContextTest`) with a stand-in
 * stack. Only a context with the real [RestClientVerdicts] shows that the probe the guard applies is one this
 * stack's policy recognises; a stack whose probe did not match its own reader would fail every start.
 *
 * Determinism: a real context, no I/O beyond the classpath, no WireMock - nothing is called, only configured.
 */
class MeshResilienceShippedConfigTest {
    @Test
    fun `should start on the reference configuration with every mesh endpoint customized`() {
        // What is tested: a real Resilience4j auto-configuration on the reference file's resilience4j + app.mesh
        //   blocks, with this module's verdicts and the core's customizers and guard.
        // Success criteria: the context starts (the guard did not refuse it), it declares the four endpoints,
        //   and every endpoint's TimeLimiter carries its derived budget and its CircuitBreaker ignores this
        //   module's buffer-limit failure and does not record a benign 404.
        // Why it matters: the core proves the mechanism with a stand-in stack; only this context shows that the
        //   blocking stack's probe and exception shapes are the ones its own policy recognises - a mismatch would
        //   fail every host's start - and that the configuration the docs recommend actually boots.
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
                assertThat(config.ignoreExceptionPredicate.test(ResponseBodyLimitExceededException(endpoint.alias, 8192, -1)))
                    .`as`("CircuitBreaker of %s must ignore this module's buffer-limit failure", endpoint.alias)
                    .isTrue()
                assertThat(config.recordExceptionPredicate.test(notFound()))
                    .`as`("CircuitBreaker of %s must not record a benign 4xx", endpoint.alias)
                    .isFalse()
            }
        }
    }

    @Test
    fun `should be registered as an auto-configuration, so the dependency alone wires the module`() {
        // What is tested: the module's AutoConfiguration.imports file.
        // Success criteria: Boot's own loader lists VerediumRestClientAutoConfiguration among the auto-configurations.
        // Why it matters: a host adds the dependency and nothing else; a missing or misspelt entry would leave it
        //   without a factory and fail at its first injection point, with nothing pointing at the file.
        //
        // Given / When
        val candidates = ImportCandidates.load(AutoConfiguration::class.java, javaClass.classLoader)

        // Then
        assertThat(candidates.candidates).contains(VerediumRestClientAutoConfiguration::class.java.name)
    }

    @Test
    fun `should back off for a host's own decorator`() {
        // What is tested: the auto-configuration in a context where the host declares its own
        //   BlockingResilienceDecorator.
        // Success criteria: the context holds exactly the host's decorator, and the module's factory next to it.
        // Why it matters: a host that tunes the decorator (its worker name, its registries) must not end up with a
        //   second one next to its own - two decorators would own two executors and two worker namings.
        //
        // Given
        val hostDecorator =
            BlockingResilienceDecorator(CircuitBreakerRegistry.ofDefaults(), TimeLimiterRegistry.ofDefaults(), BulkheadRegistry.ofDefaults(), workerNamePrefix = "host-call-")

        // When
        referenceContext()
            .withBean("hostDecorator", BlockingResilienceDecorator::class.java, Supplier { hostDecorator })
            .run { context ->
                // Then
                assertThat(context).hasNotFailed()
                assertThat(context).getBean(BlockingResilienceDecorator::class.java).isSameAs(hostDecorator)
                assertThat(context).hasSingleBean(ResilientRestClientFactory::class.java)
            }
    }

    /**
     * Resilience4j's three auto-configurations on the reference keys, Boot's Jackson and `RestClient.Builder`, plus
     * [VerediumRestClientAutoConfiguration] - the core's mesh configuration and [RestClientMeshRegistrar], which
     * registers the customizers and the guard with this stack's verdicts, the decorator and the factory.
     */
    private fun referenceContext(): ApplicationContextRunner =
        ApplicationContextRunner()
            .withInitializer { context ->
                context.environment.propertySources.addFirst(MapPropertySource("reference-application-yml", ConfigurationFiles.referenceProperties("resilience4j.", "app.mesh.")))
            }.withConfiguration(
                AutoConfigurations.of(
                    TimeLimiterAutoConfiguration::class.java,
                    CircuitBreakerAutoConfiguration::class.java,
                    BulkheadAutoConfiguration::class.java,
                    JacksonAutoConfiguration::class.java,
                    RestClientAutoConfiguration::class.java,
                    VerediumRestClientAutoConfiguration::class.java,
                ),
            )

    private fun notFound(): HttpClientErrorException = HttpClientErrorException.create(HttpStatus.NOT_FOUND, "Not Found", HttpHeaders(), ByteArray(0), null)
}
