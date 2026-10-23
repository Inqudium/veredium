package eu.inqudium.veredium.webclient

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.okJson
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import eu.inqudium.veredium.core.mesh.MeshProperties
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micrometer.observation.ObservationRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.boot.webclient.WebClientCustomizer
import org.springframework.boot.webclient.autoconfigure.WebClientAutoConfiguration
import org.springframework.boot.webclient.autoconfigure.WebClientObservationAutoConfiguration
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.json.JsonMapper
import java.time.Duration
import java.util.function.Supplier

/**
 * Wiring test for [ResilientWebClientFactory]'s base builder: it must be Boot's auto-configured
 * `WebClient.Builder` bean, not a builder of its own.
 *
 * Whatever the host puts on Boot's builder - a `WebClientCustomizer` bean of its own, and the
 * `ObservationRegistry` Boot's `ObservationWebClientCustomizer` places there - must reach the clients this
 * factory produces; the module toggle `http-observation-enabled=false` must still switch the observation of
 * THESE clients off although Boot wired the registry; and a context without the builder bean must fail to start
 * rather than run silently on a bare builder. A factory built on `WebClient.builder()` would have to reimplement
 * the one piece of Boot's wiring it needs (the registry), and every other customizer - a host correlation
 * header, a custom observation convention, Boot's codec settings - would reach every WebClient of the host
 * except these. And a Kotlin default on the parameter would let Spring treat the bean as optional, so a host
 * that excluded `WebClientAutoConfiguration` would lose its CLIENT span without a log line. Both are invisible
 * at runtime; only a context test pins them.
 *
 * The contexts are driven with an [ApplicationContextRunner] - a real Spring context, no mocks - and the
 * backend is a local WireMock on a dynamic port, as in `ResilientWebClientIntegrationTest`. No inner
 * `@Configuration` class: every bean is registered on the runner, so nothing here can leak into another
 * test's component scan.
 */
class ResilientWebClientFactoryBuilderWiringTest {
    companion object {
        private const val BACKEND = "probe"
        private const val PROBE_PATH = "/probe"
        private const val PROBE_HEADER = "X-Wiring-Probe"
        private const val PROBE_VALUE = "from-host-customizer"
        private const val HTTP_CLIENT_OBSERVATION = "http.client.requests"

        private lateinit var wireMock: WireMockServer

        @JvmStatic
        @BeforeAll
        fun startWireMock() {
            wireMock = WireMockServer(wireMockConfig().dynamicPort()).apply { start() }
            wireMock.stubFor(get(urlEqualTo(PROBE_PATH)).willReturn(okJson("""{"message":"hi"}""")))
        }

        @JvmStatic
        @AfterAll
        fun stopWireMock() {
            wireMock.stop()
        }
    }

    @Nested
    inner class `Host customizers` {
        @Test
        fun `should send the header a WebClientCustomizer bean of the host puts on Boot's builder`() {
            // What is tested: that a host's own WebClientCustomizer reaches the clients of this factory.
            // Success criteria: the marker header the customizer sets as a default header arrives at WireMock
            //   on a request made through a client of the context's factory.
            // Why it matters: a factory that did not clone Boot's builder would let every customizer of the host -
            //   a correlation header, a custom observation convention, Boot's codec settings - reach every
            //   WebClient of the host except these, and nothing at runtime would say so.
            //
            // Given: Boot's auto-configuration plus a host WebClientCustomizer bean that sets a marker header
            wireMock.resetRequests()
            val meterRegistry = SimpleMeterRegistry()
            factoryContext(observationEnabled = true, meterRegistry = meterRegistry)
                .withBootsWebClientAutoConfiguration()
                .withBean(
                    WebClientCustomizer::class.java,
                    Supplier { WebClientCustomizer { builder -> builder.defaultHeader(PROBE_HEADER, PROBE_VALUE) } },
                ).run { context ->
                    // When: a client of this factory makes a request
                    probeThrough(context.getBean(ResilientWebClientFactory::class.java))

                    // Then: the backend saw the marker header - the customizer reached the factory's client
                    wireMock.verify(getRequestedFor(urlEqualTo(PROBE_PATH)).withHeader(PROBE_HEADER, equalTo(PROBE_VALUE)))
                }
        }
    }

    @Nested
    inner class `Observation wired by Boot` {
        @Test
        fun `should record an http client observation per request without the factory wiring the registry`() {
            // What is tested: that the observation registry Boot's ObservationWebClientCustomizer puts on the
            //   builder survives into the factory's clients when the module toggle is on.
            // Success criteria: one request produces an `http.client.requests` timer with count 1 in the meter
            //   registry behind the context's ObservationRegistry.
            // Why it matters: the factory does not wire the registry itself; if Boot's did not arrive, the client
            //   span would vanish without a log line.
            //
            // Given: the toggle ON, an ObservationRegistry bean, and Boot's ObservationWebClientCustomizer
            val meterRegistry = SimpleMeterRegistry()
            factoryContext(observationEnabled = true, meterRegistry = meterRegistry)
                .withBootsWebClientAutoConfiguration()
                .run { context ->
                    // When: one request
                    probeThrough(context.getBean(ResilientWebClientFactory::class.java))

                    // Then: the observation landed as a meter - Boot placed the registry on the builder,
                    // the factory only cloned it
                    val timer = meterRegistry.find(HTTP_CLIENT_OBSERVATION).timer()
                    assertThat(timer).isNotNull()
                    assertThat(timer!!.count()).isEqualTo(1L)
                }
        }

        @Test
        fun `should record no http client observation when the module toggle is off although Boot wired the registry`() {
            // What is tested: the toggle's OFF branch against a builder Boot DID wire.
            // Success criteria: after one request the meter registry holds no `http.client.requests` timer and no
            //   meter at all.
            // Why it matters: the toggle is the module's only way to keep its own clients unobserved while the
            //   host observes everything else; a factory that kept Boot's registry regardless would pass the
            //   test above and ignore the operator's choice here.
            //
            // Given: the toggle OFF, the same registry bean and the same Boot customizer as above
            val meterRegistry = SimpleMeterRegistry()
            factoryContext(observationEnabled = false, meterRegistry = meterRegistry)
                .withBootsWebClientAutoConfiguration()
                .run { context ->
                    // When: one request
                    probeThrough(context.getBean(ResilientWebClientFactory::class.java))

                    // Then: no meter - the factory replaced Boot's registry with NOOP for its own clients
                    assertThat(meterRegistry.find(HTTP_CLIENT_OBSERVATION).timer()).isNull()
                    assertThat(meterRegistry.meters).isEmpty()
                }
        }
    }

    @Nested
    inner class `Missing builder bean` {
        @Test
        fun `should fail the context start naming WebClient Builder when no builder bean exists`() {
            // What is tested: a context without Boot's WebClient auto-configuration and without a builder bean of
            //   its own.
            // Success criteria: the context fails to start, and the failure names both `WebClient$Builder` and
            //   the bean that required it, `resilientWebClientFactory`.
            // Why it matters: a Kotlin default on that parameter would let Spring treat the bean as optional and
            //   construct the factory on a bare builder - no customizer, no observation, no error. The required
            //   parameter turns that silent degradation into a failed start that says what is missing.
            //
            // Given: the factory's collaborators but neither Boot's auto-configuration nor a builder bean
            factoryContext(observationEnabled = true, meterRegistry = SimpleMeterRegistry())
                // When: the context starts
                .run { context ->
                    // Then: it fails, and the failure names the missing bean type AND the bean that needed it - a
                    // required parameter, so Spring reports the absence instead of silently constructing on a bare
                    // builder. Spring prints the JVM binary name of the nested interface, WebClient$Builder, and
                    // the bean by its decapitalised default name.
                    assertThat(context).hasFailed()
                    assertThat(context.startupFailure)
                        .hasMessageContainingAll("WebClient\$Builder", "'resilientWebClientFactory'")
                }
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /**
     * The factory's collaborators, registered the way a host's context would hold them - minus everything the
     * test does not exercise. Boot's auto-configuration is NOT part of this: each case adds what it needs.
     */
    private fun factoryContext(
        observationEnabled: Boolean,
        meterRegistry: SimpleMeterRegistry,
    ): ApplicationContextRunner =
        ApplicationContextRunner()
            .withBean(
                WebClientProperties::class.java,
                Supplier {
                    WebClientProperties(
                        observability = WebClientProperties.Observability(httpObservationEnabled = observationEnabled),
                    )
                },
            ).withBean(
                MeshProperties::class.java,
                Supplier {
                    MeshProperties(
                        defaults = MeshProperties.Defaults(individualRequestTimeoutMs = 1000),
                        endpoints =
                            listOf(
                                MeshProperties.EndpointConfig(
                                    triplet = "/endpoint/localhost/GET/$BACKEND",
                                    alias = BACKEND,
                                    baseUrl = wireMock.baseUrl(),
                                ),
                            ),
                    )
                },
            ).withBean(
                ReactiveResilienceDecorator::class.java,
                Supplier {
                    ReactiveResilienceDecorator(
                        CircuitBreakerRegistry.ofDefaults(),
                        // Generous on purpose: the wiring is the subject, the call must never time out.
                        TimeLimiterRegistry.of(TimeLimiterConfig.custom().timeoutDuration(Duration.ofSeconds(30)).build()),
                        BulkheadRegistry.ofDefaults(),
                    )
                },
            ).withBean(ObjectMapper::class.java, Supplier { JsonMapper.builder().build() })
            // A recording registry: every observation lands as a meter in the SimpleMeterRegistry, which is what
            // the host's actuator ObservationAutoConfiguration does with its MeterRegistry.
            .withBean(
                ObservationRegistry::class.java,
                Supplier {
                    ObservationRegistry.create().apply {
                        observationConfig().observationHandler(DefaultMeterObservationHandler(meterRegistry))
                    }
                },
            ).withBean(ResilientWebClientFactory::class.java)

    /** Boot's WebClient auto-configuration, which provides the builder bean and the observation customizer. */
    private fun ApplicationContextRunner.withBootsWebClientAutoConfiguration(): ApplicationContextRunner =
        withConfiguration(
            AutoConfigurations.of(
                WebClientAutoConfiguration::class.java,
                WebClientObservationAutoConfiguration::class.java,
            ),
        )

    /** One GET against the probe endpoint through a client of the context's factory. */
    private fun probeThrough(factory: ResilientWebClientFactory) {
        factory.create(BACKEND).get(Map::class.java) { it.uri(PROBE_PATH) }.block()
    }
}
