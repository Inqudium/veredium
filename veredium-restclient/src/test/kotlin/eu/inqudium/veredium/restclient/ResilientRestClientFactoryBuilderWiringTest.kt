package eu.inqudium.veredium.restclient

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.okJson
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import eu.inqudium.legatium.restclient.logging.ClientLoggingAutoConfiguration
import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.resilience.BlockingResilienceDecorator
import eu.inqudium.veredium.testsupport.Greeting
import eu.inqudium.veredium.testsupport.capturing
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.springboot.bulkhead.autoconfigure.BulkheadAutoConfiguration
import io.github.resilience4j.springboot.circuitbreaker.autoconfigure.CircuitBreakerAutoConfiguration
import io.github.resilience4j.springboot.timelimiter.autoconfigure.TimeLimiterAutoConfiguration
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
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration
import org.springframework.boot.restclient.RestClientCustomizer
import org.springframework.boot.restclient.autoconfigure.RestClientAutoConfiguration
import org.springframework.boot.restclient.autoconfigure.RestClientObservationAutoConfiguration
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.core.env.MapPropertySource
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.json.JsonMapper
import java.time.Duration
import java.util.function.Supplier

/**
 * Wiring test for [ResilientRestClientFactory]'s base builder: it must be Boot's auto-configured
 * `RestClient.Builder` bean, not a builder of its own.
 *
 * The tests pin that whatever the host puts on Boot's builder - a `RestClientCustomizer` bean of its own, and the
 * `ObservationRegistry` Boot's `ObservationRestClientCustomizer` places there - reaches the clients this factory
 * produces (a marker header set by a test customizer arrives at a local WireMock backend, an
 * `http.client.requests` meter appears behind the context's `ObservationRegistry`); that the module toggle
 * `http-observation-enabled=false` still switches the observation of THESE clients off although Boot wired the
 * registry (no such meter); and that a context without the builder bean fails to start, naming
 * `RestClient.Builder`, rather than running silently on a bare builder.
 *
 * A factory built on `RestClient.builder()` would reimplement the one piece of Boot's wiring it needed and lose
 * every other customizer - a host correlation header, a custom observation convention, Boot's message converters.
 * And a Kotlin default on the parameter would let Spring treat the bean as optional, so a host that excluded
 * `RestClientAutoConfiguration` would lose its CLIENT span without a log line. Both are invisible at runtime;
 * only a context test pins them.
 *
 * The contexts are driven with an [ApplicationContextRunner] - a real Spring context, no mocks - and the
 * backend is a local WireMock on a dynamic port, as in `ResilientRestClientIntegrationTest`. No inner
 * `@Configuration` class: every bean is registered on the runner, so nothing here can leak into another
 * test's component scan. The factory is registered on the runner too, except in `The module's auto-configuration`,
 * whose contexts load [VerediumRestClientAutoConfiguration] - the bean method a host runs - and make a call through
 * the factory it builds.
 */
class ResilientRestClientFactoryBuilderWiringTest {
    companion object {
        private const val BACKEND = "probe"
        private const val PROBE_PATH = "/probe"
        private const val PROBE_HEADER = "X-Wiring-Probe"
        private const val PROBE_VALUE = "from-host-customizer"
        private const val HTTP_CLIENT_OBSERVATION = "http.client.requests"

        /** legatium's exchange logger, as the reference application.yml names it. */
        private const val EXCHANGE_LOGGER = "adapter-http-exchange"

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
        fun `should send the header a RestClientCustomizer bean of the host puts on Boot's builder`() {
            // What is tested: that the factory clones Boot's builder bean, with every host customizer already
            //   applied, instead of starting from a builder of its own.
            // Success criteria: the marker header a host RestClientCustomizer bean sets arrives at WireMock on a
            //   request made through a client of the factory.
            // Why it matters: a host puts its correlation headers, converters and conventions on Boot's builder;
            //   a factory that bypassed it would drop all of them without an error.
            //
            // Given: Boot's auto-configuration plus a host RestClientCustomizer bean that sets a marker header
            wireMock.resetRequests()
            val meterRegistry = SimpleMeterRegistry()
            factoryContext(observationEnabled = true, meterRegistry = meterRegistry)
                .withBootsRestClientAutoConfiguration()
                .withBean(
                    RestClientCustomizer::class.java,
                    Supplier { RestClientCustomizer { builder -> builder.defaultHeader(PROBE_HEADER, PROBE_VALUE) } },
                ).run { context ->
                    // When: a client of this factory makes a request
                    probeThrough(context.getBean(ResilientRestClientFactory::class.java))

                    // Then: the backend saw the marker header - the customizer reached the factory's client
                    wireMock.verify(getRequestedFor(urlEqualTo(PROBE_PATH)).withHeader(PROBE_HEADER, equalTo(PROBE_VALUE)))
                }
        }
    }

    @Nested
    inner class `Observation wired by Boot` {
        @Test
        fun `should record an http client observation per request without the factory wiring the registry`() {
            // What is tested: the observation with the toggle ON comes from Boot's ObservationRestClientCustomizer
            //   on the builder bean - the factory only keeps it.
            // Success criteria: one request produces exactly one `http.client.requests` timer in the registry
            //   behind the context's ObservationRegistry.
            // Why it matters: the client span and the request timer of every call depend on this route; a
            //   factory that wired a registry itself would diverge from the host's own observation conventions.
            //
            // Given: the toggle ON, an ObservationRegistry bean, and Boot's ObservationRestClientCustomizer
            val meterRegistry = SimpleMeterRegistry()
            factoryContext(observationEnabled = true, meterRegistry = meterRegistry)
                .withBootsRestClientAutoConfiguration()
                .run { context ->
                    // When: one request
                    probeThrough(context.getBean(ResilientRestClientFactory::class.java))

                    // Then: the observation landed as a meter - Boot placed the registry on the builder,
                    // the factory only cloned it
                    val timer = meterRegistry.find(HTTP_CLIENT_OBSERVATION).timer()
                    assertThat(timer).isNotNull()
                    assertThat(timer!!.count()).isEqualTo(1L)
                }
        }

        @Test
        fun `should record no http client observation when the module toggle is off although Boot wired the registry`() {
            // What is tested: the module toggle OFF overriding what Boot put on the builder - for this factory's
            //   clients only.
            // Success criteria: no `http.client.requests` timer after a request, with the same registry and the
            //   same Boot customizer as the case above.
            // Why it matters: the toggle is the operator's off switch for these clients' spans; one that lost to
            //   Boot's wiring would switch nothing off.
            //
            // Given: the toggle OFF, the same registry bean and the same Boot customizer as above
            val meterRegistry = SimpleMeterRegistry()
            factoryContext(observationEnabled = false, meterRegistry = meterRegistry)
                .withBootsRestClientAutoConfiguration()
                .run { context ->
                    // When: one request
                    probeThrough(context.getBean(ResilientRestClientFactory::class.java))

                    // Then: no meter - the factory replaced Boot's registry with NOOP for its own clients
                    assertThat(meterRegistry.find(HTTP_CLIENT_OBSERVATION).timer()).isNull()
                }
        }
    }

    @Nested
    inner class `Missing builder bean` {
        @Test
        fun `should fail the context start naming RestClient Builder when no builder bean exists`() {
            // What is tested: the builder bean as a REQUIRED dependency of the factory.
            // Success criteria: the context fails to start, and the failure names `RestClient$Builder` and the
            //   factory bean that needed it.
            // Why it matters: a Kotlin default on the parameter would make Spring treat the bean as optional, and
            //   a host without RestClientAutoConfiguration would silently run on a bare builder - no customizers,
            //   no client span, no log line.
            //
            // Given: the factory's collaborators but neither Boot's auto-configuration nor a builder bean
            factoryContext(observationEnabled = true, meterRegistry = SimpleMeterRegistry())
                // When: the context starts
                .run { context ->
                    // Then: it fails, and the failure names the missing bean type AND the bean that needed it - a
                    // required parameter, so Spring reports the absence instead of silently constructing on a bare
                    // builder. Spring prints the JVM binary name of the nested interface, RestClient$Builder, and
                    // the bean by its decapitalised default name.
                    assertThat(context).hasFailed()
                    assertThat(context.startupFailure)
                        .hasMessageContainingAll("RestClient\$Builder", "'resilientRestClientFactory'")
                }
        }
    }

    @Nested
    inner class `The module's auto-configuration` {
        @Test
        fun `should send a call through the factory the auto-configuration builds, with the host's builder, registry and exchange log`() {
            // What is tested: the factory as a host gets it - built by VerediumRestClientAutoConfiguration's bean
            //   method, next to Boot's RestClient, Jackson and Resilience4j auto-configurations, legatium's
            //   auto-configuration, a host RestClientCustomizer bean and a host MeterRegistry - driven by one call.
            // Success criteria: the call returns the backend's value; the host customizer's marker header reached
            //   the backend; the host registry carries the factory's `restclient.response.body.size` summary with
            //   the one call's sample; legatium wrote one INFO / success exchange event naming the backend.
            // Why it matters: every other context that makes a call builds the factory itself (withBean). The
            //   bean method is what hosts run, and what it hands the factory - Boot's builder bean rather than a
            //   builder of its own, the host's registry rather than none - is invisible at runtime: a wrong
            //   argument there silently costs every client the host's customizers, the exchange event and the
            //   body-size meter, while each case above stays green.
            //
            // Given: the module's auto-configuration as a host loads it, plus a host customizer and registry
            wireMock.resetRequests()
            val meterRegistry = SimpleMeterRegistry()
            val exchangeLogger = LoggerFactory.getLogger(EXCHANGE_LOGGER) as Logger
            autoConfiguredContext()
                .withBean(SimpleMeterRegistry::class.java, Supplier { meterRegistry })
                .withBean(
                    RestClientCustomizer::class.java,
                    Supplier { RestClientCustomizer { builder -> builder.defaultHeader(PROBE_HEADER, PROBE_VALUE) } },
                ).run { context ->
                    assertThat(context).hasNotFailed()
                    capturing(exchangeLogger, level = Level.INFO) { exchanges ->
                        // When: one call through a client of the context's factory
                        val answer = context.getBean(ResilientRestClientFactory::class.java).create(BACKEND).get(Greeting::class.java) { it.uri(PROBE_PATH) }

                        // Then: the answer, the host's header on the wire, the host's meter, and the exchange event
                        assertThat(answer.message).isEqualTo("hi")
                        wireMock.verify(getRequestedFor(urlEqualTo(PROBE_PATH)).withHeader(PROBE_HEADER, equalTo(PROBE_VALUE)))
                        val bodySize = meterRegistry.find(RestClientMetrics.RESPONSE_BODY_SIZE).summary()
                        assertThat(bodySize).`as`("the body-size summary in the host's registry").isNotNull()
                        assertThat(bodySize!!.count()).isEqualTo(1L)
                        val event = exchanges.awaitEvents().single()
                        assertThat(event.level).isEqualTo(Level.INFO)
                        assertThat(event.formattedMessage).contains("$BACKEND -> 200")
                        assertThat(event.keyValuePairs.orEmpty()).anyMatch { it.key == "adapter_name" && it.value == BACKEND }
                    }
                }
        }

        @Test
        fun `should fail the start naming RestClient Builder when the host has no builder bean`() {
            // What is tested: the module's auto-configuration without Boot's RestClient auto-configuration, so
            //   without a `RestClient.Builder` bean.
            // Success criteria: the context fails to start, naming `RestClient$Builder` and the factory bean
            //   'resilientRestClientFactory'.
            // Why it matters: the case above shows the builder bean is used when there is one; this one, that the
            //   bean method does not fall back to a bare builder when there is none - a host that excluded Boot's
            //   auto-configuration would otherwise run without its customizers and the exchange log, unannounced.
            //
            // Given: everything the case above loads but Boot's RestClient auto-configuration
            ApplicationContextRunner()
                .withInitializer { context -> context.environment.propertySources.addFirst(MapPropertySource("probe-mesh", meshKeys())) }
                .withConfiguration(
                    AutoConfigurations.of(
                        TimeLimiterAutoConfiguration::class.java,
                        CircuitBreakerAutoConfiguration::class.java,
                        BulkheadAutoConfiguration::class.java,
                        JacksonAutoConfiguration::class.java,
                        VerediumRestClientAutoConfiguration::class.java,
                    ),
                )
                // When: the context starts
                .run { context ->
                    // Then
                    assertThat(context).hasFailed()
                    assertThat(context.startupFailure).hasMessageContainingAll("RestClient\$Builder", "'resilientRestClientFactory'")
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
    ): ApplicationContextRunner {
        val circuitBreakers = CircuitBreakerRegistry.ofDefaults()
        // Generous on purpose: the wiring is the subject, the call must never time out.
        val timeLimiters = TimeLimiterRegistry.of(TimeLimiterConfig.custom().timeoutDuration(Duration.ofSeconds(30)).build())
        val bulkheads = BulkheadRegistry.ofDefaults()
        return ApplicationContextRunner()
            .withBean(
                RestClientProperties::class.java,
                Supplier {
                    RestClientProperties(
                        observability = RestClientProperties.Observability(httpObservationEnabled = observationEnabled),
                    )
                },
            ).withBean(
                MeshProperties::class.java,
                Supplier {
                    MeshProperties(
                        defaults =
                            MeshProperties.Defaults(
                                individualRequestTimeoutMs = 1000,
                                transport = MeshProperties.Transport(sidecarBaseUrl = wireMock.baseUrl()),
                            ),
                        endpoints = listOf(MeshProperties.EndpointConfig(triplet = "/endpoint/localhost/GET/$BACKEND", alias = BACKEND)),
                    )
                },
            ).withBean(CircuitBreakerRegistry::class.java, Supplier { circuitBreakers })
            .withBean(TimeLimiterRegistry::class.java, Supplier { timeLimiters })
            .withBean(BulkheadRegistry::class.java, Supplier { bulkheads })
            // Built here rather than through VerediumRestClientAutoConfiguration, which would also import the core's mesh
            // wiring - whose guard needs Resilience4j's auto-configuration this hand-assembled context leaves out.
            .withBean(
                BlockingResilienceDecorator::class.java,
                Supplier { BlockingResilienceDecorator(circuitBreakers, timeLimiters, bulkheads, workerNamePrefix = VerediumRestClientAutoConfiguration.WORKER_NAME_PREFIX) },
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
            ).withBean(ResilientRestClientFactory::class.java)
    }

    /**
     * A host's context as the module's auto-configuration meets it: Resilience4j's three Boot auto-configurations,
     * Boot's Jackson and RestClient auto-configurations, legatium's, and [VerediumRestClientAutoConfiguration] - which
     * binds `app.mesh` and `app.restclient`, registers the mesh customizers and the startup guard, and builds the
     * decorator and the factory. The mesh is the one probe endpoint against WireMock, declared on all three
     * Resilience4j layers so the guard lets the context start.
     */
    private fun autoConfiguredContext(): ApplicationContextRunner =
        ApplicationContextRunner()
            .withInitializer { context -> context.environment.propertySources.addFirst(MapPropertySource("probe-mesh", meshKeys())) }
            .withConfiguration(
                AutoConfigurations.of(
                    TimeLimiterAutoConfiguration::class.java,
                    CircuitBreakerAutoConfiguration::class.java,
                    BulkheadAutoConfiguration::class.java,
                    JacksonAutoConfiguration::class.java,
                    RestClientAutoConfiguration::class.java,
                    ClientLoggingAutoConfiguration::class.java,
                    VerediumRestClientAutoConfiguration::class.java,
                ),
            )

    /** `app.mesh` with the probe endpoint against WireMock, and its Resilience4j instances on all three layers. */
    private fun meshKeys(): Map<String, Any> =
        mapOf(
            "app.mesh.defaults.individual-request-timeout-ms" to "1000",
            "app.mesh.defaults.transport.sidecar-base-url" to wireMock.baseUrl(),
            "app.mesh.endpoints[0].triplet" to "/endpoint/localhost/GET/$BACKEND",
            "app.mesh.endpoints[0].alias" to BACKEND,
            "resilience4j.timelimiter.instances.$BACKEND.cancel-running-future" to "true",
            "resilience4j.circuitbreaker.instances.$BACKEND.sliding-window-size" to "100",
            "resilience4j.bulkhead.instances.$BACKEND.max-concurrent-calls" to "25",
        )

    /** Boot's RestClient auto-configuration, which provides the builder bean and the observation customizer. */
    private fun ApplicationContextRunner.withBootsRestClientAutoConfiguration(): ApplicationContextRunner =
        withConfiguration(
            AutoConfigurations.of(
                RestClientAutoConfiguration::class.java,
                RestClientObservationAutoConfiguration::class.java,
            ),
        )

    /** One GET against the probe endpoint through a client of the context's factory. */
    private fun probeThrough(factory: ResilientRestClientFactory) {
        factory.create(BACKEND).get(Map::class.java) { it.uri(PROBE_PATH) }
    }
}
