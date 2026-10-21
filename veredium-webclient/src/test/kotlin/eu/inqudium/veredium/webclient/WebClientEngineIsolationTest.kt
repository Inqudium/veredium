package eu.inqudium.veredium.webclient

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.matching
import com.github.tomakehurst.wiremock.client.WireMock.okJson
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import eu.inqudium.veredium.core.Veredium
import eu.inqudium.veredium.core.mesh.MeshProperties
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.EnumSource
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.http.client.autoconfigure.HttpClientAutoConfiguration
import org.springframework.boot.http.client.autoconfigure.reactive.ReactiveHttpClientAutoConfiguration
import org.springframework.boot.http.client.reactive.ClientHttpConnectorBuilder
import org.springframework.boot.http.client.reactive.ReactorClientHttpConnectorBuilder
import org.springframework.boot.test.context.assertj.AssertableApplicationContext
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.boot.webclient.autoconfigure.WebClientAutoConfiguration
import org.springframework.http.client.reactive.ClientHttpConnector
import org.springframework.http.client.reactive.JdkClientHttpConnector
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.client.WebClient
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.json.JsonMapper
import java.time.Duration
import java.util.function.Supplier

/**
 * The isolation guarantee of `app.webclient.engine`, in a real Spring context with Boot's own HTTP-client
 * auto-configuration: the engine this module selects for its backends must not reach any other WebClient of
 * the host, and Boot's global connector switch must not reach this module's backends.
 *
 * How "which engine served a request" is observed: on the HOST's side by the `User-Agent` an engine sends when
 * the caller sets none - `ReactorNetty/...`, `Jetty/...`, `Apache-HttpAsyncClient/...`, `Java-http-client/...`
 * - read off the request WireMock received. That is evidence from the wire, independent of how Boot wires its
 * connector. On THIS module's side, where every request carries Veredium's own User-Agent, by the connector
 * the factory built the backend's client with.
 *
 * The context holds Boot's `HttpClientAutoConfiguration`, `ReactiveHttpClientAutoConfiguration` (Boot's
 * `ClientHttpConnector`, its builder and the `ReactorResourceFactory`) and `WebClientAutoConfiguration` (the
 * prototype `WebClient.Builder` and the customizer that puts Boot's connector on it) - the pieces a host has
 * that the engine switch could plausibly disturb. The `app.webclient` binding is the module's own
 * `@EnableConfigurationProperties`, so the engine value travels from the Environment like a host's YAML.
 */
class WebClientEngineIsolationTest {
    companion object {
        private const val BACKEND = "probe"

        private lateinit var wireMock: WireMockServer

        @JvmStatic
        @BeforeAll
        fun startWireMock() {
            wireMock = WireMockServer(wireMockConfig().dynamicPort()).apply { start() }
        }

        @JvmStatic
        @AfterAll
        fun stopWireMock() {
            wireMock.stop()
        }

        /** The User-Agent prefix each engine sends on its own - the wire evidence of which engine served a call. */
        private val ENGINE_USER_AGENT =
            mapOf(
                "reactor" to "ReactorNetty/.*",
                "jetty" to "Jetty/.*",
                "http-components" to "Apache-HttpAsyncClient/.*",
                "jdk" to "Java-http-client/.*",
            )
    }

    @BeforeEach
    fun resetWireMock() {
        wireMock.resetAll()
        wireMock.stubFor(get(urlEqualTo("/host")).willReturn(okJson("""{"message":"host"}""")))
        wireMock.stubFor(get(urlEqualTo("/$BACKEND")).willReturn(okJson("""{"message":"veredium"}""")))
    }

    @ParameterizedTest(name = "app.webclient.engine.default={0}")
    @EnumSource(WebClientEngine::class, names = ["JDK", "JETTY", "APACHE"])
    fun `should leave the host's own WebClient on Boot's connector whatever engine this module selects`(engine: WebClientEngine) {
        // What is tested: a host WebClient built from Boot's WebClient.Builder bean, next to this module's
        //   factory configured with a non-default engine.
        // Success criteria: the host's call reaches WireMock with Reactor Netty's own User-Agent - Boot's
        //   auto-detected connector - while this module's backend runs on the selected engine; the context holds
        //   exactly one ClientHttpConnector bean and one ClientHttpConnectorBuilder bean, both Boot's Reactor
        //   ones; and Boot's switch spring.http.clients.reactive.connector is still unset.
        // Why it matters: the requirement of the engine switch is that it changes nothing outside this module.
        //   Setting Boot's switch, registering a connector bean or customizing the shared builder would each
        //   move every WebClient of the host to the module's engine - silently, since they all keep working.
        //
        // Given: Boot's HTTP-client auto-configuration and the module's factory on the selected engine
        hostContext()
            .withPropertyValues("app.webclient.engine.default=${engine.propertyValue}")
            .run { context ->
                // When: the module's client and a host client built from Boot's builder each make a call
                val factory = context.getBean(ResilientWebClientFactory::class.java)
                callThroughModule(factory)
                callThroughHost(context)

                // Then: the host on Boot's Reactor Netty connector, the module on its own engine, Boot's beans
                // and switch untouched
                wireMock.verify(getRequestedFor(urlEqualTo("/host")).withHeader("User-Agent", matching(ENGINE_USER_AGENT.getValue("reactor"))))
                wireMock.verify(getRequestedFor(urlEqualTo("/$BACKEND")).withHeader("User-Agent", equalTo(Veredium.userAgent())))
                assertThat(factory.engineOf(BACKEND)).isEqualTo(engine)
                assertThat(factory.connectorOf(BACKEND)).isNotInstanceOf(ReactorClientHttpConnector::class.java)
                assertThat(context.getBeansOfType(ClientHttpConnector::class.java).values)
                    .singleElement()
                    .isInstanceOf(ReactorClientHttpConnector::class.java)
                assertThat(context.getBeansOfType(ClientHttpConnectorBuilder::class.java).values)
                    .singleElement()
                    .isInstanceOf(ReactorClientHttpConnectorBuilder::class.java)
                assertThat(context.environment.getProperty("spring.http.clients.reactive.connector")).isNull()
            }
    }

    @ParameterizedTest(name = "Boot connector={0}, module engine={1}")
    @CsvSource("jetty, reactor-netty", "http-components, jdk", "jdk, reactor-netty")
    fun `should keep this module's engine when the host sets Boot's global connector switch`(
        bootConnector: String,
        moduleEngine: String,
    ) {
        // What is tested: the reverse direction - a host that moves its own WebClients to another engine with
        //   Boot's spring.http.clients.reactive.connector.
        // Success criteria: the host's call carries the User-Agent of the engine Boot was told to use, and this
        //   module's backend is still built on the engine app.webclient.engine names (Reactor Netty, or the JDK
        //   client), not on Boot's.
        // Why it matters: the factory clones Boot's builder, which now carries Boot's chosen connector; only
        //   the factory replacing it on the clone keeps the host's switch from deciding the mesh clients' engine
        //   - and with it their pool, timeouts and meters.
        //
        // Given: Boot's switch set by the host, the module's engine set separately
        hostContext()
            .withPropertyValues(
                "spring.http.clients.reactive.connector=$bootConnector",
                "app.webclient.engine.default=$moduleEngine",
            ).run { context ->
                // When
                val factory = context.getBean(ResilientWebClientFactory::class.java)
                callThroughModule(factory)
                callThroughHost(context)

                // Then
                wireMock.verify(
                    getRequestedFor(urlEqualTo("/host")).withHeader("User-Agent", matching(ENGINE_USER_AGENT.getValue(bootConnector))),
                )
                val expected =
                    if (moduleEngine == "jdk") JdkClientHttpConnector::class.java else ReactorClientHttpConnector::class.java
                assertThat(factory.connectorOf(BACKEND)).isInstanceOf(expected)
            }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /** The module's client: one call through the factory's backend. */
    private fun callThroughModule(factory: ResilientWebClientFactory) {
        factory.create(BACKEND).get(Greeting::class.java) { it.uri("/$BACKEND") }.block()
    }

    /** The host's client: built from Boot's builder bean, as any other component of the host would build one. */
    private fun callThroughHost(context: AssertableApplicationContext) {
        assertThat(context).hasNotFailed()
        context
            .getBean(WebClient.Builder::class.java)
            .build()
            .get()
            .uri(wireMock.baseUrl() + "/host")
            .retrieve()
            .toBodilessEntity()
            .block()
    }

    private fun hostContext(): ApplicationContextRunner =
        ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    HttpClientAutoConfiguration::class.java,
                    ReactiveHttpClientAutoConfiguration::class.java,
                    WebClientAutoConfiguration::class.java,
                ),
            ).withUserConfiguration(BoundWebClientProperties::class.java)
            .withBean(
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
                        // Generous: the engines are the subject, the call must never time out.
                        TimeLimiterRegistry.of(TimeLimiterConfig.custom().timeoutDuration(Duration.ofSeconds(30)).build()),
                        BulkheadRegistry.ofDefaults(),
                    )
                },
            ).withBean(ObjectMapper::class.java, Supplier { JsonMapper.builder().build() })
            .withBean(ResilientWebClientFactory::class.java)

    /** The `app.webclient` binding alone, as in `LegatiumExchangeLoggingIntegrationTest`. */
    @EnableConfigurationProperties(WebClientProperties::class)
    class BoundWebClientProperties
}
