package eu.inqudium.veredium.restclient

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.okJson
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import eu.inqudium.veredium.core.Veredium
import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.resilience.BlockingResilienceDecorator
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.http.client.ClientHttpRequestFactoryBuilder
import org.springframework.boot.http.client.HttpClientSettings
import org.springframework.boot.http.client.autoconfigure.HttpClientAutoConfiguration
import org.springframework.boot.http.client.autoconfigure.imperative.ImperativeHttpClientAutoConfiguration
import org.springframework.boot.restclient.autoconfigure.RestClientAutoConfiguration
import org.springframework.boot.test.context.assertj.AssertableApplicationContext
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.http.client.ClientHttpRequestFactory
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory
import org.springframework.web.client.RestClient
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.json.JsonMapper
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.function.Supplier

/**
 * The isolation guarantee of `app.restclient.engine`: the property selects the engine of THIS module's clients and
 * of no other client in the host.
 *
 * Every case runs a real Spring context with Boot's own RestClient auto-configuration - the host's side - next to
 * the factory, and a RestClient the HOST builds from Boot's `RestClient.Builder` bean. Which engine carried a
 * request is read off the wire, where each engine announces itself: the host's client sets no User-Agent, so
 * WireMock sees the engine's default token (`Apache-HttpClient/...`, `Java-http-client/...`), while the module's
 * clients send `veredium/<version>` on every engine. The module's side is therefore read off two other traces: its
 * `Accept-Encoding` (Apache offers `gzip, x-gzip, deflate`, Jetty and Reactor Netty `gzip`, the JDK client none),
 * and the selected engine's own pool, which counts the connection of an exchange held in flight. Neither depends on
 * what the factory records about itself. No reflection on Spring's client internals, no mocks.
 */
class RestClientEngineIsolationTest {
    companion object {
        private const val BACKEND = "veredium"

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
    }

    @BeforeEach
    fun resetStubs() {
        wireMock.resetAll()
        wireMock.stubFor(get(urlEqualTo("/host")).willReturn(okJson("""{"message":"host"}""")))
        wireMock.stubFor(get(urlEqualTo("/module")).willReturn(okJson("""{"message":"module"}""")))
    }

    @ParameterizedTest
    @EnumSource(RestClientEngine::class, names = ["JDK", "JETTY", "REACTOR_NETTY"])
    fun `should leave the engine of a host client built from Boot's builder bean untouched`(engine: RestClientEngine) {
        // What is tested: the property's reach - a module default other than Apache, and the host's own RestClient
        //   from the same Boot builder bean the factory clones.
        // Success criteria: the host's request reaches WireMock with Apache HttpClient's User-Agent (Boot's
        //   auto-detected engine with httpclient5 on the classpath), while the module's client sent the module's
        //   token and an Accept-Encoding other than Apache's, and - where the engine counts connections - its
        //   held exchange shows as the one connection in use of the selected engine.
        // Why it matters: the requirement is that the engine switch must not affect any other client of the host.
        //   A factory that set the request factory on Boot's builder rather than on its clone, or registered an
        //   engine bean Boot's auto-configuration picks up, would move the host's clients along with its own.
        //
        // Given: a host context with Boot's RestClient auto-configuration and the module on `engine`
        hostContext(RestClientProperties.Engine(default = engine)).run { context ->
            assertThat(context).hasNotFailed()
            val factory = context.getBean(ResilientRestClientFactory::class.java)

            // When: the module's client and a host client built from Boot's builder bean each make a request
            factory.create(BACKEND).get(Map::class.java) { it.uri("/module") }
            hostClient(context)
                .get()
                .uri("/host")
                .retrieve()
                .toBodilessEntity()

            // Then: the host's request went out on Apache, the module's on the selected engine
            assertThat(userAgentOf("/host")).startsWith("Apache-HttpClient/")
            assertThat(userAgentOf("/module")).isEqualTo(Veredium.userAgent())
            assertThat(acceptEncodingOf("/module")).`as`("the module's request must not have gone out on Apache").isNotEqualTo(acceptEncodingOf("/host"))
            val transport = factory.engineTransports.getValue(engine)
            if (transport.connectionsInUse() != null) {
                assertThat(connectionsInUseDuringHeldCall(factory, transport)).isEqualTo(1)
            }
        }
    }

    @Test
    fun `should keep the module on its own engine when the host sets Boot's global switch`() {
        // What is tested: the other direction - Boot's `spring.http.clients.imperative.factory`, set by the host,
        //   against the module's default engine.
        // Success criteria: the host's client runs on the JDK client Boot's switch selects (its User-Agent on the
        //   wire), the module's client on Apache - Apache's Accept-Encoding on the wire, its held exchange the one
        //   connection in use of the factory's Apache pool, and its request factory Spring's HttpComponents one,
        //   built by the factory, not the one Boot's switch put on the builder.
        // Why it matters: the module neither reads Boot's switch nor lets it in through the builder it clones; a
        //   host that moves its own clients must not move the mesh clients, whose timeouts and pool the switch
        //   knows nothing about.
        //
        // Given: Boot's HTTP client auto-configuration with its global switch on `jdk`
        hostContext(RestClientProperties.Engine())
            .withConfiguration(AutoConfigurations.of(HttpClientAutoConfiguration::class.java, ImperativeHttpClientAutoConfiguration::class.java))
            .withPropertyValues("spring.http.clients.imperative.factory=jdk")
            .run { context ->
                assertThat(context).hasNotFailed()
                val factory = context.getBean(ResilientRestClientFactory::class.java)

                // When
                factory.create(BACKEND).get(Map::class.java) { it.uri("/module") }
                hostClient(context)
                    .get()
                    .uri("/host")
                    .retrieve()
                    .toBodilessEntity()

                // Then
                assertThat(userAgentOf("/host")).startsWith("Java-http-client/")
                assertThat(factory.requestFactoryOf(BACKEND)).isInstanceOf(HttpComponentsClientHttpRequestFactory::class.java)
                assertThat(userAgentOf("/module")).isEqualTo(Veredium.userAgent())
                assertThat(acceptEncodingOf("/module")).contains("x-gzip")
                assertThat(connectionsInUseDuringHeldCall(factory, factory.engineTransports.getValue(RestClientEngine.APACHE))).isEqualTo(1)
            }
    }

    @Test
    fun `should register no HTTP client, request factory or Boot client settings bean for any engine`() {
        // What is tested: what the module leaves in the host's context with all four engines in use.
        // Success criteria: the context holds no bean of Spring's ClientHttpRequestFactory, Boot's
        //   ClientHttpRequestFactoryBuilder or HttpClientSettings, nor of any engine's client or pool type.
        // Why it matters: Boot's auto-configuration and a host's own wiring look such beans up by type - one the
        //   module registered would silently become the engine of the host's other clients.
        //
        // Given: one backend per engine
        val engines =
            RestClientProperties.Engine(
                endpoints = mapOf("on-jdk" to RestClientEngine.JDK, "on-jetty" to RestClientEngine.JETTY, "on-reactor" to RestClientEngine.REACTOR_NETTY),
            )
        hostContext(engines, aliases = listOf(BACKEND, "on-jdk", "on-jetty", "on-reactor")).run { context ->
            assertThat(context).hasNotFailed()

            // When: every engine is created and used
            val factory = context.getBean(ResilientRestClientFactory::class.java)
            listOf(BACKEND, "on-jdk", "on-jetty", "on-reactor").forEach { factory.create(it).get(Map::class.java) { spec -> spec.uri("/module") } }
            assertThat(factory.engineTransports.keys).containsExactlyInAnyOrder(*RestClientEngine.entries.toTypedArray())

            // Then
            listOf(
                ClientHttpRequestFactory::class.java,
                ClientHttpRequestFactoryBuilder::class.java,
                HttpClientSettings::class.java,
                org.apache.hc.client5.http.classic.HttpClient::class.java,
                java.net.http.HttpClient::class.java,
                org.eclipse.jetty.client.HttpClient::class.java,
                reactor.netty.http.client.HttpClient::class.java,
                reactor.netty.resources.ConnectionProvider::class.java,
                reactor.netty.resources.LoopResources::class.java,
            ).forEach { type -> assertThat(context.getBeanNamesForType(type)).`as`("beans of %s", type.name).isEmpty() }
        }
    }

    /** The User-Agent of the one request WireMock received on [path]. */
    private fun userAgentOf(path: String): String =
        wireMock
            .findAll(getRequestedFor(urlEqualTo(path)))
            .single()
            .header("User-Agent")
            .firstValue()

    /** The Accept-Encoding of the one request WireMock received on [path], empty when it sent none. */
    private fun acceptEncodingOf(path: String): String =
        wireMock
            .findAll(getRequestedFor(urlEqualTo(path)))
            .single()
            .header("Accept-Encoding")
            .let { if (it.isPresent) it.values().joinToString(", ") else "" }

    /**
     * The connections in use of [transport] while one call of the module's [BACKEND] is held in flight by the
     * backend - read once the request has reached WireMock, then the call is let finish.
     */
    private fun connectionsInUseDuringHeldCall(
        factory: ResilientRestClientFactory,
        transport: EngineTransport,
    ): Int? {
        val arrived = CountDownLatch(1)
        wireMock.stubFor(get(urlEqualTo("/held")).willReturn(okJson("""{"message":"held"}""").withFixedDelay(500)))
        wireMock.addMockServiceRequestListener { request, _ -> if (request.url == "/held") arrived.countDown() }
        val call = CompletableFuture.supplyAsync { factory.create(BACKEND).get(Map::class.java) { it.uri("/held") } }
        assertThat(arrived.await(5, TimeUnit.SECONDS)).`as`("the held call must have reached the backend").isTrue()
        val inUse = transport.connectionsInUse()
        call.get(5, TimeUnit.SECONDS)
        return inUse
    }

    /** A client the HOST builds from Boot's builder bean, against WireMock - nothing of the module on it. */
    private fun hostClient(context: AssertableApplicationContext): RestClient =
        context
            .getBean(RestClient.Builder::class.java)
            .baseUrl(wireMock.baseUrl())
            .build()

    /**
     * A host context: Boot's RestClient auto-configuration (the builder bean both sides use) and the module's
     * factory with its collaborators, the engines selected by [engines]. Every backend of [aliases] points at
     * WireMock with the triplet host the JDK engine can send.
     */
    private fun hostContext(
        engines: RestClientProperties.Engine,
        aliases: List<String> = listOf(BACKEND),
    ): ApplicationContextRunner {
        val circuitBreakers = CircuitBreakerRegistry.ofDefaults()
        val timeLimiters = TimeLimiterRegistry.of(TimeLimiterConfig.custom().timeoutDuration(Duration.ofSeconds(30)).build())
        val bulkheads = BulkheadRegistry.ofDefaults()
        val authority = "localhost:${wireMock.port()}"
        return ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(RestClientAutoConfiguration::class.java))
            .withBean(RestClientProperties::class.java, Supplier { RestClientProperties(engine = engines) })
            .withBean(
                MeshProperties::class.java,
                Supplier {
                    MeshProperties(
                        defaults =
                            MeshProperties.Defaults(
                                individualRequestTimeoutMs = 1000,
                                transport = MeshProperties.Transport(sidecarBaseUrl = wireMock.baseUrl()),
                            ),
                        endpoints = aliases.map { MeshProperties.EndpointConfig(triplet = "/endpoint/$authority/GET/$it", alias = it) },
                    )
                },
            ).withBean(CircuitBreakerRegistry::class.java, Supplier { circuitBreakers })
            .withBean(TimeLimiterRegistry::class.java, Supplier { timeLimiters })
            .withBean(BulkheadRegistry::class.java, Supplier { bulkheads })
            // Built here rather than through RestClientConfiguration, as in ResilientRestClientFactoryBuilderWiringTest:
            // the core's mesh wiring it imports needs Resilience4j's auto-configuration, which is not the subject.
            .withBean(
                BlockingResilienceDecorator::class.java,
                Supplier { BlockingResilienceDecorator(circuitBreakers, timeLimiters, bulkheads, workerNamePrefix = RestClientConfiguration.WORKER_NAME_PREFIX) },
            ).withBean(ObjectMapper::class.java, Supplier { JsonMapper.builder().build() })
            .withBean(ResilientRestClientFactory::class.java)
    }
}
