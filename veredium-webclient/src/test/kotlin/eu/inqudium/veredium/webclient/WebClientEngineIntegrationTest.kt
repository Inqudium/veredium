package eu.inqudium.veredium.webclient

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import ch.qos.logback.core.read.ListAppender
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.absent
import com.github.tomakehurst.wiremock.client.WireMock.containing
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.equalToJson
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.okJson
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import eu.inqudium.legatium.webclient.logging.ClientRequestLoggingFilter
import eu.inqudium.veredium.core.Veredium
import eu.inqudium.veredium.core.client.EmptyResponseBodyException
import eu.inqudium.veredium.core.mesh.MeshProperties
import io.github.resilience4j.bulkhead.BulkheadConfig
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micrometer.observation.Observation
import io.micrometer.observation.ObservationHandler
import io.micrometer.observation.ObservationRegistry
import io.netty.resolver.DefaultAddressResolverGroup
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.MethodSource
import org.slf4j.LoggerFactory
import org.springframework.core.io.buffer.DataBufferLimitException
import org.springframework.http.MediaType
import org.springframework.http.client.reactive.JdkClientHttpConnector
import org.springframework.http.client.reactive.JettyClientHttpConnector
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.util.unit.DataSize
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientRequestException
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Flux
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.json.JsonMapper
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.stream.Stream
import java.util.zip.GZIPOutputStream
import kotlin.concurrent.thread

/**
 * The client contract on EVERY engine of `app.webclient.engine`, end to end against a real backend: what a
 * caller gets for a success, an error status, a slow backend, an oversized or empty body, and what a
 * TimeLimiter abort does to the exchange - plus the per-backend override, the lifecycle of the engine
 * resources and the startup lines.
 *
 * Parameterized over [WebClientEngine] rather than written per engine, because the point of most cases is
 * that the answer does NOT depend on the engine: everything above the connector - codec limit, status mapping,
 * empty-body contract, headers, Resilience4j - is shared, and a test per engine would invite the cases to
 * drift apart. Where an engine legitimately differs (compression), the expectation names it.
 *
 * The JDK engine runs here because the main test JVM starts with `jdk.httpclient.allowRestrictedHeaders=host`
 * (see the module POM) - the setup the jdk engine documents. The failure without it is pinned in its own JVM
 * by `JdkEngineRestrictedHostHeaderTest`.
 *
 * No Spring context: the factories are constructed directly, on a bare builder standing in for Boot's bean,
 * as in `ResilientWebClientIntegrationTest`. Boot's context and its own connector are the subject of
 * `WebClientEngineIsolationTest`.
 */
class WebClientEngineIntegrationTest {
    companion object {
        private lateinit var wireMock: WireMockServer

        /** The TimeLimiter budget of every case not ABOUT the budget - see `newFactory` there for why not 1 s. */
        private val GENEROUS_BUDGET: Duration = Duration.ofSeconds(30)

        /** The endpoints' anchor; the derived response timeout is 1000 + 200 + 200 = 1400 ms. */
        private const val ANCHOR_MS = 1000L
        private val RESPONSE_TIMEOUT: Duration = Duration.ofMillis(1400)

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

        /** Every engine against both error statuses the mapping must preserve. */
        @JvmStatic
        fun enginesAndErrorStatuses(): Stream<Arguments> = WebClientEngine.entries.stream().flatMap { engine -> Stream.of(Arguments.of(engine, 404), Arguments.of(engine, 500)) }
    }

    private val objectMapper: ObjectMapper = JsonMapper.builder().build()

    private val factories = mutableListOf<ResilientWebClientFactory>()

    @BeforeEach
    fun resetStubs() {
        wireMock.resetAll()
    }

    @AfterEach
    fun tearDown() {
        factories.forEach { it.shutdown() }
        factories.clear()
    }

    @Nested
    inner class `The client contract on every engine` {
        @ParameterizedTest(name = "{0}")
        @EnumSource(WebClientEngine::class)
        fun `should deliver a GET with the mesh Host header, the library's User-Agent and JSON Accept`(engine: WebClientEngine) {
            // What is tested: the happy path of get() and the default headers on each engine - including the
            //   explicit Host header, which an engine is free to overwrite with the URI's authority.
            // Success criteria: the call returns the stubbed message; WireMock saw Host `localhost` (the triplet's
            //   host, not `localhost:<port>` of the base URL), Veredium's User-Agent rather than the engine's own,
            //   and Accept application/json.
            // Why it matters: the Host header is what routes a sidecar to the upstream. An engine that replaced
            //   it with the authority of the base URL would send every call of the backend to the wrong upstream,
            //   and nothing on the client side would fail.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/hello")).willReturn(okJson("""{"message":"hi from $engine"}""")))
            val client = newFactory(engine, "greet").create("greet")

            // When
            val result = client.get(Greeting::class.java) { it.uri("/hello") }.block()

            // Then
            assertThat(result?.message).isEqualTo("hi from $engine")
            wireMock.verify(
                getRequestedFor(urlEqualTo("/hello"))
                    .withHeader("Host", equalTo("localhost"))
                    .withHeader("User-Agent", equalTo(Veredium.userAgent()))
                    .withHeader("Accept", equalTo(MediaType.APPLICATION_JSON_VALUE)),
            )
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(WebClientEngine::class)
        fun `should run the cloned builder's filters and observation over the engine`(engine: WebClientEngine) {
            // What is tested: that what sits on Boot's builder - a filter (legatium's exchange logging is one),
            //   the observation registry - still runs when the factory swaps the connector on its clone.
            // Success criteria: a filter on the builder sees the request carrying the backend name in legatium's
            //   adapter-name attribute, and one `http.client.requests` timer with status 200 is recorded.
            // Why it matters: the exchange event and the client span are the operator's view of every call; an
            //   engine switch that lost them would change what is observable, not what is called.
            //
            // Given: a builder carrying a recording filter and an observing registry, as Boot's would, and the
            // module's observation toggle on
            wireMock.stubFor(get(urlEqualTo("/observed")).willReturn(okJson("""{"message":"seen"}""")))
            val meters = SimpleMeterRegistry()
            val adapterNames = CopyOnWriteArrayList<Any?>()
            // The observation stops on the thread that completes the exchange, which may be after block() returned.
            val stopped = StopLatch()
            val builder =
                WebClient
                    .builder()
                    .observationRegistry(
                        ObservationRegistry.create().apply {
                            // The latch first: Micrometer notifies onStop in REVERSE registration order, so it
                            // fires after the meter handler recorded the timer.
                            observationConfig()
                                .observationHandler(stopped)
                                .observationHandler(DefaultMeterObservationHandler(meters))
                        },
                    ).filter { request, next ->
                        adapterNames += request.attribute(ClientRequestLoggingFilter.ADAPTER_NAME_ATTRIBUTE).orElse(null)
                        next.exchange(request)
                    }
            val client = newFactory(WebClientProperties.Engine(default = engine), listOf("observed"), webClientBuilder = builder, observationEnabled = true).create("observed")

            // When
            client.get(Greeting::class.java) { it.uri("/observed") }.block()

            // Then
            assertThat(adapterNames).containsExactly("observed")
            assertThat(stopped.latch.await(5, TimeUnit.SECONDS)).`as`("the observation stopped").isTrue()
            val timer = meters.find("http.client.requests").tag("status", "200").timer()
            assertThat(timer).isNotNull()
            assertThat(timer!!.count()).isEqualTo(1L)
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(WebClientEngine::class)
        fun `should send a POST body once and deserialise the answer`(engine: WebClientEngine) {
            // What is tested: post() on each engine - the request body reaching the wire, the answer decoded.
            // Success criteria: WireMock received exactly one POST whose body is the serialised request, and the
            //   call returns the stubbed message.
            // Why it matters: each engine adapts Spring's request body publisher to its own body API (a Flow
            //   publisher, Jetty content chunks, an Apache entity producer); a body that arrives empty or twice is
            //   an engine-specific defect a GET never shows.
            //
            // Given
            wireMock.stubFor(
                post(urlEqualTo("/lookup"))
                    .withRequestBody(equalToJson("""{"id":"42"}"""))
                    .willReturn(okJson("""{"message":"found"}""")),
            )
            val client = newFactory(engine, "lookup").create("lookup")

            // When
            val result =
                client
                    .post(Greeting::class.java, LookupRequest("42")) {
                        it.uri("/lookup").contentType(MediaType.APPLICATION_JSON)
                    }.block()

            // Then
            assertThat(result?.message).isEqualTo("found")
            wireMock.verify(1, postRequestedFor(urlEqualTo("/lookup")).withRequestBody(equalToJson("""{"id":"42"}""")))
        }

        @ParameterizedTest(name = "{0} -> {1}")
        @MethodSource("eu.inqudium.veredium.webclient.WebClientEngineIntegrationTest#enginesAndErrorStatuses")
        fun `should surface an error status as a WebClientResponseException carrying it`(
            engine: WebClientEngine,
            status: Int,
        ) {
            // What is tested: the status mapping of retrieve() on each engine, for a benign 4xx and a 5xx.
            // Success criteria: the call fails with a WebClientResponseException whose status is the stubbed one.
            // Why it matters: the breaker verdicts and the error line's severity read the status off this very
            //   exception type; an engine that surfaced a 404 as a transport error would count benign answers as
            //   outages.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/status")).willReturn(aResponse().withStatus(status).withBody("nope")))
            val client = newFactory(engine, "status").create("status")

            // When
            val failure = catchThrowable { client.get(Greeting::class.java) { it.uri("/status") }.block() }

            // Then
            assertThat(failure).isInstanceOfSatisfying(WebClientResponseException::class.java) { error ->
                assertThat(error.statusCode.value()).isEqualTo(status)
            }
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(WebClientEngine::class)
        fun `should fail at the endpoint's response timeout, before the backend answers`(engine: WebClientEngine) {
            // What is tested: that the per-endpoint response timeout of the mesh ladder reaches each engine.
            // Success criteria: against a backend 3 s late and a 30 s budget, the call fails with a
            //   WebClientRequestException (the transport's verdict, not the TimeLimiter's TimeoutException), no
            //   earlier than the 1400 ms response timeout and before the ladder's TimeLimiter rung at 1800 ms
            //   (response timeout + connect 100 + pending-acquire 200 + 100 headroom) - the rung the transport
            //   guard must beat when the budget is the derived one.
            // Why it matters: the response timeout is the ladder's transport guard - tighter than the budget, so
            //   the breaker records a transport failure rather than a budget overrun. An engine that ignored it
            //   would wait for the backend however long it takes, and the ladder would silently lose a rung.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/late")).willReturn(okJson("""{"message":"late"}""").withFixedDelay(3_000)))
            val client = newFactory(engine, "late").create("late")
            assertThat(client.responseTimeout).isEqualTo(RESPONSE_TIMEOUT)

            // When
            val started = System.nanoTime()
            val failure = catchThrowable { client.get(Greeting::class.java) { it.uri("/late") }.block() }
            val elapsed = Duration.ofNanos(System.nanoTime() - started)

            // Then
            assertThat(failure).isInstanceOf(WebClientRequestException::class.java)
            assertThat(elapsed).isGreaterThanOrEqualTo(RESPONSE_TIMEOUT.minusMillis(50)).isLessThan(RESPONSE_TIMEOUT.plusMillis(400))
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(WebClientEngine::class)
        fun `should refuse a body past the codec limit as a client-side failure the breaker ignores`(engine: WebClientEngine) {
            // What is tested: maxInMemorySize on each engine, through the real pipeline and the shipped verdicts.
            // Success criteria: a 4 KB answer against a 200 B limit fails with DataBufferLimitException as the
            //   root cause, and the breaker records no failure.
            // Why it matters: the limit sits in the codec, above the connector, so it must hold for every engine;
            //   and the verdict reads the cause chain, whose shape an engine could change - a different wrapper
            //   would make the breaker count a healthy backend's correct answer.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/big")).willReturn(okJson("""{"message":"${"X".repeat(4000)}"}""")))
            val circuitBreakers = CircuitBreakerRegistry.ofDefaults()
            val factory = newFactory(engine, "big", maxInMemorySize = DataSize.ofBytes(200), circuitBreakers = circuitBreakers)
            CustomizedRegistries.circuitBreakers(circuitBreakers, meshFor(listOf("big"), wireMock.baseUrl()))
            val client = factory.create("big")

            // When
            val failure = catchThrowable { client.get(Greeting::class.java) { it.uri("/big") }.block() }

            // Then
            assertThat(failure).hasRootCauseInstanceOf(DataBufferLimitException::class.java)
            val breaker = circuitBreakers.circuitBreaker("big")
            assertThat(breaker.metrics.numberOfFailedCalls).isZero()
            assertThat(breaker.state).isEqualTo(CircuitBreaker.State.CLOSED)
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(WebClientEngine::class)
        fun `should fail a 204 with EmptyResponseBodyException`(engine: WebClientEngine) {
            // What is tested: the empty-body contract on each engine.
            // Success criteria: a 204 answer fails the call with EmptyResponseBodyException naming the backend.
            // Why it matters: an engine reports "no body" its own way (no content publisher, an empty one, a
            //   zero-length entity); the contract must not depend on which, or the Mono completes empty again on
            //   one engine and the caller's awaitSingle() fails with a message that names nothing.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/nothing")).willReturn(aResponse().withStatus(204)))
            val client = newFactory(engine, "nothing").create("nothing")

            // When
            val failure = catchThrowable { client.get(Greeting::class.java) { it.uri("/nothing") }.block() }

            // Then
            assertThat(failure).isInstanceOf(EmptyResponseBodyException::class.java).hasMessageContaining("'nothing'")
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(WebClientEngine::class)
        fun `should abort the exchange when the TimeLimiter cancels it, releasing the bulkhead permit`(engine: WebClientEngine) {
            // What is tested: what the TimeLimiter's cancellation does to the exchange underneath, per engine.
            // Success criteria: against a backend that reads the request and never answers, the call fails with
            //   the TimeLimiter's TimeoutException (300 ms budget); the bulkhead has its one permit back; and the
            //   backend sees the client close the connection within a second of the timeout - before the 1400 ms
            //   response timeout could have done it; and Reactor logs no "onErrorDropped" ERROR for the failure
            //   an engine reports after the abort.
            // Why it matters: cancellation is how the budget frees what a stalled call holds. An engine that only
            //   drops the subscriber keeps the connection - and with it a pool slot - until the response timeout,
            //   so under a stalled backend the pool fills with exchanges nobody waits for. Spring's Apache
            //   connector is such a case on its own; this pins that the module's abort bridge closes the gap, and
            //   that the other engines abort on their own. The ERROR line is pinned because Jetty and Apache
            //   report the aborted exchange as a failure while the cancellation is still travelling - Reactor
            //   would log every TimeLimiter timeout on those engines as an unhandled error.
            //
            // Given: a backend that never answers, a 300 ms budget, a one-permit bulkhead, Reactor's log captured
            val reactorLog = LoggerFactory.getLogger("reactor.core.publisher.Operators") as Logger
            val dropped = ConcurrentAppender().apply { start() }
            reactorLog.addAppender(dropped)
            SilentBackend().use { backend ->
                val bulkheads = BulkheadRegistry.of(BulkheadConfig.custom().maxConcurrentCalls(1).build())
                val client =
                    newFactory(
                        engine,
                        "stalled",
                        baseUrl = backend.baseUrl,
                        timeLimiter = Duration.ofMillis(300),
                        bulkheads = bulkheads,
                    ).create("stalled")

                // When
                val started = System.nanoTime()
                val failure = catchThrowable { runBlocking { client.get(Greeting::class.java) { it.uri("/stalled") }.awaitSingle() } }

                // Then: the budget's verdict, the permit back, and the connection closed by the client
                assertThat(failure).isExactlyInstanceOf(TimeoutException::class.java)
                assertThat(bulkheads.bulkhead("stalled").metrics.availableConcurrentCalls).isEqualTo(1)
                assertThat(backend.requestArrived.await(5, TimeUnit.SECONDS)).`as`("the request reached the backend").isTrue()
                assertThat(backend.connectionClosed.await(1, TimeUnit.SECONDS))
                    .`as`("the client closed the connection after the abort")
                    .isTrue()
                assertThat(Duration.ofNanos(backend.closedAtNanos - started)).isLessThan(RESPONSE_TIMEOUT)
                assertThat(dropped.events.filter { it.level == Level.ERROR }.map { it.formattedMessage }).isEmpty()
            }
            reactorLog.detachAppender(dropped)
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(WebClientEngine::class, names = ["REACTOR_NETTY", "JETTY", "APACHE"])
        fun `should ask for gzip and decode a gzip answer where the startup line claims it`(engine: WebClientEngine) {
            // What is tested: the "gzip" entry of the startup line of the three engines that claim it.
            // Success criteria: the request carries an Accept-Encoding naming gzip, and a gzip-encoded answer is
            //   decoded to the message.
            // Why it matters: the startup line is the operator's statement of what applies; a claim nothing pins
            //   would drift from the truth on the first dependency bump.
            //
            // Given
            wireMock.stubFor(
                get(urlEqualTo("/zipped")).willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                        .withHeader("Content-Encoding", "gzip")
                        .withBody(gzip("""{"message":"unzipped"}""")),
                ),
            )
            val client = newFactory(engine, "zipped").create("zipped")

            // When
            val result = client.get(Greeting::class.java) { it.uri("/zipped") }.block()

            // Then
            assertThat(result?.message).isEqualTo("unzipped")
            wireMock.verify(getRequestedFor(urlEqualTo("/zipped")).withHeader("Accept-Encoding", containing("gzip")))
        }

        @Test
        fun `should not ask for compression on the jdk engine, whose startup line says it does not apply`() {
            // What is tested: the other side of the gzip claim - the JDK client decodes no content coding.
            // Success criteria: the request carries no Accept-Encoding header.
            // Why it matters: a client that asked for gzip without decoding it would hand the codec compressed
            //   bytes; the startup line's "does NOT apply ... gzip" is what an operator relies on.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/plain")).willReturn(okJson("""{"message":"plain"}""")))
            val client = newFactory(WebClientEngine.JDK, "plain").create("plain")

            // When
            client.get(Greeting::class.java) { it.uri("/plain") }.block()

            // Then
            wireMock.verify(getRequestedFor(urlEqualTo("/plain")).withHeader("Accept-Encoding", absent()))
        }
    }

    @Nested
    inner class `Per-backend override` {
        @Test
        fun `should move only the overridden backend to its engine and keep the other on the default`() {
            // What is tested: app.webclient.engine.endpoints - one of two backends overridden to jetty.
            // Success criteria: the overridden backend's client is built on Spring's Jetty connector, the other on
            //   Reactor Netty's; both clients complete a call.
            // Why it matters: the override exists to move ONE backend - to try an engine against real traffic, or
            //   to work around one backend's quirk - and must not take its neighbours with it.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/hello")).willReturn(okJson("""{"message":"hi"}""")))
            val factory =
                newFactory(
                    WebClientProperties.Engine(endpoints = mapOf("moved" to WebClientEngine.JETTY)),
                    listOf("moved", "stays"),
                )

            // When
            val moved = factory.create("moved").get(Greeting::class.java) { it.uri("/hello") }.block()
            val stays = factory.create("stays").get(Greeting::class.java) { it.uri("/hello") }.block()

            // Then
            assertThat(moved?.message).isEqualTo("hi")
            assertThat(stays?.message).isEqualTo("hi")
            assertThat(factory.engineOf("moved")).isEqualTo(WebClientEngine.JETTY)
            assertThat(factory.engineOf("stays")).isEqualTo(WebClientEngine.REACTOR_NETTY)
            assertThat((factory.connectorOf("moved") as JettyEndpointConnector).delegate).isInstanceOf(JettyClientHttpConnector::class.java)
            assertThat(factory.connectorOf("stays")).isInstanceOf(ReactorClientHttpConnector::class.java)
        }

        @Test
        fun `should build each engine on Spring's connector for it`() {
            // What is tested: the mapping of the four values onto Spring's four reactive connectors.
            // Success criteria: reactor-netty -> ReactorClientHttpConnector, jdk -> JdkClientHttpConnector,
            //   jetty -> the module's per-request wrapper around JettyClientHttpConnector, apache -> the module's
            //   abort bridge, which drives HttpComponentsClientHttpConnector per exchange.
            // Why it matters: the documentation promises Spring's connectors; a value that silently fell back to
            //   another engine would pass every behavioural test above that the two engines share.
            //
            // Given
            val engines = WebClientEngine.entries.associateBy { it.propertyValue }
            val factory = newFactory(WebClientProperties.Engine(endpoints = engines), engines.keys.toList())

            // When
            engines.keys.forEach(factory::create)

            // Then
            assertThat(factory.connectorOf("reactor-netty")).isInstanceOf(ReactorClientHttpConnector::class.java)
            assertThat(factory.connectorOf("jdk")).isInstanceOf(JdkClientHttpConnector::class.java)
            assertThat((factory.connectorOf("jetty") as JettyEndpointConnector).delegate).isInstanceOf(JettyClientHttpConnector::class.java)
            assertThat(factory.connectorOf("apache")).isInstanceOf(ApacheEndpointConnector::class.java)
        }
    }

    @Nested
    inner class `Engine resources` {
        @Test
        fun `should create only the engines in use, one per engine for all its backends`() {
            // What is tested: lazy creation and sharing of the engine resources.
            // Success criteria: before any client exists, no engine has resources; after two jdk backends and one
            //   apache backend built their clients, exactly jdk and apache have resources - one transport each -
            //   and Reactor Netty's pool, the default engine's, was never created.
            // Why it matters: an engine nobody uses must cost nothing - no pool, no I/O reactor, no threads - and
            //   backends of one engine must share its pool rather than each opening their own.
            //
            // Given
            val factory =
                newFactory(
                    WebClientProperties.Engine(
                        default = WebClientEngine.JDK,
                        endpoints = mapOf("third" to WebClientEngine.APACHE),
                    ),
                    listOf("first", "second", "third"),
                )
            assertThat(factory.startedEngines()).isEmpty()

            // When
            listOf("first", "second", "third").forEach(factory::create)

            // Then
            assertThat(factory.startedEngines().keys).containsExactlyInAnyOrder(WebClientEngine.JDK, WebClientEngine.APACHE)
        }

        @Test
        fun `should run reactor-netty on the factory's own loops and resolver, not Reactor Netty's global resources`() {
            // What is tested: the isolation of the default engine from Reactor Netty's process-wide HttpResources.
            // Success criteria: the response of a reactor-netty backend is handled on a thread of the factory's
            //   own loop group (named after the pool, `lap-webclient-...`), not on a `reactor-http-...` thread of
            //   the global loops; and the backend's HttpClient carries the explicit resolver rather than the
            //   default one, which HttpResources would supply.
            // Why it matters: HttpResources is shared with every other Reactor Netty client of the JVM and managed,
            //   in a Boot host, by the host's ReactorResourceFactory - disposed when the host decides, sized as the
            //   host decides. A factory on those resources is coupled to clients it knows nothing of.
            //
            // Given: a builder whose filter records the thread the response arrives on
            wireMock.stubFor(get(urlEqualTo("/where")).willReturn(okJson("""{"message":"here"}""")))
            val threads = CopyOnWriteArrayList<String>()
            val builder =
                WebClient.builder().filter { request, next ->
                    next.exchange(request).doOnNext { threads += Thread.currentThread().name }
                }
            val factory = newFactory(WebClientProperties.Engine(), listOf("where"), webClientBuilder = builder)

            // When
            factory.create("where").get(Greeting::class.java) { it.uri("/where") }.block()

            // Then
            assertThat(threads).singleElement().asString().startsWith(ResilientWebClientFactory.POOL_NAME)
            val transport = factory.startedEngines().getValue(WebClientEngine.REACTOR_NETTY) as ReactorNettyTransport
            val endpoint = meshFor(listOf("where"), wireMock.baseUrl()).resolveAll().values.single()
            assertThat(transport.httpClientFor(endpoint).configuration().resolver()).isSameAs(DefaultAddressResolverGroup.INSTANCE)
        }

        @Test
        fun `should dispose the reactor-netty loops after the drain delay, once nothing can still need them`() {
            // What is tested: the deferred disposal of the engine's own event loops.
            // Success criteria: release() disposes the pool at once and leaves the loops running; the loops are
            //   disposed once the delay passed (bounded wait, a 50 ms delay here).
            // Why it matters: closing a loop group closes every channel on it, so disposing it together with the
            //   pool would cut the in-flight calls the pool disposal lets finish
            //   (`ResilientWebClientIntegrationTest` pins that); never disposing it would leak its threads into
            //   the next context of the JVM.
            //
            // Given
            val transport =
                ReactorNettyTransport(WebClientProperties(), MeshProperties.Transport(), "loops-probe", Duration.ofMillis(50)) { it.block() }

            // When
            transport.release()

            // Then
            assertThat(transport.isReleased()).isTrue()
            assertThat(transport.loops.isDisposed).isFalse()
            val disposed =
                Flux
                    .interval(Duration.ofMillis(10))
                    .map { transport.loops.isDisposed }
                    .filter { it }
                    .blockFirst(Duration.ofSeconds(5))
            assertThat(disposed).isTrue()
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(WebClientEngine::class)
        fun `should release the engine's resources on shutdown`(engine: WebClientEngine) {
            // What is tested: shutdown() for each engine after a completed call left a connection behind.
            // Success criteria: before shutdown the engine's resources are live; after it they are released
            //   (Reactor Netty's pool disposed, the JDK client terminated, Jetty's client stopped, Apache's I/O
            //   reactor shut down and its evictor stopped); a second shutdown does not throw.
            // Why it matters: each engine owns threads and sockets the factory created; a context that stops
            //   without releasing them leaks them into the next context of the same JVM - a test suite, a
            //   refresh - where nothing points back at their owner.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/bye")).willReturn(okJson("""{"message":"bye"}""")))
            val factory = newFactory(engine, "bye")
            factory.create("bye").get(Greeting::class.java) { it.uri("/bye") }.block()
            val transport = factory.startedEngines().getValue(engine)
            assertThat(transport.isReleased()).isFalse()

            // When
            factory.shutdown()
            val second = catchThrowable { factory.shutdown() }

            // Then
            assertThat(transport.isReleased()).isTrue()
            assertThat(second).isNull()
        }
    }

    @Nested
    inner class `Startup lines` {
        @Test
        fun `should name the engine of every backend sorted by alias, and one coverage line per engine in use`() {
            // What is tested: the INFO lines the factory writes at construction about the engines.
            // Success criteria: one line lists `alpha=jdk, beta=reactor-netty, gamma=jdk` in alias order with the
            //   default; one line per engine in use (jdk, reactor-netty, not jetty or apache) states what applies
            //   and what does not - the jdk line names max-connections under "does NOT apply" and says it has no
            //   transport meters, the reactor-netty line names its two meter families.
            // Why it matters: an engine silently ignores the settings it has no knob for; the line is the only
            //   place where an operator learns that a tuned pool setting does nothing for a backend.
            //
            // Given
            val logger = LoggerFactory.getLogger(ResilientWebClientFactory::class.java) as Logger
            val lines = ListAppender<ILoggingEvent>().apply { start() }
            logger.addAppender(lines)
            try {
                // When
                newFactory(
                    WebClientProperties.Engine(endpoints = mapOf("alpha" to WebClientEngine.JDK, "gamma" to WebClientEngine.JDK)),
                    listOf("gamma", "beta", "alpha"),
                )

                // Then
                val engineLines = lines.list.filter { it.level == Level.INFO && it.formattedMessage.startsWith("HTTP engine") }.map { it.formattedMessage }
                assertThat(engineLines).hasSize(3)
                assertThat(engineLines[0])
                    .isEqualTo("HTTP engine per backend (app.webclient.engine, default reactor-netty): alpha=jdk, beta=reactor-netty, gamma=jdk")
                assertThat(engineLines[1])
                    .startsWith("HTTP engine 'reactor-netty' (ReactorClientHttpConnector over Reactor Netty) for [beta]: applies ")
                    .contains("does NOT apply -", "reactor.netty.http.client.*", "reactor.netty.connection.provider.*")
                assertThat(engineLines[2])
                    .startsWith("HTTP engine 'jdk' (JdkClientHttpConnector over java.net.http.HttpClient) for [alpha, gamma]: applies ")
                    .contains("does NOT apply max-connections=100", "transport meters none")
            } finally {
                logger.detachAppender(lines)
            }
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private fun newFactory(
        engine: WebClientEngine,
        backend: String,
        baseUrl: String = wireMock.baseUrl(),
        timeLimiter: Duration = GENEROUS_BUDGET,
        bulkheads: BulkheadRegistry = BulkheadRegistry.ofDefaults(),
        circuitBreakers: CircuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults(),
        maxInMemorySize: DataSize = WebClientProperties().maxInMemorySize,
    ): ResilientWebClientFactory =
        newFactory(
            WebClientProperties.Engine(default = engine),
            listOf(backend),
            baseUrl,
            timeLimiter,
            bulkheads,
            circuitBreakers,
            maxInMemorySize,
        )

    /**
     * A factory for [backends] on [engine], against [baseUrl]. Each backend's triplet is `localhost`, so its Host
     * header is `localhost` whatever the port of the base URL.
     */
    private fun newFactory(
        engine: WebClientProperties.Engine,
        backends: List<String>,
        baseUrl: String = wireMock.baseUrl(),
        timeLimiter: Duration = GENEROUS_BUDGET,
        bulkheads: BulkheadRegistry = BulkheadRegistry.ofDefaults(),
        circuitBreakers: CircuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults(),
        maxInMemorySize: DataSize = WebClientProperties().maxInMemorySize,
        // Deliberately no Spring context here: a bare builder stands in for Boot's bean unless a case passes one
        // carrying what Boot's customizers would have put there.
        webClientBuilder: WebClient.Builder = WebClient.builder(),
        observationEnabled: Boolean = false,
    ): ResilientWebClientFactory =
        ResilientWebClientFactory(
            WebClientProperties(
                maxInMemorySize = maxInMemorySize,
                engine = engine,
                observability = WebClientProperties.Observability(httpObservationEnabled = observationEnabled),
            ),
            meshFor(backends, baseUrl),
            ReactiveResilienceDecorator(
                circuitBreakers,
                TimeLimiterRegistry.of(TimeLimiterConfig.custom().timeoutDuration(timeLimiter).build()),
                bulkheads,
            ),
            objectMapper,
            webClientBuilder = webClientBuilder,
        ).also { factories += it }

    private fun meshFor(
        backends: List<String>,
        baseUrl: String,
    ) = MeshProperties(
        defaults = MeshProperties.Defaults(individualRequestTimeoutMs = ANCHOR_MS),
        endpoints =
            backends.map { backend ->
                MeshProperties.EndpointConfig(triplet = "/endpoint/localhost/GET/$backend", alias = backend, baseUrl = baseUrl)
            },
    )

    private fun gzip(text: String): ByteArray = ByteArrayOutputStream().also { bytes -> GZIPOutputStream(bytes).use { it.write(text.toByteArray()) } }.toByteArray()

    /** Counts down once the first observation stops - the bounded wait for an observation another thread ends. */
    private class StopLatch : ObservationHandler<Observation.Context> {
        val latch = CountDownLatch(1)

        override fun supportsContext(context: Observation.Context): Boolean = true

        override fun onStop(context: Observation.Context) = latch.countDown()
    }

    /** Captures events appended from any thread; Logback's `ListAppender` is not safe for that. */
    private class ConcurrentAppender : AppenderBase<ILoggingEvent>() {
        val events = CopyOnWriteArrayList<ILoggingEvent>()

        override fun append(eventObject: ILoggingEvent) {
            events += eventObject
        }
    }

    /**
     * A backend that accepts a connection, reads the request and never answers - so the only thing that can end
     * the exchange is the client. It records when the client closed the connection, which is what an abort
     * looks like on the wire for HTTP/1.1: the connection of an exchange in flight cannot be reused.
     *
     * WireMock cannot say this: it reports requests, not the moment a client hangs up on a delayed answer.
     */
    private class SilentBackend : AutoCloseable {
        private val server = ServerSocket(0, 50, InetAddress.getLoopbackAddress())
        val baseUrl: String = "http://${server.inetAddress.hostAddress}:${server.localPort}"
        val requestArrived = CountDownLatch(1)
        val connectionClosed = CountDownLatch(1)

        @Volatile
        var closedAtNanos: Long = 0L

        init {
            thread(isDaemon = true, name = "silent-backend-acceptor") {
                try {
                    while (true) {
                        val socket = server.accept()
                        thread(isDaemon = true, name = "silent-backend-connection") { hold(socket) }
                    }
                } catch (_: IOException) {
                    // The server socket was closed - the test is over.
                }
            }
        }

        private fun hold(socket: Socket) =
            socket.use { connection ->
                val buffer = ByteArray(8192)
                try {
                    while (connection.getInputStream().read(buffer) >= 0) requestArrived.countDown()
                } catch (_: IOException) {
                    // A reset is a close as well.
                }
                closedAtNanos = System.nanoTime()
                connectionClosed.countDown()
            }

        override fun close() = server.close()
    }
}
