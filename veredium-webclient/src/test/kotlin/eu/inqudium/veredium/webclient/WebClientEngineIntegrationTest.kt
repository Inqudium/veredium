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
import org.junit.jupiter.api.Assumptions.assumeTrue
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
import reactor.core.publisher.Hooks
import reactor.core.publisher.Mono
import reactor.core.publisher.Operators
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.json.JsonMapper
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Consumer
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

        /**
         * An anchor whose response timeout (10.4 s) outlasts every case that uses it, so a held exchange is ended
         * by the case - a TimeLimiter, a cancel, a release - and never by the transport's own timeout.
         */
        private const val LONG_ANCHOR_MS = 10_000L

        /** The engines that report a failure of the exchange AFTER the TimeLimiter's abort, every time (client class KDoc). */
        private val REPORTS_FAILURE_AFTER_ABORT = setOf(WebClientEngine.JETTY, WebClientEngine.APACHE)

        /**
         * The engine that reports such a late failure only SOMETIMES: cancelling the JDK client's future completes
         * it at once, and whether the exchange's own failure follows depends on whether the JDK had already failed
         * it - a race, decided differently on a loaded machine.
         */
        private val MAY_REPORT_FAILURE_AFTER_ABORT = setOf(WebClientEngine.JDK)

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

        /** Reactor's per-subscription onErrorDropped key, as a caller would put a hook of its own there. */
        private const val ON_ERROR_DROPPED_KEY = "reactor.onErrorDropped.local"

        /**
         * The engines whose release, at the deadline, closes what is still running - Reactor Netty's pool lets an
         * exchange that outlives its graceful disposal run on, until the loops go.
         */
        @JvmStatic
        fun drainingEngines(): Stream<WebClientEngine> = Stream.of(WebClientEngine.JDK, WebClientEngine.JETTY, WebClientEngine.APACHE)

        /** [drainingEngines], each with a deadline ahead and one passed. */
        @JvmStatic
        fun drainingEnginesAndDeadlines(): Stream<Arguments> = drainingEngines().flatMap { engine -> Stream.of(Arguments.of(engine, 300L), Arguments.of(engine, -1L)) }

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
        fun `should fail each backend at its own response timeout, before the backend answers`(engine: WebClientEngine) {
            // What is tested: that the per-endpoint response timeout of the mesh ladder reaches each engine - per
            //   BACKEND: two backends on one engine, whose client or pool they share, with different anchors.
            // Success criteria: against a backend 3 s late and a 30 s budget, each call fails with a
            //   WebClientRequestException (the transport's verdict, not the TimeLimiter's TimeoutException), no
            //   earlier than its OWN response timeout (900 ms for the 500 ms anchor, 1400 ms for the 1000 ms one)
            //   and before its ladder's TimeLimiter rung 400 ms above it (connect 100 + pending-acquire 200 + 100
            //   headroom). Each window is measured after a warm-up call of the same backend, so it holds the
            //   timeout and not the engine's first-exchange class loading.
            // Why it matters: the response timeout is the ladder's transport guard - tighter than the budget, so
            //   the breaker records a transport failure rather than a budget overrun. The JDK, Jetty and Apache
            //   engines share ONE client between their backends and carry the timeout per connector; one that kept
            //   the first backend's value, or the largest, would hand a backend another backend's rung.
            //
            // Given: two backends on the engine, each warmed up once
            wireMock.stubFor(get(urlEqualTo("/late")).willReturn(okJson("""{"message":"late"}""").withFixedDelay(3_000)))
            wireMock.stubFor(get(urlEqualTo("/warm")).willReturn(okJson("""{"message":"warm"}""")))
            val timeouts = mapOf("quick" to Duration.ofMillis(900), "slow" to RESPONSE_TIMEOUT)
            val backends = timeouts.keys.toList()
            val factory =
                newFactory(
                    WebClientProperties.Engine(default = engine),
                    backends,
                    mesh = meshFor(backends, wireMock.baseUrl(), anchors = mapOf("quick" to 500L)),
                )
            backends.forEach { backend -> factory.create(backend).get(Greeting::class.java) { it.uri("/warm") }.block() }

            timeouts.forEach { (backend, responseTimeout) ->
                val client = factory.create(backend)
                assertThat(client.responseTimeout).isEqualTo(responseTimeout)

                // When
                val started = System.nanoTime()
                val failure = catchThrowable { client.get(Greeting::class.java) { it.uri("/late") }.block() }
                val elapsed = Duration.ofNanos(System.nanoTime() - started)

                // Then
                assertThat(failure).`as`(backend).isInstanceOf(WebClientRequestException::class.java)
                assertThat(elapsed).`as`(backend).isGreaterThanOrEqualTo(responseTimeout.minusMillis(50)).isLessThan(responseTimeout.plusMillis(400))
            }
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
            //   response timeout could have done it; and on the engines that always report a failure after the
            //   abort (Jetty, Apache), the client's DEBUG line "ended after cancellation" for the backend arrives
            //   (bounded wait) while Reactor logs no "onErrorDropped" ERROR - checked only AFTER that line, so the
            //   check cannot pass before the engine's late failure was even reported. The JDK client reports one
            //   only sometimes; its line is waited for (bounded) but not required, and no ERROR is allowed either.
            // Why it matters: cancellation is how the budget frees what a stalled call holds. An engine that only
            //   drops the subscriber keeps the connection - and with it a pool slot - until the response timeout,
            //   so under a stalled backend the pool fills with exchanges nobody waits for. Spring's Apache
            //   connector is such a case on its own; this pins that the module's abort bridge closes the gap, and
            //   that the other engines abort on their own. The ERROR line is pinned because these engines report
            //   the aborted exchange as a failure while the cancellation is still travelling - Reactor would log
            //   every TimeLimiter timeout on them as an unhandled error; waiting for the DEBUG line also pins the
            //   Reactor hook key the client relies on (its Assumption).
            //
            // Given: a backend that never answers, a 300 ms budget, a one-permit bulkhead, Reactor's log and the
            // client's DEBUG lines captured
            val reactorLog = LoggerFactory.getLogger("reactor.core.publisher.Operators") as Logger
            val dropped = ConcurrentAppender().apply { start() }
            val clientLog = LoggerFactory.getLogger(ResilientWebClient::class.java) as Logger
            val clientLines = ConcurrentAppender().apply { start() }
            val clientLevel = clientLog.level
            reactorLog.addAppender(dropped)
            clientLog.addAppender(clientLines)
            clientLog.level = Level.DEBUG
            try {
                abortAndAssert(engine, dropped, clientLines)
            } finally {
                reactorLog.detachAppender(dropped)
                clientLog.detachAppender(clientLines)
                clientLog.level = clientLevel
            }
        }

        private fun abortAndAssert(
            engine: WebClientEngine,
            dropped: ConcurrentAppender,
            clientLines: ConcurrentAppender,
        ) {
            SilentBackend().use { backend ->
                val bulkheads = BulkheadRegistry.of(BulkheadConfig.custom().maxConcurrentCalls(1).build())
                // Two backends on the engine: `warm` (WireMock) makes the engine's first exchange - its start, the
                // class loading, the codec setup - before the 300 ms budget of `stalled` is measured; on a loaded
                // machine the first exchange alone can take longer than that, and the budget would expire before
                // the request reached the backend.
                val stalledMesh = meshFor(listOf("stalled"), backend.baseUrl)
                val mesh = stalledMesh.copy(endpoints = stalledMesh.endpoints + meshFor(listOf("warm"), wireMock.baseUrl()).endpoints)
                val factory =
                    newFactory(
                        WebClientProperties.Engine(default = engine),
                        listOf("stalled", "warm"),
                        timeLimiter = Duration.ofMillis(300),
                        bulkheads = bulkheads,
                        mesh = mesh,
                    )
                wireMock.stubFor(get(urlEqualTo("/warm")).willReturn(okJson("""{"message":"warm"}""")))
                // Its outcome does not matter - under load it may itself exceed the shared 300 ms budget; the engine is
                // initialised either way.
                catchThrowable { runBlocking { factory.create("warm").get(Greeting::class.java) { it.uri("/warm") }.awaitSingle() } }
                val client = factory.create("stalled")

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
                if (engine in REPORTS_FAILURE_AFTER_ABORT || engine in MAY_REPORT_FAILURE_AFTER_ABORT) {
                    // Bounded wait for the late failure, so the ERROR check below does not pass before it came.
                    val lateFailure =
                        eventually {
                            clientLines.events.any { it.level == Level.DEBUG && it.formattedMessage.startsWith("Exchange of backend 'stalled' ended after cancellation") }
                        }
                    if (engine in REPORTS_FAILURE_AFTER_ABORT) {
                        assertThat(lateFailure).`as`("the engine's failure after the abort was written at DEBUG").isTrue()
                    }
                }
                assertThat(dropped.events.filter { it.level == Level.ERROR }.map { it.formattedMessage }).isEmpty()
            }
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
        fun `should create only the engines in use`() {
            // What is tested: lazy creation of the engine resources.
            // Success criteria: before any client exists, no engine has resources; after two jdk backends and one
            //   apache backend built their clients, exactly jdk and apache have resources, and Reactor Netty's
            //   pool, the default engine's, was never created.
            // Why it matters: an engine nobody uses must cost nothing - no pool, no I/O reactor, no threads.
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

        @ParameterizedTest(name = "{0}")
        @EnumSource(WebClientEngine::class)
        fun `should share the engine's pool between its backends, except for reactor-netty's sub-pool per backend`(engine: WebClientEngine) {
            // What is tested: which connections the backends of one engine share - observed on the wire, not read
            //   off the factory's map of transports, which cannot hold two per engine by construction.
            // Success criteria: against a keep-alive backend, a call of one backend and then a call of another
            //   backend of the same engine arrive over ONE connection on jdk, jetty and apache (one client, one
            //   pool); on reactor-netty over TWO - its provider keys sub-pools by remote address AND the client's
            //   configuration, and each backend's HttpClient carries its own response timeout and URI tagger.
            // Why it matters: a connector built on a client of its own per backend would still answer every call -
            //   while opening its own connections, threads and (Apache) I/O reactor per backend, of which release()
            //   would only ever find the last. For reactor-netty the case pins the documented per-backend
            //   sub-pool, the reason max-connections there bounds each backend rather than the engine.
            //
            // Given: two backends on the engine against a backend that keeps its connections open
            KeepAliveBackend().use { backend ->
                val backends = listOf("first", "second")
                val factory =
                    newFactory(WebClientProperties.Engine(default = engine), backends, mesh = meshFor(backends, backend.baseUrl))

                // When
                backends.forEach { name -> factory.create(name).get(Greeting::class.java) { it.uri("/shared") }.block() }

                // Then
                assertThat(backend.requests.get()).isEqualTo(2)
                val expected = if (engine == WebClientEngine.REACTOR_NETTY) 2 else 1
                assertThat(backend.connections.get()).`as`("connections opened for two backends' calls").isEqualTo(expected)
            }
        }

        @Test
        fun `should refuse to build a client after shutdown, instead of creating an engine nothing releases`() {
            // What is tested: create() for a backend whose client was not built before shutdown().
            // Success criteria: it fails with an IllegalStateException saying the factory was shut down, and no
            //   engine was created by it; a client built before shutdown is still returned from the cache.
            // Why it matters: engines are created lazily, by the first client; one created after shutdown()
            //   released the others would own threads and sockets that nothing releases any more.
            //
            // Given
            val factory =
                newFactory(
                    WebClientProperties.Engine(endpoints = mapOf("late" to WebClientEngine.JDK)),
                    listOf("early", "late"),
                )
            val early = factory.create("early")
            factory.shutdown()

            // When
            val failure = catchThrowable { factory.create("late") }

            // Then
            assertThat(failure).isInstanceOf(IllegalStateException::class.java).hasMessageContaining("shut down")
            assertThat(factory.startedEngines().keys).containsExactly(WebClientEngine.REACTOR_NETTY)
            assertThat(factory.create("early")).isSameAs(early)
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
                ReactorNettyTransport(WebClientProperties(), MeshProperties.Transport(), "loops-probe", Duration.ofMillis(50)) { disposal, _ -> disposal.block() }

            // When
            transport.release(System.nanoTime() + GENEROUS_BUDGET.toNanos(), System::nanoTime)

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
    inner class `Release against the shutdown deadline` {
        @ParameterizedTest(name = "{0}, deadline {1} ms from now")
        @MethodSource("eu.inqudium.veredium.webclient.WebClientEngineIntegrationTest#drainingEnginesAndDeadlines")
        fun `should close an engine that does not drain by the deadline, with a WARN`(
            engine: WebClientEngine,
            deadlineMillis: Long,
        ) {
            // What is tested: the release of the engines that WAIT for in-flight exchanges here - the JDK client's
            //   orderly shutdown, and Jetty's and Apache's wait on their exchanges in flight before they stop -
            //   against an exchange that never ends, with a deadline 300 ms ahead and one that has already passed.
            // Success criteria: release() returns within 2 s; the transport reports itself released; the held
            //   exchange's connection is closed and its CALL fails at once (not at its 30 s budget); and the
            //   factory's logger has a WARN for the engine - "did not drain by the factory's shutdown deadline"
            //   for every engine, each of which waits for the exchange up to the deadline; and no ERROR is logged,
            //   neither by Apache nor by the factory (a
            //   reactor closed hard while a worker is still shutting down reports a ClosedSelectorException, which
            //   during the release is the worker's expected end).
            // Why it matters: the bound is the whole point of a draining shutdown - a context close must not wait
            //   for an exchange the backend never ends, and the caller of that exchange must not be left waiting
            //   for its budget either (Apache's hard close completes no callback of an exchange in flight). A deadline already spent by an engine released before
            //   this one must not turn into "wait forever" either (Apache reads a wait of 0 as forever in places).
            //
            // Given: a call held by a backend that never answers, the factory's log captured
            val logger = LoggerFactory.getLogger(ResilientWebClientFactory::class.java) as Logger
            val lines = ConcurrentAppender().apply { start() }
            val engineLog = LoggerFactory.getLogger("org.apache.hc.client5.http.impl.async") as Logger
            val engineLines = ConcurrentAppender().apply { start() }
            logger.addAppender(lines)
            engineLog.addAppender(engineLines)
            try {
                SilentBackend().use { backend ->
                    val factory =
                        newFactory(
                            WebClientProperties.Engine(default = engine),
                            listOf("held"),
                            mesh = meshFor(listOf("held"), backend.baseUrl, anchors = mapOf("held" to LONG_ANCHOR_MS)),
                        )
                    val held = factory.create("held").get(Greeting::class.java) { it.uri("/held") }.toFuture()
                    assertThat(backend.requestArrived.await(5, TimeUnit.SECONDS)).`as`("the request reached the backend").isTrue()
                    val transport = factory.startedEngines().getValue(engine)

                    // When
                    val started = System.nanoTime()
                    transport.release(started + TimeUnit.MILLISECONDS.toNanos(deadlineMillis), System::nanoTime)
                    val elapsed = Duration.ofNanos(System.nanoTime() - started)

                    // Then
                    assertThat(elapsed).isLessThan(Duration.ofSeconds(2))
                    val warnings = lines.events.filter { it.level == Level.WARN }.map { it.formattedMessage }
                    val drained = "did not drain by the factory's shutdown deadline"
                    assertThat(warnings).anySatisfy { assertThat(it).contains("'$engine'", drained) }
                    assertThat(transport.isReleased()).isTrue()
                    assertThat(backend.connectionClosed.await(2, TimeUnit.SECONDS)).`as`("the held connection was closed").isTrue()
                    assertThat(catchThrowable { held.get(2, TimeUnit.SECONDS) }).isNotNull().isNotInstanceOf(TimeoutException::class.java)
                    assertThat(engineLines.events.filter { it.level == Level.ERROR }.map { it.throwableProxy?.className }).isEmpty()
                    assertThat(lines.events.filter { it.level == Level.ERROR }.map { it.formattedMessage }).isEmpty()
                }
            } finally {
                logger.detachAppender(lines)
                engineLog.detachAppender(engineLines)
            }
        }
    }

    @Nested
    inner class `Draining shutdown` {
        @ParameterizedTest(name = "{0}")
        @EnumSource(WebClientEngine::class)
        fun `should let an in-flight exchange finish before closing the engine`(engine: WebClientEngine) {
            // What is tested: the drain of every engine's release - an exchange still on the wire when the release
            //   starts, answered 500 ms later, well before the deadline.
            // Success criteria: the call SUCCEEDS with the backend's answer; release() waited for it (at least
            //   100 ms) and then closed the engine; the transport reports itself released; the factory logs no WARN.
            // Why it matters: a graceful shutdown must not fail the calls still in flight on one engine while the
            //   others let them finish - Jetty's stop and Apache's reactor shutdown each end what is still running,
            //   so without a wait of the transport's own they would cut a call that had time left.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/finishing")).willReturn(okJson("""{"message":"finished"}""").withFixedDelay(500)))
            val arrived = CountDownLatch(1)
            wireMock.addMockServiceRequestListener { request, _ -> if (request.url == "/finishing") arrived.countDown() }
            val logger = LoggerFactory.getLogger(ResilientWebClientFactory::class.java) as Logger
            val lines = ConcurrentAppender().apply { start() }
            logger.addAppender(lines)
            try {
                val factory = newFactory(engine, "draining")
                val call = factory.create("draining").get(Greeting::class.java) { it.uri("/finishing") }.toFuture()
                assertThat(arrived.await(5, TimeUnit.SECONDS)).`as`("the request reached the backend").isTrue()
                val transport = factory.startedEngines().getValue(engine)

                // When
                val started = System.nanoTime()
                transport.release(started + TimeUnit.SECONDS.toNanos(5), System::nanoTime)
                val waited = Duration.ofNanos(System.nanoTime() - started)

                // Then
                assertThat(call.get(2, TimeUnit.SECONDS)?.message).isEqualTo("finished")
                assertThat(waited).`as`("release() must have waited for the exchange").isGreaterThan(Duration.ofMillis(100))
                assertThat(transport.isReleased()).isTrue()
                assertThat(lines.events.filter { it.level == Level.WARN }.map { it.formattedMessage }).isEmpty()
            } finally {
                logger.detachAppender(lines)
            }
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(WebClientEngine::class)
        fun `should let a call still waiting for a connection finish before closing the engine`(engine: WebClientEngine) {
            // What is tested: the drain of every engine's release, for a call that is not yet on a
            //   connection when the release starts - max-connections 1, one call holding the connection for
            //   500 ms, a second call of the same backend waiting for it.
            // Success criteria: BOTH calls succeed with the backend's answer; the transport reports itself
            //   released; the factory logs no WARN.
            // Why it matters: a call queued for a connection is in flight for its caller as much as one on the
            //   wire. An engine that stops serving connections the moment its release starts (Apache's reactor in
            //   SHUTTING_DOWN opens none, Reactor Netty's pool disposed without a disposeTimeout fails its pending
            //   acquires) would fail it, although it had all the time of the deadline left.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/queued")).willReturn(okJson("""{"message":"served"}""").withFixedDelay(500)))
            val arrived = CountDownLatch(1)
            wireMock.addMockServiceRequestListener { request, _ -> if (request.url == "/queued") arrived.countDown() }
            val logger = LoggerFactory.getLogger(ResilientWebClientFactory::class.java) as Logger
            val lines = ConcurrentAppender().apply { start() }
            logger.addAppender(lines)
            try {
                val factory =
                    newFactory(
                        WebClientProperties.Engine(default = engine),
                        listOf("queued"),
                        mesh =
                            meshFor(
                                listOf("queued"),
                                wireMock.baseUrl(),
                                MeshProperties.Transport(maxConnections = 1, pendingAcquireTimeoutMs = 3_000),
                                anchors = mapOf("queued" to LONG_ANCHOR_MS),
                            ),
                    )
                val client = factory.create("queued")
                val first = client.get(Greeting::class.java) { it.uri("/queued") }.toFuture()
                assertThat(arrived.await(5, TimeUnit.SECONDS)).`as`("the first request reached the backend").isTrue()
                val second = client.get(Greeting::class.java) { it.uri("/queued") }.toFuture()
                val transport = factory.startedEngines().getValue(engine)

                // When
                transport.release(System.nanoTime() + TimeUnit.SECONDS.toNanos(5), System::nanoTime)

                // Then
                assertThat(first.get(2, TimeUnit.SECONDS)?.message).isEqualTo("served")
                assertThat(second.get(2, TimeUnit.SECONDS)?.message).isEqualTo("served")
                assertThat(transport.isReleased()).isTrue()
                assertThat(lines.events.filter { it.level == Level.WARN }.map { it.formattedMessage }).isEmpty()
            } finally {
                logger.detachAppender(lines)
            }
        }
    }

    @Nested
    inner class `Jetty's shutdown` {
        @Test
        fun `should stop waiting when the shutdown thread is interrupted and leave the flag set`() {
            // What is tested: the interrupt exit of the Jetty engine's drain, against an exchange that never ends
            //   and a deadline far off.
            // Success criteria: release() on an interrupted thread returns within 2 s, the flag is still set, the
            //   WARN names the interrupt, and the client is stopped shortly after (its stop runs on a thread of
            //   its own), which closes the held connection.
            // Why it matters: a container tearing the context down under its own deadline interrupts the
            //   shutdown thread. Jetty's stop waits for its own threads regardless of the interrupt; run on the
            //   shutdown thread it would hold the shutdown anyway.
            //
            // Given
            val logger = LoggerFactory.getLogger(ResilientWebClientFactory::class.java) as Logger
            val lines = ConcurrentAppender().apply { start() }
            logger.addAppender(lines)
            try {
                SilentBackend().use { backend ->
                    val factory =
                        newFactory(
                            WebClientProperties.Engine(default = WebClientEngine.JETTY),
                            listOf("held"),
                            mesh = meshFor(listOf("held"), backend.baseUrl, anchors = mapOf("held" to LONG_ANCHOR_MS)),
                        )
                    val held = factory.create("held").get(Greeting::class.java) { it.uri("/held") }.toFuture()
                    assertThat(backend.requestArrived.await(5, TimeUnit.SECONDS)).`as`("the request reached the backend").isTrue()
                    val transport = factory.startedEngines().getValue(WebClientEngine.JETTY)

                    // When
                    val release =
                        CompletableFuture.supplyAsync(
                            {
                                Thread.currentThread().interrupt()
                                transport.release(System.nanoTime() + TimeUnit.MINUTES.toNanos(1), System::nanoTime)
                                Thread.currentThread().isInterrupted
                            },
                            { task -> Thread.ofVirtual().start(task) },
                        )

                    // Then
                    assertThat(release.get(2, TimeUnit.SECONDS)).`as`("the interrupt flag must survive release()").isTrue()
                    assertThat(lines.events.filter { it.level == Level.WARN }.map { it.formattedMessage })
                        .anySatisfy { assertThat(it).contains("'jetty'", "because the shutdown thread was interrupted") }
                    assertThat(eventually { transport.isReleased() }).`as`("the client stops on its own thread").isTrue()
                    assertThat(backend.connectionClosed.await(2, TimeUnit.SECONDS)).`as`("the held connection was closed").isTrue()
                    assertThat(catchThrowable { held.get(2, TimeUnit.SECONDS) }).isNotNull().isNotInstanceOf(TimeoutException::class.java)
                }
            } finally {
                logger.detachAppender(lines)
            }
        }
    }

    @Nested
    inner class `Pool limits on every engine` {
        @ParameterizedTest(name = "{0}")
        @EnumSource(WebClientEngine::class)
        fun `should bound a call beyond max-connections the way the engine's startup line says`(engine: WebClientEngine) {
            // What is tested: transport.max-connections, and what bounds the wait for a connection, on each
            //   engine - with max-connections 1, one call of a backend holding the only connection and a second
            //   call of the same backend needing one.
            // Success criteria: reactor-netty fails the second call at its pending-acquire timeout (a
            //   PoolAcquireTimeoutException in the cause chain, well before the case's 1 s bound); jetty and apache
            //   keep it waiting until the case's own 1 s timeout ends it - jetty in its queue, apache in a lease
            //   whose connection-request timeout its async pool checks only when the pool is next used - all three
            //   without opening a second connection; the jdk client, whose line says max-connections does NOT
            //   apply, opens a second connection for it.
            // Why it matters: the bulkheads are sized against this limit (sum of permits <= max-connections). An
            //   engine that silently ran on its library's default pool would let one stalled backend take every
            //   connection the sidecar allows, while the startup line says "applies max-connections=1".
            //
            // Given: a backend that never answers, one connection, the first call holding it
            SilentBackend().use { backend ->
                val factory =
                    newFactory(
                        WebClientProperties.Engine(default = engine),
                        listOf("pooled"),
                        mesh =
                            meshFor(
                                listOf("pooled"),
                                backend.baseUrl,
                                MeshProperties.Transport(maxConnections = 1),
                                anchors = mapOf("pooled" to LONG_ANCHOR_MS),
                            ),
                    )
                val client = factory.create("pooled")
                val holder = client.get(Greeting::class.java) { it.uri("/hold") }.toFuture()
                try {
                    assertThat(backend.requestArrived.await(5, TimeUnit.SECONDS)).`as`("the holder's request reached the backend").isTrue()

                    // When: a second call, bounded by the case at 1 s (its TimeLimiter budget is the generous one)
                    val started = System.nanoTime()
                    val failure =
                        catchThrowable {
                            runBlocking {
                                client.get(Greeting::class.java) { it.uri("/wait") }.timeout(Duration.ofSeconds(1)).awaitSingle()
                            }
                        }
                    val elapsed = Duration.ofNanos(System.nanoTime() - started)

                    // Then
                    when (engine) {
                        WebClientEngine.REACTOR_NETTY -> {
                            assertThat(causeNames(failure)).contains("PoolAcquireTimeoutException")
                            assertThat(elapsed).isLessThan(Duration.ofMillis(800))
                            assertThat(backend.connections.get()).isEqualTo(1)
                        }

                        WebClientEngine.APACHE, WebClientEngine.JETTY -> {
                            assertThat(failure).isExactlyInstanceOf(TimeoutException::class.java)
                            assertThat(backend.connections.get()).isEqualTo(1)
                        }

                        WebClientEngine.JDK -> {
                            assertThat(failure).isExactlyInstanceOf(TimeoutException::class.java)
                            assertThat(eventually { backend.connections.get() == 2 }).`as`("a second connection was opened").isTrue()
                        }
                    }
                } finally {
                    holder.cancel(true)
                }
            }
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(WebClientEngine::class)
        fun `should fail a connect the backend never accepts at the connect timeout`(engine: WebClientEngine) {
            // What is tested: transport.connect-timeout-ms (100 ms) on each engine, against a listening socket
            //   whose accept backlog is full, so the kernel leaves the connect unanswered.
            // Success criteria: the call fails as a transport failure (a WebClientRequestException, not the
            //   TimeLimiter's TimeoutException) within 2 s, under a 30 s budget and a 10.4 s response timeout.
            // Why it matters: the connect timeout is a rung of the ladder - the TimeLimiter margin is computed
            //   from it. Without it an engine waits on its library's default (Reactor Netty 30 s, Jetty 15 s,
            //   Apache 3 min) and every call to an unreachable sidecar spends the whole budget instead.
            //
            // Given: a socket that accepts no further connection, a client against it
            UnansweredConnectBackend().use { backend ->
                assumeTrue(backend.saturated, "the kernel answered every connect to the full backlog - no way to hold a connect open here")
                val client =
                    newFactory(
                        WebClientProperties.Engine(default = engine),
                        listOf("unreachable"),
                        mesh = meshFor(listOf("unreachable"), backend.baseUrl, anchors = mapOf("unreachable" to LONG_ANCHOR_MS)),
                    ).create("unreachable")

                // When
                val started = System.nanoTime()
                val failure = catchThrowable { runBlocking { client.get(Greeting::class.java) { it.uri("/never") }.awaitSingle() } }
                val elapsed = Duration.ofNanos(System.nanoTime() - started)

                // Then
                assertThat(failure).isInstanceOf(WebClientRequestException::class.java)
                assertThat(elapsed).isLessThan(Duration.ofSeconds(2))
            }
        }
    }

    @Nested
    inner class `Dropped errors` {
        @Test
        fun `should leave an error dropped while the exchange was not cancelled to Reactor's own handling`() {
            // What is tested: the client's per-subscription onErrorDropped hook, for an error dropped by an
            //   operator although nothing was cancelled - here a response publisher that signals an error after
            //   it completed, the kind of anomaly the hook must not hide.
            // Success criteria: the dropped error reaches Reactor's global onErrorDropped hook, and the client
            //   writes no "ended after cancellation" line for it; with a hook of the caller's own in the
            //   subscription's context, that hook receives it instead.
            // Why it matters: the hook exists to quieten ONE expected case - an engine's failure after the
            //   TimeLimiter aborted the exchange. Taking every dropped error to DEBUG would make a genuine operator
            //   bug invisible at the level it is logged at everywhere else.
            //
            // Given: a builder whose filter completes the response and then signals a late error
            wireMock.stubFor(get(urlEqualTo("/late-error")).willReturn(okJson("""{"message":"done"}""")))
            val builder =
                WebClient.builder().filter { request, next ->
                    Mono.from { subscriber ->
                        subscriber.onSubscribe(Operators.emptySubscription())
                        next.exchange(request).subscribe(
                            { response ->
                                subscriber.onNext(response)
                                subscriber.onComplete()
                                subscriber.onError(IllegalStateException("late error of a misbehaving publisher"))
                            },
                            subscriber::onError,
                        )
                    }
                }
            val client = newFactory(WebClientProperties.Engine(), listOf("late-error"), webClientBuilder = builder).create("late-error")
            val global = CopyOnWriteArrayList<Throwable>()
            val local = CopyOnWriteArrayList<Throwable>()
            val clientLog = LoggerFactory.getLogger(ResilientWebClient::class.java) as Logger
            val clientLines = ConcurrentAppender().apply { start() }
            val clientLevel = clientLog.level
            clientLog.addAppender(clientLines)
            clientLog.level = Level.DEBUG
            Hooks.onErrorDropped { global += it }
            try {
                // When
                client.get(Greeting::class.java) { it.uri("/late-error") }.block()
                client
                    .get(Greeting::class.java) { it.uri("/late-error") }
                    .contextWrite { it.put(ON_ERROR_DROPPED_KEY, Consumer<Throwable> { error -> local += error }) }
                    .block()

                // Then
                assertThat(global).singleElement().extracting { it.message }.isEqualTo("late error of a misbehaving publisher")
                assertThat(local).singleElement().extracting { it.message }.isEqualTo("late error of a misbehaving publisher")
                assertThat(clientLines.events.map { it.formattedMessage }).noneMatch { it.contains("ended after cancellation") }
            } finally {
                Hooks.resetOnErrorDropped()
                clientLog.detachAppender(clientLines)
                clientLog.level = clientLevel
            }
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
     * header is `localhost` whatever the port of the base URL. A case that tunes the ladder or the transport
     * passes its own [mesh]; one that needs a budget per backend passes its own [timeLimiters].
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
        mesh: MeshProperties = meshFor(backends, baseUrl),
        timeLimiters: TimeLimiterRegistry = TimeLimiterRegistry.of(TimeLimiterConfig.custom().timeoutDuration(timeLimiter).build()),
    ): ResilientWebClientFactory =
        ResilientWebClientFactory(
            WebClientProperties(
                maxInMemorySize = maxInMemorySize,
                engine = engine,
                observability = WebClientProperties.Observability(httpObservationEnabled = observationEnabled),
            ),
            mesh,
            ReactiveResilienceDecorator(circuitBreakers, timeLimiters, bulkheads),
            objectMapper,
            webClientBuilder = webClientBuilder,
        ).also { factories += it }

    /** The mesh of [backends] against [baseUrl], on [transport], with the anchor of [anchors] where a backend has one. */
    private fun meshFor(
        backends: List<String>,
        baseUrl: String,
        transport: MeshProperties.Transport = MeshProperties.Transport(),
        anchors: Map<String, Long> = emptyMap(),
    ) = MeshProperties(
        defaults = MeshProperties.Defaults(individualRequestTimeoutMs = ANCHOR_MS, transport = transport),
        endpoints =
            backends.map { backend ->
                MeshProperties.EndpointConfig(
                    triplet = "/endpoint/localhost/GET/$backend",
                    alias = backend,
                    baseUrl = baseUrl,
                    individualRequestTimeoutMs = anchors[backend],
                )
            },
    )

    /** The simple class names along [failure]'s cause chain - for an engine's exception whose type is shaded. */
    private fun causeNames(failure: Throwable?): List<String> = generateSequence(failure) { it.cause.takeIf { cause -> cause !== it } }.map { it.javaClass.simpleName }.toList()

    /** Polls [condition] every 10 ms until it holds or [within] passed - a bounded wait, never a sleep. */
    private fun eventually(
        within: Duration = Duration.ofSeconds(5),
        condition: () -> Boolean,
    ): Boolean =
        Flux
            .interval(Duration.ZERO, Duration.ofMillis(10))
            .map { condition() }
            .filter { it }
            .take(1)
            .timeout(within, Mono.just(false))
            .blockLast() ?: false

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

        /** The connections the backend accepted so far. */
        val connections = AtomicInteger()

        @Volatile
        var closedAtNanos: Long = 0L

        init {
            thread(isDaemon = true, name = "silent-backend-acceptor") {
                try {
                    while (true) {
                        val socket = server.accept()
                        connections.incrementAndGet()
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

    /**
     * A minimal HTTP/1.1 backend that answers every request with a small JSON body and keeps the connection open
     * for the next one - so the number of connections it accepted says whether two calls shared one.
     *
     * WireMock cannot say this: it reports requests, not the connections they arrived over.
     */
    private class KeepAliveBackend : AutoCloseable {
        private val server = ServerSocket(0, 50, InetAddress.getLoopbackAddress())
        val baseUrl: String = "http://${server.inetAddress.hostAddress}:${server.localPort}"
        val connections = AtomicInteger()
        val requests = AtomicInteger()

        init {
            thread(isDaemon = true, name = "keep-alive-backend-acceptor") {
                try {
                    while (true) {
                        val socket = server.accept()
                        connections.incrementAndGet()
                        thread(isDaemon = true, name = "keep-alive-backend-connection") { serve(socket) }
                    }
                } catch (_: IOException) {
                    // The server socket was closed - the test is over.
                }
            }
        }

        private fun serve(socket: Socket) =
            socket.use { connection ->
                val input = connection.getInputStream().buffered()
                val output = connection.getOutputStream()
                val body = """{"message":"kept"}""".toByteArray()
                try {
                    // One request per round: its head ends with an empty line; the GETs here carry no body.
                    while (readHead(input)) {
                        requests.incrementAndGet()
                        output.write(
                            (
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: ${body.size}\r\n" +
                                    "Connection: keep-alive\r\n\r\n"
                            ).toByteArray(),
                        )
                        output.write(body)
                        output.flush()
                    }
                } catch (_: IOException) {
                    // The client closed the connection.
                }
            }

        /** Reads one request head; `false` at the end of the stream. */
        private fun readHead(input: java.io.InputStream): Boolean {
            var matched = 0
            val terminator = "\r\n\r\n"
            while (matched < terminator.length) {
                val next = input.read()
                if (next < 0) return false
                matched =
                    if (next.toChar() == terminator[matched]) {
                        matched + 1
                    } else if (next.toChar() == '\r') {
                        1
                    } else {
                        0
                    }
            }
            return true
        }

        override fun close() = server.close()
    }

    /**
     * A listening socket that accepts no connection, its accept backlog filled by probe connects - so the kernel
     * leaves any further connect unanswered (Linux drops the SYN of a full accept queue), and only the client's
     * connect timeout can end it. [saturated] says whether a probe connect actually went unanswered; on a
     * platform that refuses instead, the case using it is skipped rather than asserting on a refusal.
     */
    private class UnansweredConnectBackend : AutoCloseable {
        private val server = ServerSocket(0, 1, InetAddress.getLoopbackAddress())
        val baseUrl: String = "http://${server.inetAddress.hostAddress}:${server.localPort}"
        private val probes = mutableListOf<Socket>()
        val saturated: Boolean = fill()

        private fun fill(): Boolean {
            repeat(MAX_PROBES) {
                val probe = Socket()
                try {
                    probe.connect(InetSocketAddress(server.inetAddress, server.localPort), PROBE_TIMEOUT_MS)
                    probes += probe
                } catch (_: SocketTimeoutException) {
                    probe.close()
                    return true
                } catch (_: IOException) {
                    probe.close()
                    return false
                }
            }
            return false
        }

        override fun close() {
            probes.forEach { it.close() }
            server.close()
        }

        private companion object {
            const val MAX_PROBES = 16
            const val PROBE_TIMEOUT_MS = 300
        }
    }
}
