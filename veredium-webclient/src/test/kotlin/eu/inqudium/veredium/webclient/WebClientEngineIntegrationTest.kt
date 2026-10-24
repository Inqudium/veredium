package eu.inqudium.veredium.webclient

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
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
import eu.inqudium.veredium.testsupport.AnsweringBackend
import eu.inqudium.veredium.testsupport.CapturingAppender
import eu.inqudium.veredium.testsupport.CustomizedRegistries
import eu.inqudium.veredium.testsupport.DrippingBackend
import eu.inqudium.veredium.testsupport.Greeting
import eu.inqudium.veredium.testsupport.LookupRequest
import eu.inqudium.veredium.testsupport.SilentBackend
import eu.inqudium.veredium.testsupport.UnansweredConnectBackend
import eu.inqudium.veredium.testsupport.WireMockArrivals
import eu.inqudium.veredium.testsupport.capturing
import eu.inqudium.veredium.testsupport.causeNames
import eu.inqudium.veredium.testsupport.eventually
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
import reactor.core.publisher.Hooks
import reactor.core.publisher.Mono
import reactor.core.publisher.Operators
import reactor.test.scheduler.VirtualTimeScheduler
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.json.JsonMapper
import java.io.ByteArrayOutputStream
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.locks.LockSupport
import java.util.function.Consumer
import java.util.stream.Stream
import java.util.zip.GZIPOutputStream

/**
 * The client contract on EVERY engine of `app.webclient.engine`, end to end against a real backend: what a
 * caller gets for a success, an error status, a slow backend, an oversized or empty body, and what a
 * TimeLimiter abort does to the exchange - plus the per-backend override, the lifecycle of the engine
 * resources, their release against the shutdown deadline (a drain that lets in-flight and queued calls
 * finish, an exchange that never ends, an interrupted shutdown thread), the pool limits each engine applies,
 * the client's handling of errors Reactor drops, and the startup lines.
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

        /** Reactor's per-subscription onErrorDropped key, as a caller would put a hook of its own there. */
        private const val ON_ERROR_DROPPED_KEY = "reactor.onErrorDropped.local"

        private lateinit var wireMock: WireMockServer

        /** The class's one request listener - registered when the server starts, cleared before every test. */
        private val arrivals = WireMockArrivals()

        @JvmStatic
        @BeforeAll
        fun startWireMock() {
            wireMock =
                WireMockServer(wireMockConfig().dynamicPort())
                    .apply { start() }
                    .also { server -> server.addMockServiceRequestListener(arrivals) }
        }

        @JvmStatic
        @AfterAll
        fun stopWireMock() {
            wireMock.stop()
        }

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
        arrivals.clear()
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
        fun `should fail a 302 with a WebClientResponseException the breaker records as a failure`(engine: WebClientEngine) {
            // What is tested: a redirect answer on each engine, through the real pipeline and the shipped verdicts -
            //   a 302 whose body would even decode into the requested type.
            // Success criteria: the call fails with a WebClientResponseException carrying 302; the Location was not
            //   followed (WireMock saw no request for it); the breaker recorded one failure and no success.
            // Why it matters: no engine follows a redirect, so a 3xx is never the answer the adapter asked for.
            //   Handed to the decoder, as retrieve() alone does with a status that is not an error, it would come back
            //   as a value decoded from a redirect page - or as an EmptyResponseBodyException - and the breaker would
            //   count a sidecar that redirects every call as a healthy backend.
            //
            // Given
            wireMock.stubFor(
                get(urlEqualTo("/moved")).willReturn(okJson("""{"message":"moved"}""").withStatus(302).withHeader("Location", "/elsewhere")),
            )
            wireMock.stubFor(get(urlEqualTo("/elsewhere")).willReturn(okJson("""{"message":"elsewhere"}""")))
            val circuitBreakers = CircuitBreakerRegistry.ofDefaults()
            val factory = newFactory(engine, "moved", circuitBreakers = circuitBreakers)
            CustomizedRegistries.circuitBreakers(circuitBreakers, meshFor(listOf("moved"), wireMock.baseUrl()), WebClientVerdicts.breakerVerdicts)
            val client = factory.create("moved")

            // When
            val failure = catchThrowable { client.get(Greeting::class.java) { it.uri("/moved") }.block() }

            // Then
            assertThat(failure).isInstanceOfSatisfying(WebClientResponseException::class.java) { error ->
                assertThat(error.statusCode.value()).isEqualTo(302)
            }
            wireMock.verify(0, getRequestedFor(urlEqualTo("/elsewhere")))
            val breaker = circuitBreakers.circuitBreaker("moved")
            assertThat(breaker.metrics.numberOfFailedCalls).isEqualTo(1)
            assertThat(breaker.metrics.numberOfSuccessfulCalls).isZero()
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(WebClientEngine::class)
        fun `should fail each backend at its own response timeout, before the backend answers`(engine: WebClientEngine) {
            // What is tested: that the per-endpoint response timeout of the mesh ladder reaches each engine - per
            //   BACKEND: two backends on one engine, whose client or pool they share, with different anchors.
            // Success criteria: against a backend 3 s late and a 30 s budget, each call fails with a
            //   WebClientRequestException (the transport's verdict, not the TimeLimiter's TimeoutException), no
            //   earlier than its OWN response timeout (900 ms for the 500 ms anchor, 1400 ms for the 1000 ms one)
            //   and before the OTHER backend's larger timeout would have fired (quick) or the backend answers
            //   (slow), each less 50 ms. Each window is measured after a warm-up call of the same backend, so it
            //   holds the timeout and not the engine's first-exchange class loading. The windows are as wide as
            //   telling the two timeouts apart allows - a loaded runner only ever makes a timeout late.
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
                val upper = timeouts.values.filter { it > responseTimeout }.minOrNull() ?: Duration.ofMillis(3_000)
                assertThat(elapsed).`as`(backend).isGreaterThanOrEqualTo(responseTimeout.minusMillis(50)).isLessThan(upper.minusMillis(50))
            }
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(WebClientEngine::class)
        fun `should refuse a dripping body past the codec limit at once, dropping the connection`(engine: WebClientEngine) {
            // What is tested: what the codec's refusal of an over-limit body does to the exchange on each engine when
            //   the rest of the body is still on its way - the body-phase cancellation (Jetty: AbortJettyOnBodyCancel).
            // Success criteria: against a backend that sends the head and 4 KiB of a declared 1 MiB body at once and
            //   then one byte every 200 ms, a 1 KiB limit and a generous budget, the call fails with
            //   DataBufferLimitException as its root cause within 2 s, and the backend sees its connection dropped
            //   within 2 s - except on the JDK client, which has no way to abort an exchange once its head arrived:
            //   cancelling its body subscription only unregisters the subscriber, the client reads the rest. For it
            //   the case pins that the connection is NOT dropped, so a JDK that changes this turns it red.
            // Why it matters: the codec cancels the body when it refuses it. An engine whose exchange outlived that
            //   cancellation would keep the connection - and its pool slot - for as long as the peer drips, up to
            //   the response timeout. A body that arrives in one piece cannot tell the difference; only one still
            //   arriving can.
            //
            // Given
            DrippingBackend().use { backend ->
                val factory =
                    newFactory(
                        WebClientProperties.Engine(default = engine),
                        listOf("flooded"),
                        maxInMemorySize = DataSize.ofKilobytes(1),
                        mesh = meshFor(listOf("flooded"), backend.baseUrl, anchors = mapOf("flooded" to LONG_ANCHOR_MS)),
                    )
                val client = factory.create("flooded")

                // When
                val started = System.nanoTime()
                val failure = catchThrowable { client.get(Greeting::class.java) { it.uri("/flood") }.block(Duration.ofSeconds(10)) }
                val elapsed = Duration.ofNanos(System.nanoTime() - started)

                // Then
                assertThat(failure).hasRootCauseInstanceOf(DataBufferLimitException::class.java)
                assertThat(elapsed).`as`("the refusal must not wait for the rest of the body").isLessThan(Duration.ofSeconds(2))
                if (engine == WebClientEngine.JDK) {
                    assertThat(backend.connectionDropped.await(500, TimeUnit.MILLISECONDS))
                        .`as`("the JDK client reads a cancelled body on (see the startup line); a failure here means it now drops the connection")
                        .isFalse()
                } else {
                    assertThat(backend.connectionDropped.await(2, TimeUnit.SECONDS))
                        .`as`("the %s engine must drop the connection once the codec refused the body", engine)
                        .isTrue()
                }
            }
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(WebClientEngine::class)
        fun `should drop the connection when the budget expires while the body is still arriving`(engine: WebClientEngine) {
            // What is tested: the TimeLimiter's cancellation in the BODY phase on each engine - the head arrived, the
            //   body drips, nothing on the client refuses it.
            // Success criteria: against the dripping backend, a 1 s budget and the default codec limit, the call
            //   fails with a TimeoutException within 2.5 s, and the backend sees its connection dropped within 2 s -
            //   except on the JDK client, which reads a cancelled body on (see the codec case above); for it the case
            //   pins that the connection is NOT dropped.
            // Why it matters: the body is the phase no per-read timeout bounds as a whole - a peer that drips passes
            //   every one - so the TimeLimiter's cancellation is what ends it, and only if it reaches the engine's
            //   exchange does the connection come back instead of being held for as long as the peer sends.
            //
            // Given
            DrippingBackend().use { backend ->
                val factory =
                    newFactory(
                        WebClientProperties.Engine(default = engine),
                        listOf("dripping"),
                        timeLimiter = Duration.ofSeconds(1),
                        mesh = meshFor(listOf("dripping"), backend.baseUrl, anchors = mapOf("dripping" to LONG_ANCHOR_MS)),
                    )
                val client = factory.create("dripping")

                // When
                val started = System.nanoTime()
                val failure = catchThrowable { client.get(Greeting::class.java) { it.uri("/drip") }.block(Duration.ofSeconds(10)) }
                val elapsed = Duration.ofNanos(System.nanoTime() - started)

                // Then
                assertThat(failure).hasCauseExactlyInstanceOf(TimeoutException::class.java)
                assertThat(elapsed).isLessThan(Duration.ofMillis(2_500))
                if (engine == WebClientEngine.JDK) {
                    assertThat(backend.connectionDropped.await(500, TimeUnit.MILLISECONDS))
                        .`as`("the JDK client reads a cancelled body on (see the startup line); a failure here means it now drops the connection")
                        .isFalse()
                } else {
                    assertThat(backend.connectionDropped.await(2, TimeUnit.SECONDS))
                        .`as`("the %s engine must drop the connection once the TimeLimiter cancelled the body", engine)
                        .isTrue()
                }
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
            CustomizedRegistries.circuitBreakers(circuitBreakers, meshFor(listOf("big"), wireMock.baseUrl()), WebClientVerdicts.breakerVerdicts)
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
            val clientLog = LoggerFactory.getLogger(ResilientWebClient::class.java) as Logger
            capturing(reactorLog) { dropped ->
                capturing(clientLog, Level.DEBUG) { clientLines ->
                    abortAndAssert(engine, dropped, clientLines)
                }
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
            val engines = WebClientEngine.entries.associateBy { it.key }
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
            // What is tested: which engines the factory creates resources for.
            // Success criteria: with two jdk backends and one apache backend, exactly jdk and apache have resources
            //   once the factory exists - before any client is built - and Reactor Netty's pool, the default engine's,
            //   was never created.
            // Why it matters: an engine nobody uses must cost nothing - no pool, no I/O reactor, no threads; an engine
            //   in use comes up with the factory, so a failing one fails the start, not the first call.
            //
            // Given / When
            val factory =
                newFactory(
                    WebClientProperties.Engine(
                        default = WebClientEngine.JDK,
                        endpoints = mapOf("third" to WebClientEngine.APACHE),
                    ),
                    listOf("first", "second", "third"),
                )

            // Then
            assertThat(factory.engineTransports.keys).containsExactlyInAnyOrder(WebClientEngine.JDK, WebClientEngine.APACHE)
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
            //   while opening its own connections, threads and (Apache) I/O reactor per backend, of which shutdown()
            //   would only ever find the last. For reactor-netty the case pins the documented per-backend
            //   sub-pool, the reason max-connections there bounds each backend rather than the engine.
            //
            // Given: two backends on the engine against a backend that keeps its connections open
            AnsweringBackend(body = """{"message":"kept"}""").use { backend ->
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
        fun `should refuse to build a client after shutdown, instead of running it on a released engine`() {
            // What is tested: create() for a backend whose client was not built before shutdown().
            // Success criteria: it fails with an IllegalStateException saying the factory was shut down; a client
            //   built before shutdown is still returned from the cache.
            // Why it matters: a client built after shutdown() released the engines would run on a pool and threads
            //   that are gone - its first call would fail far from the cause.
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
            assertThat(factory.create("early")).isSameAs(early)
        }

        @Test
        fun `should run reactor-netty on the factory's own loops and resolver, not Reactor Netty's global resources`() {
            // What is tested: the isolation of the default engine from Reactor Netty's process-wide HttpResources.
            // Success criteria: the response of a reactor-netty backend is handled on a thread of the factory's
            //   own loop group (named after the pool, `webclient-reactor-netty-...`), not on a `reactor-http-...` thread of
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
            val transport = factory.engineTransports.getValue(WebClientEngine.REACTOR_NETTY) as ReactorNettyEngineTransport
            val endpoint = meshFor(listOf("where"), wireMock.baseUrl()).resolveAll().values.single()
            assertThat(transport.httpClientFor(endpoint).configuration().resolver()).isSameAs(DefaultAddressResolverGroup.INSTANCE)
        }

        @Test
        fun `should dispose the reactor-netty loops after the drain delay, once nothing can still need them`() {
            // What is tested: the deferred disposal of the engine's own event loops, on a virtual clock.
            // Success criteria: shutdown() disposes the pool at once and leaves the loops running; one millisecond
            //   before the 30 s delay passed they are still running, at it they are disposed.
            // Why it matters: closing a loop group closes every channel on it, so disposing it together with the
            //   pool would cut the in-flight calls the pool disposal lets finish
            //   (`ResilientWebClientIntegrationTest` pins that); never disposing it would leak its threads into
            //   the next context of the JVM. The virtual clock makes the "not yet" half a statement about the delay,
            //   not about how quickly this thread got to look.
            //
            // Given: Reactor's default schedulers on a virtual clock, which the transport's delay runs on
            val clock = VirtualTimeScheduler.getOrSet()
            try {
                val bound = Duration.ofSeconds(30)
                val transport =
                    ReactorNettyEngineTransport(WebClientProperties(), MeshProperties.Transport(), "loops-probe", bound) { disposal, _ -> disposal.block() }

                // When
                transport.shutdown(System.nanoTime() + GENEROUS_BUDGET.toNanos(), System::nanoTime)
                clock.advanceTimeBy(bound.minusMillis(1))

                // Then: the pool is gone, the loops are not
                assertThat(transport.isClosed).isTrue()
                assertThat(transport.loops.isDisposed).isFalse()

                // When
                clock.advanceTimeBy(Duration.ofMillis(1))

                // Then
                assertThat(eventually { transport.loops.isDisposed }).`as`("the loops are disposed once the delay passed").isTrue()
            } finally {
                VirtualTimeScheduler.reset()
            }
        }

        @Test
        fun `should bound the reactor-netty disposal by what is left of the shared deadline`() {
            // What is tested: the wait for the Reactor Netty pool's disposal when the engine's turn comes late - the
            //   transport's share of the shared deadline, and the factory's wait bounded by it.
            // Success criteria: released with a deadline already spent, the transport hands its wait a remaining
            //   time of zero; the factory's wait, given 100 ms left and a disposal that never ends, returns within
            //   1 s - not after the full drain bound - with its WARN naming the drain window, as every engine's
            //   does, not the 100 ms that were left of it.
            // Why it matters: the deadline is shared so that releasing several engines takes the drain bound once,
            //   not once per engine. A wait that took the full bound for Reactor Netty would add it again after the
            //   engines before it had spent the deadline. The line states the configured window an operator can
            //   look up, whichever engine's turn came last.
            //
            // Given
            val logger = LoggerFactory.getLogger(ResilientWebClientFactory::class.java) as Logger
            capturing(logger) { lines ->
                val recorded = mutableListOf<Duration>()
                val transport =
                    ReactorNettyEngineTransport(WebClientProperties(), MeshProperties.Transport(), "left-probe", Duration.ofMillis(50)) { disposal, left ->
                        recorded += left
                        disposal.block()
                    }
                val factory = newFactory(WebClientEngine.REACTOR_NETTY, "late")

                // When
                transport.shutdown(System.nanoTime() - 1, System::nanoTime)
                val started = System.nanoTime()
                factory.awaitPoolDisposal(Mono.never(), Duration.ofMillis(100))
                val waited = Duration.ofNanos(System.nanoTime() - started)

                // Then
                assertThat(recorded).containsExactly(Duration.ZERO)
                assertThat(waited).isLessThan(Duration.ofSeconds(1))
                assertThat(lines.events.filter { it.level == Level.WARN }.map { it.formattedMessage })
                    .singleElement()
                    .asString()
                    .contains("'reactor-netty' did not drain within ${factory.poolDisposeTimeout}")
                    .doesNotContain("PT0.1S")
            }
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
            val transport = factory.engineTransports.getValue(engine)
            assertThat(transport.isClosed).isFalse()

            // When
            factory.shutdown()
            val second = catchThrowable { factory.shutdown() }

            // Then
            assertThat(transport.isClosed).isTrue()
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
            // Success criteria: shutdown() returns within 2 s; the transport reports itself released; the held
            //   exchange's connection is closed and its CALL fails at once (not at its 30 s budget); and the
            //   factory's logger has a WARN for the engine - "did not drain within"
            //   for every engine, each of which waits for the exchange up to the deadline; and no ERROR is logged,
            //   neither by Apache nor by the factory (a
            //   reactor closed hard while a worker is still shutting down reports a ClosedSelectorException, which
            //   during the shutdown is the worker's expected end).
            // Why it matters: the bound is the whole point of a draining shutdown - a context close must not wait
            //   for an exchange the backend never ends, and the caller of that exchange must not be left waiting
            //   for its budget either (Apache's hard close completes no callback of an exchange in flight). A deadline already spent by an engine released before
            //   this one must not turn into "wait forever" either (Apache reads a wait of 0 as forever in places).
            //
            // Given: a call held by a backend that never answers, the factory's log captured
            val logger = LoggerFactory.getLogger(ResilientWebClientFactory::class.java) as Logger
            val engineLog = LoggerFactory.getLogger("org.apache.hc.client5.http.impl.async") as Logger
            capturing(logger) { lines ->
                capturing(engineLog) { engineLines ->
                    SilentBackend().use { backend ->
                        val factory =
                            newFactory(
                                WebClientProperties.Engine(default = engine),
                                listOf("held"),
                                mesh = meshFor(listOf("held"), backend.baseUrl, anchors = mapOf("held" to LONG_ANCHOR_MS)),
                            )
                        val held = factory.create("held").get(Greeting::class.java) { it.uri("/held") }.toFuture()
                        assertThat(backend.requestArrived.await(5, TimeUnit.SECONDS)).`as`("the request reached the backend").isTrue()
                        val transport = factory.engineTransports.getValue(engine)

                        // When
                        val started = System.nanoTime()
                        transport.shutdown(started + TimeUnit.MILLISECONDS.toNanos(deadlineMillis), System::nanoTime)
                        val elapsed = Duration.ofNanos(System.nanoTime() - started)

                        // Then
                        assertThat(elapsed).isLessThan(Duration.ofSeconds(2))
                        val warnings = lines.events.filter { it.level == Level.WARN }.map { it.formattedMessage }
                        val drained = "did not drain within"
                        assertThat(warnings).anySatisfy { assertThat(it).contains("'$engine'", drained) }
                        assertThat(transport.isClosed).isTrue()
                        assertThat(backend.connectionClosed.await(2, TimeUnit.SECONDS)).`as`("the held connection was closed").isTrue()
                        assertThat(catchThrowable { held.get(2, TimeUnit.SECONDS) }).isNotNull().isNotInstanceOf(TimeoutException::class.java)
                        // The hard close does not join the reactor's workers: a ClosedSelectorException arrives after
                        // shutdown() returned, if at all. Wait for the workers to end, so "no ERROR" is a statement.
                        assertThat(eventually { Thread.getAllStackTraces().keys.none { it.name.startsWith(ApacheEngineTransport.THREAD_NAME_PREFIX) } })
                            .`as`("the Apache engine's I/O threads must end")
                            .isTrue()
                        assertThat(engineLines.events.filter { it.level == Level.ERROR }.map { it.throwableProxy?.className }).isEmpty()
                        assertThat(lines.events.filter { it.level == Level.ERROR }.map { it.formattedMessage }).isEmpty()
                    }
                }
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
            // Success criteria: the call SUCCEEDS with the backend's answer; shutdown() waited for it (at least
            //   100 ms) and then closed the engine; the transport reports itself released; the factory logs no WARN.
            // Why it matters: a graceful shutdown must not fail the calls still in flight on one engine while the
            //   others let them finish - Jetty's stop and Apache's reactor shutdown each end what is still running,
            //   so without a wait of the transport's own they would cut a call that had time left.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/finishing")).willReturn(okJson("""{"message":"finished"}""").withFixedDelay(500)))
            val arrived = arrivals.arrivalOf("/finishing")
            val logger = LoggerFactory.getLogger(ResilientWebClientFactory::class.java) as Logger
            capturing(logger) { lines ->
                val factory = newFactory(engine, "draining")
                val call = factory.create("draining").get(Greeting::class.java) { it.uri("/finishing") }.toFuture()
                assertThat(arrived.await(5, TimeUnit.SECONDS)).`as`("the request reached the backend").isTrue()
                val transport = factory.engineTransports.getValue(engine)

                // When
                val started = System.nanoTime()
                transport.shutdown(started + TimeUnit.SECONDS.toNanos(5), System::nanoTime)
                val waited = Duration.ofNanos(System.nanoTime() - started)

                // Then
                assertThat(call.get(2, TimeUnit.SECONDS)?.message).isEqualTo("finished")
                assertThat(waited).`as`("shutdown() must have waited for the exchange").isGreaterThan(Duration.ofMillis(100))
                assertThat(transport.isClosed).isTrue()
                assertThat(lines.events.filter { it.level == Level.WARN }.map { it.formattedMessage }).isEmpty()
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
            val arrived = arrivals.arrivalOf("/queued")
            val logger = LoggerFactory.getLogger(ResilientWebClientFactory::class.java) as Logger
            capturing(logger) { lines ->
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
                val transport = factory.engineTransports.getValue(engine)

                // When
                transport.shutdown(System.nanoTime() + TimeUnit.SECONDS.toNanos(5), System::nanoTime)

                // Then
                assertThat(first.get(2, TimeUnit.SECONDS)?.message).isEqualTo("served")
                assertThat(second.get(2, TimeUnit.SECONDS)?.message).isEqualTo("served")
                assertThat(transport.isClosed).isTrue()
                assertThat(lines.events.filter { it.level == Level.WARN }.map { it.formattedMessage }).isEmpty()
            }
        }
    }

    @Nested
    inner class `Interrupted shutdown` {
        @ParameterizedTest(name = "{0}")
        @EnumSource(WebClientEngine::class)
        fun `should stop waiting when the shutdown thread is interrupted and leave the flag set`(engine: WebClientEngine) {
            // What is tested: the interrupt exit of every engine's release - the JDK client's awaitTermination,
            //   Jetty's and Apache's drain, the wait for Reactor Netty's pool disposal - against an exchange that
            //   never ends and a deadline far off, and how the interrupted release leaves the caller of that exchange.
            // Success criteria: shutdown() on an interrupted thread returns within 2 s without throwing, the flag is
            //   still set, the WARN names the engine and the interrupt, and the engine is released shortly after
            //   (Jetty's stop runs on a thread of its own). The held call FAILS and its connection is closed: on
            //   jdk, jetty and apache by the release, long before the 10.4 s response timeout; on reactor-netty,
            //   whose pool disposal keeps serving it, at its own 1.4 s response timeout (a ReadTimeoutException)
            //   and not earlier - and there the engine's event loops are disposed one drain window later, the
            //   loop thread that carried the exchange ended.
            // Why it matters: a container tearing the context down under its own deadline interrupts the
            //   shutdown thread. A release that ignored the interrupt would hold the shutdown for the whole
            //   window; Jetty's stop waits for its own threads regardless of it. A release the interrupt threw
            //   out of would skip what follows the wait: Reactor Netty's loops would never be disposed and leak
            //   their threads into the next context of the JVM, and the log would carry a stack trace instead of
            //   the drain line.
            //
            // Given: a call held by a backend that never answers - on reactor-netty with the short anchor, so its
            // response timeout and the drain window (2.7 s) both pass within the case - and a filter recording the
            // thread the exchange's failure arrives on
            val reactorNetty = engine == WebClientEngine.REACTOR_NETTY
            val anchor = if (reactorNetty) ANCHOR_MS else LONG_ANCHOR_MS
            val failedOn = CopyOnWriteArrayList<Thread>()
            val builder =
                WebClient.builder().filter { request, next ->
                    next.exchange(request).doOnError { failedOn += Thread.currentThread() }
                }
            val logger = LoggerFactory.getLogger(ResilientWebClientFactory::class.java) as Logger
            capturing(logger) { lines ->
                SilentBackend().use { backend ->
                    val factory =
                        newFactory(
                            WebClientProperties.Engine(default = engine),
                            listOf("held"),
                            webClientBuilder = builder,
                            mesh = meshFor(listOf("held"), backend.baseUrl, anchors = mapOf("held" to anchor)),
                        )
                    val sent = System.nanoTime()
                    val held = factory.create("held").get(Greeting::class.java) { it.uri("/held") }.toFuture()
                    val endedAfter = held.handle { _, _ -> Duration.ofNanos(System.nanoTime() - sent) }
                    assertThat(backend.requestArrived.await(5, TimeUnit.SECONDS)).`as`("the request reached the backend").isTrue()
                    val transport = factory.engineTransports.getValue(engine)

                    // When
                    val release =
                        CompletableFuture.supplyAsync(
                            {
                                Thread.currentThread().interrupt()
                                transport.shutdown(System.nanoTime() + TimeUnit.MINUTES.toNanos(1), System::nanoTime)
                                Thread.currentThread().isInterrupted
                            },
                            { task -> Thread.ofVirtual().start(task) },
                        )

                    // Then: the release
                    assertThat(release.get(2, TimeUnit.SECONDS)).`as`("the interrupt flag must survive shutdown()").isTrue()
                    assertThat(lines.events.filter { it.level == Level.WARN }.map { it.formattedMessage })
                        .anySatisfy { assertThat(it).contains("'$engine'", "because the shutdown thread was interrupted") }
                    assertThat(eventually { transport.isClosed }).`as`("the %s engine is released", engine).isTrue()

                    // Then: the caller
                    val failure = catchThrowable { held.get(5, TimeUnit.SECONDS) }
                    assertThat(failure).`as`("the held call must fail, not hang").isNotNull().isNotInstanceOf(TimeoutException::class.java)
                    assertThat(backend.connectionClosed.await(2, TimeUnit.SECONDS)).`as`("the held connection was closed").isTrue()
                    if (reactorNetty) {
                        assertThat(causeNames(failure)).contains("ReadTimeoutException")
                        assertThat(endedAfter.get()).`as`("served until its response timeout").isGreaterThanOrEqualTo(RESPONSE_TIMEOUT)
                        val loops = (transport as ReactorNettyEngineTransport).loops
                        assertThat(eventually(Duration.ofSeconds(10)) { loops.isDisposed }).`as`("the event loops are disposed").isTrue()
                        assertThat(failedOn).isNotEmpty()
                        assertThat(eventually { failedOn.none { it.isAlive } }).`as`("the loop thread of the exchange has ended: %s", failedOn).isTrue()
                    } else {
                        assertThat(endedAfter.get()).`as`("ended by the release").isLessThan(Duration.ofSeconds(5))
                    }
                }
            }
        }
    }

    @Nested
    inner class `Pool limits on every engine` {
        @ParameterizedTest(name = "{0}")
        @EnumSource(WebClientEngine::class)
        fun `should replace a connection idle past max-idle-time where the engine applies it`(engine: WebClientEngine) {
            // What is tested: pool.max-idle-time (300 ms) with pool.evict-in-background (100 ms) on each engine,
            //   against a backend that answers and counts its connections.
            // Success criteria: a second call 800 ms after the first runs on a NEW connection on reactor-netty,
            //   jetty and apache; on the jdk client, whose startup line says the setting does NOT apply, it reuses
            //   the first one.
            // Why it matters: the startup line says "applies pool.max-idle-time" for three engines. An engine that
            //   silently ran on its library's default (Reactor Netty: no limit, Jetty: 30 s, Apache: no evictor)
            //   would keep connections the sidecar may already have closed, and nothing but this case would say so.
            //
            // Given
            AnsweringBackend().use { backend ->
                val client =
                    newFactory(
                        WebClientProperties.Engine(default = engine),
                        listOf("idle"),
                        mesh = meshFor(listOf("idle"), backend.baseUrl),
                        pool = WebClientProperties.Pool(maxIdleTime = Duration.ofMillis(300), evictInBackground = Duration.ofMillis(100)),
                    ).create("idle")
                client.get(Greeting::class.java) { it.uri("/first") }.block(Duration.ofSeconds(5))

                // When: the idle time passes - nothing to wait ON, the pool acts on its own clock
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(800))
                client.get(Greeting::class.java) { it.uri("/second") }.block(Duration.ofSeconds(5))

                // Then
                assertThat(backend.connections.get())
                    .`as`("connections the %s engine opened for two calls 800 ms apart", engine)
                    .isEqualTo(if (engine == WebClientEngine.JDK) 1 else 2)
            }
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(WebClientEngine::class)
        fun `should replace a connection older than max-life-time where the engine applies it`(engine: WebClientEngine) {
            // What is tested: pool.max-life-time (300 ms) on each engine, with an idle time far longer, against a
            //   backend that answers and counts its connections.
            // Success criteria: a second call 800 ms after the first runs on a NEW connection on reactor-netty,
            //   jetty (whose pool checks the duration when it leases) and apache; on the jdk client, whose startup
            //   line says the setting does NOT apply, it reuses the first one.
            // Why it matters: the lifetime bound is what spreads connections over the sidecar's restarts and DNS
            //   changes; a pool on the library's default (unbounded on every engine) would never recycle one, while
            //   the startup line says "applies pool.max-life-time".
            //
            // Given
            AnsweringBackend().use { backend ->
                val client =
                    newFactory(
                        WebClientProperties.Engine(default = engine),
                        listOf("aging"),
                        mesh = meshFor(listOf("aging"), backend.baseUrl),
                        pool =
                            WebClientProperties.Pool(
                                maxIdleTime = Duration.ofSeconds(30),
                                maxLifeTime = Duration.ofMillis(300),
                                evictInBackground = Duration.ofMillis(100),
                            ),
                    ).create("aging")
                client.get(Greeting::class.java) { it.uri("/first") }.block(Duration.ofSeconds(5))

                // When: the lifetime passes - nothing to wait ON, the pool acts on its own clock
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(800))
                client.get(Greeting::class.java) { it.uri("/second") }.block(Duration.ofSeconds(5))

                // Then
                assertThat(backend.connections.get())
                    .`as`("connections the %s engine opened for two calls 800 ms apart", engine)
                    .isEqualTo(if (engine == WebClientEngine.JDK) 1 else 2)
            }
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(value = WebClientEngine::class, names = ["REACTOR_NETTY", "JETTY"])
        fun `should reject a call beyond pending-acquire-max-count the way the engine's startup line says`(engine: WebClientEngine) {
            // What is tested: pool.pending-acquire-max-count (1) with max-connections 1 on the two engines that apply
            //   it, against a backend that never answers - one call holding the connection, the rest waiting.
            // Success criteria: reactor-netty lets ONE call wait and rejects the next at once; jetty, whose request
            //   queue holds max-connections + the count (jettyQueueCapacity), lets TWO wait and rejects the next at
            //   once - each rejection well inside the case's 1 s, so it is the limit, not a timeout.
            // Why it matters: the queue bound is what sheds a burst before it piles up behind a stalled backend. An
            //   engine on its library's default (Reactor Netty 2 x max-connections, Jetty 1024) would let it pile
            //   up, while the startup line says "applies pool.pending-acquire-max-count=1".
            //
            // Given
            SilentBackend().use { backend ->
                val factory =
                    newFactory(
                        WebClientProperties.Engine(default = engine),
                        listOf("queued"),
                        mesh =
                            meshFor(
                                listOf("queued"),
                                backend.baseUrl,
                                MeshProperties.Transport(maxConnections = 1, pendingAcquireTimeoutMs = 5_000),
                                anchors = mapOf("queued" to LONG_ANCHOR_MS),
                            ),
                        pool = WebClientProperties.Pool(pendingAcquireMaxCount = 1),
                    )
                val client = factory.create("queued")
                val waiting = if (engine == WebClientEngine.JETTY) 2 else 1
                // The holder first, and the others only once it is ON its connection: on a cold pool Jetty keeps the
                // holder in its queue until the connection is open, so a burst would meet the queue's cold-pool
                // capacity (max-connections + the count) instead of the count behind a busy connection.
                val held = mutableListOf(client.get(Greeting::class.java) { it.uri("/held") }.toFuture())
                try {
                    assertThat(backend.requestArrived.await(5, TimeUnit.SECONDS)).`as`("the first call holds the connection").isTrue()

                    // When: one call more than may wait - in whatever order they reach the pool
                    val started = System.nanoTime()
                    repeat(waiting + 1) { held += client.get(Greeting::class.java) { it.uri("/held") }.toFuture() }
                    val behind = held.drop(1)

                    // Then: exactly one is rejected at once, by the limit, not by a timeout; the others wait
                    assertThat(eventually(Duration.ofMillis(800)) { behind.count { it.isDone } == 1 })
                        .`as`("one call beyond the limit must be rejected at once")
                        .isTrue()
                    val elapsed = Duration.ofNanos(System.nanoTime() - started)
                    val rejected = behind.single { it.isDone }
                    assertThat(causeNames(catchThrowable { rejected.get() })).doesNotContain("TimeoutException")
                    assertThat(elapsed).`as`("the limit rejects at once").isLessThan(Duration.ofMillis(800))
                    assertThat(behind.count { !it.isDone }).`as`("calls waiting behind the busy connection").isEqualTo(waiting)
                    assertThat(held.first()).isNotDone()
                } finally {
                    held.forEach { it.cancel(true) }
                }
            }
        }

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
            //   TimeLimiter's TimeoutException) carrying the engine's own connect-timeout exception, not before
            //   the 100 ms (less 50 ms of timer slack) and within 2 s, under a 30 s budget and a 10.4 s response
            //   timeout; on CI the case may not skip itself.
            // Why it matters: the connect timeout is a rung of the ladder - the TimeLimiter margin is computed
            //   from it. Without it an engine waits on its library's default (Reactor Netty 30 s, Jetty 15 s,
            //   Apache 3 min) and every call to an unreachable sidecar spends the whole budget instead.
            //
            // Given: a socket that accepts no further connection, a client against it
            UnansweredConnectBackend().use { backend ->
                // Skipped where the kernel refuses instead of dropping - but never silently on CI, where the case
                // must run: a skip there would leave the connect timeout of every engine unchecked.
                if (System.getenv("CI") == "true") {
                    assertThat(backend.saturated).`as`("the CI runner must be able to hold a connect open").isTrue()
                }
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
                assertThat(causeNames(failure).any { it == "ConnectTimeoutException" || it == "HttpConnectTimeoutException" } || failure.cause?.message == "Connect Timeout")
                    .`as`("a connect timeout, not another failure: %s", failure)
                    .isTrue()
                assertThat(elapsed).isGreaterThanOrEqualTo(Duration.ofMillis(50)).isLessThan(Duration.ofSeconds(2))
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
            Hooks.onErrorDropped { global += it }
            try {
                capturing(clientLog, Level.DEBUG) { clientLines ->
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
                }
            } finally {
                Hooks.resetOnErrorDropped()
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
            capturing(logger) { lines ->
                // When
                newFactory(
                    WebClientProperties.Engine(endpoints = mapOf("alpha" to WebClientEngine.JDK, "gamma" to WebClientEngine.JDK)),
                    listOf("gamma", "beta", "alpha"),
                )

                // Then
                val engineLines = lines.events.filter { it.level == Level.INFO && it.formattedMessage.startsWith("HTTP engine") }.map { it.formattedMessage }
                assertThat(engineLines).hasSize(3)
                assertThat(engineLines[0])
                    .isEqualTo("HTTP engine per backend (app.webclient.engine, default reactor-netty): alpha=jdk, beta=reactor-netty, gamma=jdk")
                assertThat(engineLines[1])
                    .startsWith("HTTP engine 'reactor-netty' (ReactorClientHttpConnector over Reactor Netty) for [beta]: applies ")
                    .contains("does NOT apply -", "reactor.netty.http.client.*", "reactor.netty.connection.provider.*")
                assertThat(engineLines[2])
                    .startsWith("HTTP engine 'jdk' (JdkClientHttpConnector over java.net.http.HttpClient) for [alpha, gamma]: applies ")
                    .contains("does NOT apply max-connections=100", "transport meters none")
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
        pool: WebClientProperties.Pool = WebClientProperties.Pool(),
    ): ResilientWebClientFactory =
        ResilientWebClientFactory(
            WebClientProperties(
                maxInMemorySize = maxInMemorySize,
                engine = engine,
                pool = pool,
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

    /** The body of the TimeLimiter-abort case, run while Reactor's log and the client's DEBUG lines are captured. */
    private fun abortAndAssert(
        engine: WebClientEngine,
        dropped: CapturingAppender,
        clientLines: CapturingAppender,
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
            runCatching { runBlocking { factory.create("warm").get(Greeting::class.java) { it.uri("/warm") }.awaitSingle() } }
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

    private fun gzip(text: String): ByteArray = ByteArrayOutputStream().also { bytes -> GZIPOutputStream(bytes).use { it.write(text.toByteArray()) } }.toByteArray()

    /** Counts down once the first observation stops - the bounded wait for an observation another thread ends. */
    private class StopLatch : ObservationHandler<Observation.Context> {
        val latch = CountDownLatch(1)

        override fun supportsContext(context: Observation.Context): Boolean = true

        override fun onStop(context: Observation.Context) = latch.countDown()
    }
}
