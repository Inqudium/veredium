package eu.inqudium.veredium.restclient

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.equalToJson
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.okJson
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import eu.inqudium.veredium.core.Veredium
import eu.inqudium.veredium.core.mesh.MeshConnectionPool
import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.resilience.BlockingResilienceDecorator
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.netty.handler.timeout.ReadTimeoutException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory
import org.springframework.http.client.JdkClientHttpRequestFactory
import org.springframework.util.unit.DataSize
import org.springframework.web.client.HttpClientErrorException
import org.springframework.web.client.HttpServerErrorException
import org.springframework.web.client.ResourceAccessException
import org.springframework.web.client.RestClient
import reactor.core.publisher.Mono
import reactor.netty.http.HttpResources
import tools.jackson.databind.json.JsonMapper
import java.io.IOException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketTimeoutException
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.LockSupport
import kotlin.concurrent.thread

/**
 * The selectable HTTP engines (`app.restclient.engine`), each driven end to end against a real (WireMock) backend
 * through [ResilientRestClientFactory] and the real [BlockingResilienceDecorator].
 *
 * Every behaviour the module promised before the engine became selectable is run once per engine - the happy
 * paths, the status mapping, the response timeout, the buffer limit, the Host header - and so is the decorator's
 * contract that an abandoned call releases its worker and its connection promptly, which each engine reaches by
 * a different route. Then what is specific to the selection: per-backend overrides, engines created only when
 * selected and shared by their backends, closed on shutdown, the fail-fast checks and the startup lines.
 *
 * Every endpoint's triplet host is `localhost:<port>` - the authority the JDK client sends for the WireMock base
 * URL - so the JDK engine, which cannot send any other Host, runs the shared cases too; the Host cases with a
 * foreign host are split by engine.
 */
class RestClientEngineIntegrationTest {
    companion object {
        private lateinit var wireMock: WireMockServer

        /**
         * What a request to a URL triggers once it reached WireMock, per test ([arrivalOf]) - behind ONE listener for
         * the class, registered with the server: `resetAll()` does not remove a request listener, so one added per
         * test would pile up and fire for a later test's request to the same URL. Cleared before every test.
         */
        private val arrivals = ConcurrentHashMap<String, () -> Unit>()

        /** A latch that opens once a request to [url] reached WireMock in the current test. */
        private fun arrivalOf(url: String): CountDownLatch = CountDownLatch(1).also { latch -> arrivals[url] = latch::countDown }

        /** The generous budget of every case that is not about timing - see `ResilientRestClientIntegrationTest`. */
        private val GENEROUS_BUDGET: TimeLimiterConfig = TimeLimiterConfig.custom().timeoutDuration(Duration.ofSeconds(30)).build()

        /** How long an abandoned exchange may take to give back its permit and its connection - far below every response timeout it is pitted against. */
        private val PROMPTLY: Duration = Duration.ofSeconds(2)

        /**
         * How much earlier than configured a timeout may appear to fire on the test's clock - the granularity of an
         * engine's timer, not a load effect: load only ever makes a timeout LATE. A lower bound with this slack is
         * what tells one timeout from another, the anchor from the response timeout above it.
         */
        private val TIMER_SLACK: Duration = Duration.ofMillis(50)

        /**
         * Asynchronous responses: a delayed answer waits on WireMock's scheduler instead of a container thread. The
         * abandoned-exchange cases leave delays of several seconds behind them that no client waits for any more -
         * on container threads they would pile up across the class and slow every later request down.
         */
        @JvmStatic
        @BeforeAll
        fun startWireMock() {
            wireMock =
                WireMockServer(
                    wireMockConfig()
                        .dynamicPort()
                        .asynchronousResponseEnabled(true)
                        .asynchronousResponseThreads(16),
                ).apply { start() }
                    .also { server -> server.addMockServiceRequestListener { request, _ -> arrivals[request.url]?.invoke() } }
        }

        @JvmStatic
        @AfterAll
        fun stopWireMock() {
            wireMock.stop()
        }
    }

    private val objectMapper = JsonMapper.builder().build()
    private val factories = mutableListOf<ResilientRestClientFactory>()
    private val decorators = mutableListOf<BlockingResilienceDecorator>()
    private val callers = Executors.newVirtualThreadPerTaskExecutor()

    @BeforeEach
    fun resetStubs() {
        wireMock.resetAll()
        arrivals.clear()
    }

    @AfterEach
    fun tearDown() {
        callers.shutdownNow()
        factories.forEach { it.shutdown() }
        decorators.forEach { it.shutdown() }
    }

    @Nested
    inner class `Every engine` {
        @ParameterizedTest
        @EnumSource(RestClientEngine::class)
        fun `should deserialise a GET response and send the module's headers once each`(engine: RestClientEngine) {
            // What is tested: the happy path of a GET on each engine - the request the engine puts on the wire
            //   and the response it hands to the bounded read.
            // Success criteria: the decoded value carries the backend's message; WireMock saw exactly ONE
            //   User-Agent (Veredium's token), the Accept header and the Host derived from the triplet.
            // Why it matters: the headers are set on the RestClient, above the engine, but whether they reach the
            //   wire as set is the engine's business - Jetty, for one, ADDS a header Spring sets, which would put
            //   its own User-Agent next to ours.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/hello")).willReturn(okJson("""{"message":"hi"}""")))
            val client = newFactory(RestClientProperties.Engine(default = engine)).create("greet")

            // When
            val result = client.get(Greeting::class.java) { it.uri("/hello") }

            // Then
            assertThat(result.message).isEqualTo("hi")
            val request = wireMock.findAll(getRequestedFor(urlEqualTo("/hello"))).single()
            assertThat(request.header("User-Agent").values()).containsExactly(Veredium.userAgent())
            assertThat(request.header("Accept").values()).containsExactly(MediaType.APPLICATION_JSON_VALUE)
            assertThat(request.header("Host").values()).containsExactly(localAuthority())
        }

        @ParameterizedTest
        @EnumSource(RestClientEngine::class)
        fun `should send a POST body once and deserialise the response`(engine: RestClientEngine) {
            // What is tested: the request body path of each engine - Spring streams the body through the
            //   engine's own writer (a body publisher, Jetty's request content, a Netty outbound).
            // Success criteria: WireMock received the serialised body as JSON and the response is decoded.
            // Why it matters: the body writer is the one part of a request each engine implements differently,
            //   and the POST lookups of a host depend on it.
            //
            // Given
            wireMock.stubFor(
                post(urlEqualTo("/lookup"))
                    .withRequestBody(equalToJson("""{"id":"42"}"""))
                    .willReturn(okJson("""{"message":"found"}""")),
            )
            val client = newFactory(RestClientProperties.Engine(default = engine)).create("lookup")

            // When
            val result =
                client.post(Greeting::class.java, LookupRequest("42")) { it.uri("/lookup").contentType(MediaType.APPLICATION_JSON) }

            // Then
            assertThat(result.message).isEqualTo("found")
            wireMock.verify(1, postRequestedFor(urlEqualTo("/lookup")).withRequestBody(equalToJson("""{"id":"42"}""")))
        }

        @ParameterizedTest
        @EnumSource(RestClientEngine::class)
        fun `should map a 404 and a 500 to Spring's status exceptions with their bodies`(engine: RestClientEngine) {
            // What is tested: the status mapping of the exchange function over each engine's response.
            // Success criteria: a 404 arrives as HttpClientErrorException.NotFound and a 500 as
            //   HttpServerErrorException.InternalServerError, each carrying the backend's error body.
            // Why it matters: the breaker verdicts and every caller key on these types; an engine whose
            //   response surfaced a status differently would change what opens a circuit.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/missing")).willReturn(aResponse().withStatus(404).withBody("""{"error":"none"}""")))
            wireMock.stubFor(get(urlEqualTo("/broken")).willReturn(aResponse().withStatus(500).withBody("""{"error":"boom"}""")))
            val client = newFactory(RestClientProperties.Engine(default = engine)).create("status")

            // When
            val notFound = catchThrowable { client.get(Greeting::class.java) { it.uri("/missing") } }
            val serverError = catchThrowable { client.get(Greeting::class.java) { it.uri("/broken") } }

            // Then
            assertThat(notFound).isInstanceOf(HttpClientErrorException.NotFound::class.java)
            assertThat((notFound as HttpClientErrorException).responseBodyAsString).isEqualTo("""{"error":"none"}""")
            assertThat(serverError).isInstanceOf(HttpServerErrorException.InternalServerError::class.java)
            assertThat((serverError as HttpServerErrorException).responseBodyAsString).isEqualTo("""{"error":"boom"}""")
        }

        @ParameterizedTest
        @EnumSource(RestClientEngine::class)
        fun `should fail with the transport's response timeout when it is the tighter bound`(engine: RestClientEngine) {
            // What is tested: the ladder's response-timeout rung on each engine - it is applied by four different
            //   knobs (Apache's socket timeout, the JDK deadline, Jetty's total and idle timeout, Reactor Netty's
            //   response timeout).
            // Success criteria: with a 300 ms anchor (response timeout 700 ms), a generous 30 s budget and a 3 s
            //   backend, the call fails with a ResourceAccessException well before the backend would answer, over
            //   the engine's own timeout type - each one legatium classifies as `timeout`.
            // Why it matters: the rung is what releases the WORKER under normal conditions; an engine that did not
            //   apply it would hold every slow call until the TimeLimiter, with its permit and its connection. The
            //   cause type is what the guide's failure table names per engine.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/stalled")).willReturn(okJson("{}").withFixedDelay(3_000)))
            val client =
                newFactory(RestClientProperties.Engine(default = engine), mesh = mesh("stalled", individualRequestTimeoutMs = 300))
                    .create("stalled")
            assertThat(client.responseTimeout).isEqualTo(Duration.ofMillis(700))

            // When
            val startNanos = System.nanoTime()
            val failure = catchThrowable { client.get(Greeting::class.java) { it.uri("/stalled") } }
            val elapsed = Duration.ofNanos(System.nanoTime() - startNanos)

            // Then
            assertThat(failure).isInstanceOf(ResourceAccessException::class.java)
            // A timeout never fires early, so the lower bound holds on a loaded machine: it is what tells the
            // response timeout from the anchor (300 ms), which an engine wired with the wrong value would use.
            assertThat(elapsed).isGreaterThanOrEqualTo(client.responseTimeout.minus(TIMER_SLACK)).isLessThan(Duration.ofMillis(2_500))
            val timeoutType =
                when (engine) {
                    RestClientEngine.APACHE -> SocketTimeoutException::class.java
                    RestClientEngine.JDK -> HttpTimeoutException::class.java
                    RestClientEngine.JETTY -> TimeoutException::class.java
                    RestClientEngine.REACTOR_NETTY -> ReadTimeoutException::class.java
                }
            assertThat(generateSequence(failure) { it.cause }.toList()).anyMatch { timeoutType.isInstance(it) }
        }

        @ParameterizedTest
        @EnumSource(RestClientEngine::class)
        fun `should refuse a body past the buffer limit and keep serving the next call`(engine: RestClientEngine) {
            // What is tested: the bounded read over each engine's response stream, and what closing the
            //   over-limit response leaves behind.
            // Success criteria: a 2 KB body against a 1 KB limit fails with ResponseBodyLimitExceededException; the
            //   next call on the same client succeeds.
            // Why it matters: the limit is enforced above the engine, but the remainder of the refused body is
            //   the engine's to discard; an engine that left the connection in a broken state would fail the
            //   call after an oversized one.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/huge")).willReturn(okJson("""{"message":"${"x".repeat(2_048)}"}""")))
            wireMock.stubFor(get(urlEqualTo("/small")).willReturn(okJson("""{"message":"ok"}""")))
            val client = newFactory(RestClientProperties.Engine(default = engine), maxInMemorySize = DataSize.ofKilobytes(1)).create("limited")

            // When
            val failure = catchThrowable { client.get(Greeting::class.java) { it.uri("/huge") } }
            val next = client.get(Greeting::class.java) { it.uri("/small") }

            // Then
            assertThat(failure).isInstanceOf(ResponseBodyLimitExceededException::class.java)
            assertThat(next.message).isEqualTo("ok")
        }

        @ParameterizedTest
        @EnumSource(RestClientEngine::class)
        fun `should refuse a dripping body past the buffer limit at once, without reading the rest`(engine: RestClientEngine) {
            // What is tested: what closing an over-limit response does on each engine when the rest of the body
            //   is still on its way - read to the end, or the exchange dropped.
            // Success criteria: against a backend that sends the head and 4 KiB of a declared 1 MiB body at once
            //   and then one byte every 200 ms, a 1 KiB limit and a generous 30 s budget, the call fails with
            //   ResponseBodyLimitExceededException within 2 s; the backend sees its connection dropped within 2 s;
            //   the bulkhead permit and the engine's connection (where it counts them) are back as well.
            // Why it matters: Apache's close READS the rest of a body to keep its connection. For a large or slow
            //   body that holds the worker, its permit and its connection for as long as the peer takes, and past
            //   the budget the client-side refusal - which the breaker ignores - becomes a timeout it records.
            //   A 2 KB body that arrives in one piece cannot tell the difference; only a body still arriving can.
            //
            // Given
            DrippingBackend().use { backend ->
                val bulkheads = BulkheadRegistry.ofDefaults()
                val factory =
                    newFactory(
                        RestClientProperties.Engine(default = engine),
                        mesh = meshAt(backend.authority, "flooded", individualRequestTimeoutMs = 8_000),
                        bulkheads = bulkheads,
                        maxInMemorySize = DataSize.ofKilobytes(1),
                    )
                val client = factory.create("flooded")

                // When
                val (failure, elapsed) = timed { client.get(Greeting::class.java) { it.uri("/flood") } }

                // Then
                assertThat(failure).isInstanceOf(ResponseBodyLimitExceededException::class.java)
                assertThat(elapsed).`as`("the refusal must not wait for the rest of the body").isLessThan(PROMPTLY)
                assertThat(backend.connectionDropped.await(PROMPTLY.toMillis(), TimeUnit.MILLISECONDS))
                    .`as`("the %s engine must drop the connection instead of reading the body to its end", engine)
                    .isTrue()
                awaitReleased(factory, engine, bulkheads, "flooded")
            }
        }

        @ParameterizedTest
        @EnumSource(RestClientEngine::class)
        fun `should fail a body the peer cuts off as a transport failure`(engine: RestClientEngine) {
            // What is tested: a transport failure in the BODY phase on each engine - the head arrived, the peer
            //   closes the connection 4 KiB into a body that declared 1 MiB.
            // Success criteria: the call fails within 2 s with Spring's ResourceAccessException whose cause is an
            //   IOException - the shape every other transport fault has - and the permit and the connection come
            //   back.
            // Why it matters: the body read is where the Reactor Netty engine hands off and unwraps the reader's
            //   failure; an unwrapping that went wrong would hand Spring a checked ExecutionException past its
            //   IOException handling, and the breaker and the exchange log would see a different failure than on
            //   the other engines - the path no other case reaches.
            //
            // Given
            DrippingBackend(cutAfterFirstChunk = true).use { backend ->
                val bulkheads = BulkheadRegistry.ofDefaults()
                val factory =
                    newFactory(
                        RestClientProperties.Engine(default = engine),
                        mesh = meshAt(backend.authority, "cut", individualRequestTimeoutMs = 8_000),
                        bulkheads = bulkheads,
                    )
                val client = factory.create("cut")

                // When
                val (failure, elapsed) = timed { client.get(Greeting::class.java) { it.uri("/cut") } }

                // Then
                assertThat(failure).isExactlyInstanceOf(ResourceAccessException::class.java).hasCauseInstanceOf(IOException::class.java)
                assertThat(elapsed).isLessThan(PROMPTLY)
                awaitReleased(factory, engine, bulkheads, "cut")
            }
        }

        @ParameterizedTest
        @EnumSource(RestClientEngine::class)
        fun `should give each backend on a shared engine its own response timeout`(engine: RestClientEngine) {
            // What is tested: the per-backend response timeout where one engine client (JDK, Jetty, Reactor Netty)
            //   or one pool (Apache) serves both backends and the timeout travels per request factory.
            // Success criteria: against a 4 s backend, `quick` (300 ms anchor, 700 ms response timeout) fails with
            //   a ResourceAccessException in under 1.5 s, `patient` (1500 ms anchor, 1.9 s response timeout) fails
            //   the same way, but not before 1.5 s - each at its own rung, after one warm-up call each.
            // Why it matters: a request factory that carried the first backend's timeout, or the largest one, to
            //   every backend of the engine would put a backend's transport rung above its own TimeLimiter or
            //   below its own anchor - with one backend per test, nothing would show it.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/slow")).willReturn(okJson("{}").withFixedDelay(4_000)))
            wireMock.stubFor(get(urlEqualTo("/warm")).willReturn(okJson("""{"message":"warm"}""")))
            val mesh =
                MeshProperties(
                    defaults = MeshProperties.Defaults(transport = MeshProperties.Transport(sidecarBaseUrl = wireMock.baseUrl())),
                    endpoints =
                        listOf(
                            MeshProperties.EndpointConfig(triplet = "/endpoint/${localAuthority()}/GET/quick", alias = "quick", individualRequestTimeoutMs = 300),
                            MeshProperties.EndpointConfig(triplet = "/endpoint/${localAuthority()}/GET/patient", alias = "patient", individualRequestTimeoutMs = 1_500),
                        ),
                )
            val factory = newFactory(RestClientProperties.Engine(default = engine), mesh = mesh)
            val quick = factory.create("quick")
            val patient = factory.create("patient")
            assertThat(quick.responseTimeout).isEqualTo(Duration.ofMillis(700))
            assertThat(patient.responseTimeout).isEqualTo(Duration.ofMillis(1_900))
            quick.get(Greeting::class.java) { it.uri("/warm") }
            patient.get(Greeting::class.java) { it.uri("/warm") }

            // When
            val (quickFailure, quickElapsed) = timed { quick.get(Greeting::class.java) { it.uri("/slow") } }
            val (patientFailure, patientElapsed) = timed { patient.get(Greeting::class.java) { it.uri("/slow") } }

            // Then
            // Each bounded from below by its own response timeout - a timeout never fires early - so a factory that
            // handed an engine the anchor instead (300 ms / 1500 ms) turns this red.
            assertThat(quickFailure).isInstanceOf(ResourceAccessException::class.java)
            assertThat(quickElapsed).isGreaterThanOrEqualTo(quick.responseTimeout.minus(TIMER_SLACK)).isLessThan(Duration.ofMillis(1_500))
            assertThat(patientFailure).isInstanceOf(ResourceAccessException::class.java)
            assertThat(patientElapsed).isGreaterThanOrEqualTo(patient.responseTimeout.minus(TIMER_SLACK)).isLessThan(Duration.ofMillis(3_500))
        }

        @ParameterizedTest
        @EnumSource(RestClientEngine::class)
        fun `should create only the selected engine, share it, and close it on shutdown`(engine: RestClientEngine) {
            // What is tested: the engine's lifecycle in the factory - created only when selected, one instance
            //   for all backends on it, closed by shutdown().
            // Success criteria: the factory holds exactly one transport, of the selected engine, for two
            //   backends; both backends call successfully, the second on the connection the first left in the
            //   engine's pool - one connection on the wire for both; after shutdown() the engine reports itself
            //   closed (Apache's pool, the JDK client terminated, Jetty stopped, Reactor Netty's pool and loops
            //   disposed).
            // Why it matters: an engine created per backend multiplies pools and threads; one created although
            //   unused costs threads for nothing; one not closed leaks them past the context. The shared
            //   connection is what shows the sharing from outside the factory's own bookkeeping: a client built per
            //   backend would open a connection of its own, and only the last one built would be closed.
            //
            // Given
            AnsweringBackend().use { backend ->
                val factory =
                    newFactory(
                        RestClientProperties.Engine(default = engine),
                        mesh =
                            MeshProperties(
                                defaults =
                                    MeshProperties.Defaults(
                                        individualRequestTimeoutMs = 1000,
                                        transport = MeshProperties.Transport(sidecarBaseUrl = "http://${backend.authority}"),
                                    ),
                                endpoints =
                                    listOf("first", "second").map {
                                        MeshProperties.EndpointConfig(triplet = "/endpoint/${backend.authority}/GET/$it", alias = it)
                                    },
                            ),
                    )
                factory.create("first").get(Greeting::class.java) { it.uri("/hello") }
                factory.create("second").get(Greeting::class.java) { it.uri("/hello") }
                assertThat(factory.engineTransports.keys).containsExactly(engine)
                assertThat(backend.connections.get()).`as`("connections the two backends on %s opened", engine).isEqualTo(1)
                val transport = factory.engineTransports.getValue(engine)

                // When
                factory.shutdown()

                // Then
                assertThat(transport.isClosed).`as`("shutdown() must close the %s engine", engine).isTrue()
            }
        }
    }

    @Nested
    inner class `Abandoned exchanges` {
        @ParameterizedTest
        @EnumSource(RestClientEngine::class)
        fun `should release the caller at the budget and the worker promptly while waiting for the head`(engine: RestClientEngine) {
            // What is tested: the blocking decorator's contract on each engine for a TimeLimiter expiry while the
            //   worker waits for the response head.
            // Success criteria: the request reached the backend before the budget ran out - so the worker was
            //   waiting for the head, not still connecting; with a 300 ms budget, an 8.4 s response timeout and a
            //   10 s backend, the caller gets a TimeoutException within 2 s; the bulkhead permit is back - the
            //   worker has ended - and the engine's connection is released (where the engine counts it) within 2 s
            //   of that.
            // Why it matters: the TimeLimiter releases the caller whatever the engine does; only an engine that
            //   honours the worker's interrupt also frees the permit and the connection. One that did not would
            //   fill the bulkhead with calls nobody waits for until the response timeout, seconds later.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/never")).willReturn(okJson("{}").withFixedDelay(10_000)))
            wireMock.stubFor(get(urlEqualTo("/warm")).willReturn(okJson("""{"message":"warm"}""")))
            val arrivedNanos = AtomicLong()
            arrivals["/never"] = { arrivedNanos.compareAndSet(0, System.nanoTime()) }
            val bulkheads = BulkheadRegistry.ofDefaults()
            val factory =
                newFactory(
                    RestClientProperties.Engine(default = engine),
                    mesh = mesh("abandoned", individualRequestTimeoutMs = 8_000),
                    timeLimiter = budget(Duration.ofMillis(300)),
                    bulkheads = bulkheads,
                )
            val client = factory.create("abandoned")
            assertThat(client.responseTimeout).isEqualTo(Duration.ofMillis(8_400))
            // A first exchange on the fresh engine, so its start, the connect and the class loading are behind
            // the call the budget is measured on. Its outcome does not matter - under load it may itself exceed the
            // 300 ms budget; the engine is initialised either way.
            catchThrowable { client.get(Greeting::class.java) { it.uri("/warm") } }

            // When
            val startNanos = System.nanoTime()
            val failure = catchThrowable { client.get(Greeting::class.java) { it.uri("/never") } }
            val failedNanos = System.nanoTime()
            val elapsed = Duration.ofNanos(failedNanos - startNanos)

            // Then
            assertThat(failure).isInstanceOf(TimeoutException::class.java)
            assertThat(arrivedNanos.get())
                .`as`("the request must have reached the backend before the budget expired")
                .isNotZero()
                .isLessThan(failedNanos)
            assertThat(elapsed).isLessThan(PROMPTLY)
            awaitReleased(factory, engine, bulkheads, "abandoned")
        }

        @ParameterizedTest
        @EnumSource(RestClientEngine::class)
        fun `should release the worker promptly when the budget expires while the body is still arriving`(engine: RestClientEngine) {
            // What is tested: the same contract while the worker is in the BODY read - the head arrived, the body
            //   drips in chunks, each well inside the response timeout.
            // Success criteria: with a 1.5 s budget and a body dripped over 10 s, the caller gets a
            //   TimeoutException within 3 s; the permit and the connection come back within 2 s of that.
            // Why it matters: the body read is the phase no transport timeout bounds as a whole (a peer that drips
            //   passes every per-read guard), so the interrupt is the only thing that ends it - and it is a
            //   different blocking point than the head on every engine. Spring's Reactor Netty body stream ignores
            //   the interrupt outright, which is why that engine hands its reads off.
            //
            // Given
            wireMock.stubFor(
                get(urlEqualTo("/drip")).willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""{"message":"${"y".repeat(1_000)}"}""")
                        .withChunkedDribbleDelay(10, 10_000),
                ),
            )
            val bulkheads = BulkheadRegistry.ofDefaults()
            val factory =
                newFactory(
                    RestClientProperties.Engine(default = engine),
                    mesh = mesh("dripping", individualRequestTimeoutMs = 8_000),
                    timeLimiter = budget(Duration.ofMillis(1_500)),
                    bulkheads = bulkheads,
                )
            val client = factory.create("dripping")

            // When
            val startNanos = System.nanoTime()
            val failure = catchThrowable { client.get(Greeting::class.java) { it.uri("/drip") } }
            val elapsed = Duration.ofNanos(System.nanoTime() - startNanos)

            // Then
            assertThat(failure).isInstanceOf(TimeoutException::class.java)
            assertThat(elapsed).isLessThan(Duration.ofSeconds(3))
            awaitReleased(factory, engine, bulkheads, "dripping")
        }

        @ParameterizedTest
        @EnumSource(RestClientEngine::class)
        fun `should release the worker promptly when the caller is interrupted`(engine: RestClientEngine) {
            // What is tested: the caller-interrupt path of the decorator on each engine - the decorator cancels
            //   the worker itself, Resilience4j does not.
            // Success criteria: the interrupted caller gets an InterruptedException; the permit and the
            //   connection come back within 2 s although budget (5 min) and response timeout (8.4 s) are far off.
            // Why it matters: a caller's own deadline or a cancelled scope is the common way a call is abandoned;
            //   without a prompt worker it would hold its permit until the response timeout.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/never")).willReturn(okJson("{}").withFixedDelay(10_000)))
            val inFlight = arrivalOf("/never")
            val bulkheads = BulkheadRegistry.ofDefaults()
            val factory =
                newFactory(
                    RestClientProperties.Engine(default = engine),
                    mesh = mesh("interrupted", individualRequestTimeoutMs = 8_000),
                    timeLimiter = budget(Duration.ofMinutes(5)),
                    bulkheads = bulkheads,
                )
            val client = factory.create("interrupted")
            val callerThread = AtomicReference<Thread>()
            val call = CompletableFuture<Throwable?>()
            callers.submit {
                callerThread.set(Thread.currentThread())
                call.complete(catchThrowable { client.get(Greeting::class.java) { it.uri("/never") } })
            }
            assertThat(inFlight.await(5, TimeUnit.SECONDS)).`as`("the request must have reached the backend").isTrue()

            // When
            callerThread.get().interrupt()

            // Then
            assertThat(call.get(5, TimeUnit.SECONDS)).isInstanceOf(InterruptedException::class.java)
            awaitReleased(factory, engine, bulkheads, "interrupted")
        }

        @ParameterizedTest
        @EnumSource(RestClientEngine::class)
        fun `should release the worker promptly when the asynchronous future is cancelled`(engine: RestClientEngine) {
            // What is tested: cancel() on the future of getAsync on each engine - it reaches the worker through the
            //   decorator's waiter.
            // Success criteria: the future is cancelled; the permit and the connection come back within 2 s.
            // Why it matters: an asynchronous caller abandons a call by cancelling; the exchange underneath must
            //   end with it on every engine, not run on to the response timeout.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/never")).willReturn(okJson("{}").withFixedDelay(10_000)))
            val inFlight = arrivalOf("/never")
            val bulkheads = BulkheadRegistry.ofDefaults()
            val factory =
                newFactory(
                    RestClientProperties.Engine(default = engine),
                    mesh = mesh("cancelled", individualRequestTimeoutMs = 8_000),
                    timeLimiter = budget(Duration.ofMinutes(5)),
                    bulkheads = bulkheads,
                )
            val future = factory.create("cancelled").getAsync(Greeting::class.java) { it.uri("/never") }
            assertThat(inFlight.await(5, TimeUnit.SECONDS)).`as`("the request must have reached the backend").isTrue()

            // When
            assertThat(future.cancel(true)).isTrue()

            // Then
            assertThat(future.isCancelled).isTrue()
            awaitReleased(factory, engine, bulkheads, "cancelled")
        }
    }

    @Nested
    inner class `The pool settings` {
        @ParameterizedTest
        @EnumSource(RestClientEngine::class)
        fun `should bound the connections by max-connections where the engine applies it`(engine: RestClientEngine) {
            // What is tested: `max-connections` on each engine's wire - one connection, held by a slow call of one
            //   backend, while a second backend on the same engine (and the same sidecar address) calls.
            // Success criteria: on Apache, Jetty and Reactor Netty the second call fails within 2 s without ever
            //   reaching the backend - it waited for the one connection, bounded by the pending-acquire timeout
            //   (Apache, Reactor Netty) or by its own total timeout (Jetty, 700 ms); on the JDK client, which the
            //   startup line says does not apply the setting, it opens a connection of its own and succeeds.
            // Why it matters: the bulkheads are sized against this bound. An engine that silently ran on its own
            //   default pool would let the excess through to the sidecar - or queue it without limit - while its
            //   startup line says "applies max-connections".
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/held")).willReturn(okJson("{}").withFixedDelay(3_000)))
            wireMock.stubFor(get(urlEqualTo("/second")).willReturn(okJson("""{"message":"second"}""")))
            val held = arrivalOf("/held")
            val mesh =
                MeshProperties(
                    defaults =
                        MeshProperties.Defaults(
                            transport = MeshProperties.Transport(sidecarBaseUrl = wireMock.baseUrl(), maxConnections = 1),
                        ),
                    endpoints =
                        listOf(
                            MeshProperties.EndpointConfig(triplet = "/endpoint/${localAuthority()}/GET/holder", alias = "holder", individualRequestTimeoutMs = 8_000),
                            MeshProperties.EndpointConfig(triplet = "/endpoint/${localAuthority()}/GET/waiter", alias = "waiter", individualRequestTimeoutMs = 300),
                        ),
                )
            val factory = newFactory(RestClientProperties.Engine(default = engine), mesh = mesh)
            val holding = callers.submit<Throwable?> { catchThrowable { factory.create("holder").get(Greeting::class.java) { it.uri("/held") } } }
            assertThat(held.await(5, TimeUnit.SECONDS)).`as`("the holding call must have reached the backend").isTrue()

            // When
            val (failure, elapsed) = timed { factory.create("waiter").get(Greeting::class.java) { it.uri("/second") } }

            // Then
            assertThat(elapsed).isLessThan(PROMPTLY)
            if (engine == RestClientEngine.JDK) {
                assertThat(failure).`as`("the JDK client has no connection bound").isNull()
                wireMock.verify(1, getRequestedFor(urlEqualTo("/second")))
            } else {
                // The engine's own bound on the wait, by its exception in the cause chain - not any failure before
                // the request went out: Apache's and Reactor Netty's pending-acquire timeout, Jetty's total timeout.
                val bound =
                    when (engine) {
                        RestClientEngine.APACHE -> "ConnectionRequestTimeoutException"
                        RestClientEngine.REACTOR_NETTY -> "PoolAcquireTimeoutException"
                        else -> "TimeoutException"
                    }
                assertThat(failure).`as`("the second call must wait for the one connection and give up").isInstanceOf(ResourceAccessException::class.java)
                assertThat(generateSequence(failure) { it.cause.takeIf { cause -> cause !== it } }.map { it.javaClass.simpleName }.toList())
                    .`as`("the %s engine's bound on the wait for a connection", engine)
                    .contains(bound)
                wireMock.verify(0, getRequestedFor(urlEqualTo("/second")))
            }
            holding.get(10, TimeUnit.SECONDS)
        }

        @ParameterizedTest
        @EnumSource(RestClientEngine::class)
        fun `should give up a connect at the connect timeout`(engine: RestClientEngine) {
            // What is tested: `connect-timeout-ms` on each engine - against a listening socket whose accept queue
            //   is full, where the kernel drops further connection attempts instead of answering them.
            // Success criteria: with the default 100 ms connect timeout, a 30 s budget and an 8.4 s response
            //   timeout, the call fails within 2 s but not before the 100 ms, with the engine's own connect-timeout
            //   exception in its cause chain - a refused connect would fail at once and prove nothing.
            // Why it matters: the TimeLimiter's margin over the response timeout is computed from this timeout;
            //   an engine on its own default (seconds to minutes) would hold the worker in the connect far past
            //   the margin, and the mesh ladder would be wrong for that engine.
            //
            // Given
            BlackholeBackend.open().use { blackhole ->
                val factory =
                    newFactory(
                        RestClientProperties.Engine(default = engine),
                        mesh = meshAt(blackhole.authority, "unreachable", individualRequestTimeoutMs = 8_000),
                    )
                val client = factory.create("unreachable")

                // When
                val (failure, elapsed) = timed { client.get(Greeting::class.java) { it.uri("/connect") } }

                // Then
                assertThat(failure).isNotNull().isNotInstanceOf(TimeoutException::class.java)
                assertThat(isConnectTimeout(failure)).`as`("a connect timeout, not another failure: %s", failure).isTrue()
                assertThat(elapsed).isGreaterThanOrEqualTo(Duration.ofMillis(100).minus(TIMER_SLACK)).isLessThan(PROMPTLY)
            }
        }
    }

    @Nested
    inner class `The shutdown` {
        @ParameterizedTest
        @EnumSource(RestClientEngine::class)
        fun `should let an in-flight exchange finish before closing the engine`(engine: RestClientEngine) {
            // What is tested: the drain of shutdown() on each engine, on a real clock.
            // Success criteria: a call whose backend answers 500 ms after shutdown() was entered SUCCEEDS with the
            //   backend's value; shutdown() waited at least 100 ms; no "did not drain" / "did not terminate" WARN.
            // Why it matters: closing an engine cuts its in-flight exchanges; without the wait a graceful shutdown
            //   would fail every call still on the wire. Apache's drain has its own cases; the other three engines
            //   drain by other routes (Jetty's and Reactor Netty's counters, the JDK's awaitTermination).
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/finishing")).willReturn(okJson("""{"message":"finished"}""").withFixedDelay(500)))
            val arrived = arrivalOf("/finishing")
            val factory = newFactory(RestClientProperties.Engine(default = engine))
            val client = factory.create("greet")
            val inFlight = callers.submit<Greeting> { client.get(Greeting::class.java) { it.uri("/finishing") } }
            assertThat(arrived.await(5, TimeUnit.SECONDS)).isTrue()

            capturingFactoryWarnings { warnings ->
                // When
                val (outcome, waited) = timed { factory.shutdown() }

                // Then
                assertThat(outcome).isNull()
                assertThat(inFlight.get(5, TimeUnit.SECONDS).message).isEqualTo("finished")
                assertThat(waited).`as`("shutdown() must have waited for the exchange").isGreaterThan(Duration.ofMillis(100))
                assertThat(warnings()).isEmpty()
            }
        }

        @ParameterizedTest
        @EnumSource(RestClientEngine::class)
        fun `should let a call still waiting for a connection finish before closing the engine`(engine: RestClientEngine) {
            // What is tested: the drain of shutdown() on each engine for a call that is not yet on a connection
            //   when the shutdown starts - max-connections 1, one call holding the connection for 500 ms, a second
            //   call of the same backend waiting for it (on the JDK client, whose pool is unbounded, on a
            //   connection of its own).
            // Success criteria: BOTH calls succeed with the backend's answer; shutdown() raises nothing; no
            //   "did not drain" / "did not terminate" WARN.
            // Why it matters: a call queued for a connection is in flight for its caller as much as one on the
            //   wire. A drain that counts only the connections handed out could close the pool in the moment the
            //   first call gives its connection back, before the waiting one has taken it - the reactive engines
            //   failed such a call before their release waited for it.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/queued")).willReturn(okJson("""{"message":"served"}""").withFixedDelay(500)))
            val arrived = arrivalOf("/queued")
            val bulkheads = BulkheadRegistry.ofDefaults()
            val mesh =
                MeshProperties(
                    defaults =
                        MeshProperties.Defaults(
                            individualRequestTimeoutMs = 3_000,
                            transport =
                                MeshProperties.Transport(
                                    sidecarBaseUrl = wireMock.baseUrl(),
                                    maxConnections = 1,
                                    pendingAcquireTimeoutMs = 3_000,
                                ),
                        ),
                    endpoints = listOf(MeshProperties.EndpointConfig(triplet = "/endpoint/${localAuthority()}/GET/queued", alias = "queued")),
                )
            val factory = newFactory(RestClientProperties.Engine(default = engine), mesh = mesh, bulkheads = bulkheads)
            val client = factory.create("queued")
            val first = callers.submit<Greeting> { client.get(Greeting::class.java) { it.uri("/queued") } }
            assertThat(arrived.await(5, TimeUnit.SECONDS)).`as`("the first request reached the backend").isTrue()
            val second = callers.submit<Greeting> { client.get(Greeting::class.java) { it.uri("/queued") } }
            val bulkhead = bulkheads.bulkhead("queued")
            awaitCondition("the second call must have started its exchange") {
                bulkhead.metrics.availableConcurrentCalls == bulkhead.bulkheadConfig.maxConcurrentCalls - 2
            }

            capturingFactoryWarnings { warnings ->
                // When
                val (outcome, _) = timed { factory.shutdown() }

                // Then
                assertThat(outcome).isNull()
                assertThat(first.get(5, TimeUnit.SECONDS).message).isEqualTo("served")
                assertThat(second.get(5, TimeUnit.SECONDS).message).isEqualTo("served")
                assertThat(warnings()).isEmpty()
            }
        }

        @ParameterizedTest
        @EnumSource(RestClientEngine::class)
        fun `should report an engine that does not drain by the deadline and close it anyway`(engine: RestClientEngine) {
            // What is tested: the deadline branch of each engine's shutdown, on a clock that is past the deadline
            //   at its next read.
            // Success criteria: with an exchange in flight, shutdown() returns without an exception, writes one WARN
            //   naming the engine's pool or client and the window, and the engine reports itself closed within 2 s.
            // Why it matters: the line is the only trace of a shutdown that cut calls off; it is unreachable on a
            //   real clock short of a call longer than every response timeout.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/slow")).willReturn(okJson("{}").withFixedDelay(3_000)))
            val arrived = arrivalOf("/slow")
            val factory =
                newFactory(
                    RestClientProperties.Engine(default = engine),
                    mesh = mesh("greet", individualRequestTimeoutMs = 8_000),
                    nanoTime = leapingClock(TimeUnit.HOURS.toNanos(1)),
                )
            val client = factory.create("greet")
            val inFlight = callers.submit<Throwable?> { catchThrowable { client.get(Greeting::class.java) { it.uri("/slow") } } }
            assertThat(arrived.await(5, TimeUnit.SECONDS)).isTrue()
            val transport = factory.engineTransports.getValue(engine)

            capturingFactoryWarnings { warnings ->
                // When
                val outcome = catchThrowable { factory.shutdown() }

                // Then
                assertThat(outcome).isNull()
                assertThat(warnings().single()).contains(engineResourceName(engine), "within ")
                // The JDK client terminates its own threads after shutdownNow() returned.
                awaitCondition("the $engine engine must report itself closed") { transport.isClosed }
            }
            inFlight.get(10, TimeUnit.SECONDS)
        }

        @ParameterizedTest
        @EnumSource(RestClientEngine::class)
        fun `should stop waiting when the shutdown thread is interrupted and leave the flag set`(engine: RestClientEngine) {
            // What is tested: the interrupt exit of each engine's drain.
            // Success criteria: with an exchange in flight, shutdown() on an interrupted thread returns within
            //   2 s (the derived window is far longer), the WARN names the interrupt, and the flag is still set.
            // Why it matters: a container tearing the context down under its own deadline interrupts the
            //   shutdown thread; a drain that ignored it - or spun on it - would hold the shutdown for the whole
            //   window.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/slow")).willReturn(okJson("{}").withFixedDelay(3_000)))
            val arrived = arrivalOf("/slow")
            val factory = newFactory(RestClientProperties.Engine(default = engine), mesh = mesh("greet", individualRequestTimeoutMs = 8_000))
            val client = factory.create("greet")
            val inFlight = callers.submit<Throwable?> { catchThrowable { client.get(Greeting::class.java) { it.uri("/slow") } } }
            assertThat(arrived.await(5, TimeUnit.SECONDS)).isTrue()

            capturingFactoryWarnings { warnings ->
                // When
                val shutdown =
                    callers.submit<Boolean> {
                        Thread.currentThread().interrupt()
                        factory.shutdown()
                        Thread.currentThread().isInterrupted
                    }

                // Then
                assertThat(shutdown.get(PROMPTLY.toMillis(), TimeUnit.MILLISECONDS)).`as`("the interrupt flag must survive").isTrue()
                assertThat(warnings().single()).contains(engineResourceName(engine), "because the shutdown thread was interrupted")
            }
            inFlight.get(10, TimeUnit.SECONDS)
        }

        @Test
        fun `should refuse a client requested after shutdown instead of building it on a closed engine`() {
            // What is tested: create() after shutdown() for a backend whose client was not built before.
            // Success criteria: an IllegalStateException naming the backend and the closed engine; a client built
            //   before the shutdown is still handed out (the cache), not refused.
            // Why it matters: the engines are created with the factory and never again - a client built later
            //   would run on a closed pool and fail every call with an engine error that points nowhere near the
            //   shutdown that caused it.
            //
            // Given
            val factory = newFactory(RestClientProperties.Engine(), mesh = mesh("early", "late"))
            val early = factory.create("early")
            factory.shutdown()

            // When / Then
            assertThat(factory.create("early")).isSameAs(early)
            assertThatThrownBy { factory.create("late") }
                .isInstanceOf(IllegalStateException::class.java)
                .hasMessageContainingAll("shut down", "'late'", "closed HTTP engine")
        }

        @ParameterizedTest
        @EnumSource(RestClientEngine::class, names = ["APACHE", "REACTOR_NETTY"])
        fun `should take the engine's pool gauges out of the host registry on shutdown`(engine: RestClientEngine) {
            // What is tested: the pool meters of the two engines that register any, across shutdown().
            // Success criteria: after one call the host registry carries the engine's pool gauge; after
            //   shutdown() it carries none.
            // Why it matters: a registry outlives the factory on a context refresh or with a second factory;
            //   gauges left behind keep reading a closed pool, and a new pool's gauge with the same name and tags
            //   would be the old one.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/hello")).willReturn(okJson("""{"message":"hi"}""")))
            val meters = SimpleMeterRegistry()
            val factory = newFactory(RestClientProperties.Engine(default = engine), meterRegistry = meters)
            factory.create("greet").get(Greeting::class.java) { it.uri("/hello") }
            val gauge = if (engine == RestClientEngine.APACHE) "restclient.pool.leased" else "reactor.netty.connection.provider.total.connections"
            assertThat(meters.find(gauge).gauges()).isNotEmpty()

            // When
            factory.shutdown()

            // Then
            assertThat(meters.find(gauge).gauges()).isEmpty()
        }
    }

    @Nested
    inner class `The Host header` {
        @ParameterizedTest
        @EnumSource(RestClientEngine::class, names = ["APACHE", "JETTY", "REACTOR_NETTY"])
        fun `should reach the backend as derived from the triplet, not from the base URL`(engine: RestClientEngine) {
            // What is tested: the explicit Host header on the engines that honour one.
            // Success criteria: WireMock, listening on localhost, receives exactly one Host header naming the
            //   triplet's host.
            // Why it matters: the sidecar routes on nothing else; an engine that replaced the header by the
            //   base URL's authority would send every call to the wrong upstream, with no error on this side.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/hosted")).willReturn(okJson("""{"message":"hi"}""")))
            val client =
                newFactory(RestClientProperties.Engine(default = engine), mesh = mesh("hosted", host = "api.example.com"))
                    .create("hosted")

            // When
            client.get(Greeting::class.java) { it.uri("/hosted") }

            // Then
            val request = wireMock.findAll(getRequestedFor(urlEqualTo("/hosted"))).single()
            assertThat(request.header("Host").values()).containsExactly("api.example.com")
        }

        @Test
        fun `should refuse the JDK engine for a backend whose Host the JDK would replace, naming the key`() {
            // What is tested: the fail-fast check of the JDK engine against its restricted Host header.
            // Success criteria: constructing the factory fails with IllegalStateException naming the override
            //   key, the Host the JDK would send instead, and the three ways out.
            // Why it matters: the JDK client silently sends the base URL's authority (`localhost:<port>`) instead;
            //   behind a sidecar every call would be routed by the wrong host. Only a failed start makes that a
            //   configuration error instead of a production incident.
            //
            // Given
            val engines = RestClientProperties.Engine(endpoints = mapOf("hosted" to RestClientEngine.JDK))

            // When / Then
            assertThatThrownBy { newFactory(engines, mesh = mesh("hosted", host = "api.example.com")) }
                .isInstanceOf(IllegalStateException::class.java)
                .hasMessageContainingAll(
                    "app.restclient.engine.endpoints.hosted",
                    "instead of 'api.example.com'",
                    localAuthority(),
                    "-Djdk.httpclient.allowRestrictedHeaders=host",
                )
        }

        @Test
        fun `should see no conflict when the JVM allows the Host header or the base URL already names the host`() {
            // What is tested: the two ways the JDK conflict check lets a backend pass - the JVM-wide property set
            //   by the host, and a base URL whose authority IS the Host (default port omitted, as the JDK does).
            // Success criteria: hostHeaderConflict is null for both, and names the JDK's Host for the plain
            //   sidecar case.
            // Why it matters: a check that refused these would push a host off the JDK engine for no reason; one
            //   that missed the sidecar case would let the misrouting through.
            //
            // Given
            val behindSidecar = mesh("x", host = "api.example.com").resolveAll().values.single()
            val direct = behindSidecar.copy(baseUrl = "http://API.example.com")

            // When / Then
            assertThat(JdkEngineTransport.hostHeaderConflict(behindSidecar, null)).contains("'${localAuthority()}'")
            assertThat(JdkEngineTransport.hostHeaderConflict(behindSidecar, "connection, host")).isNull()
            assertThat(JdkEngineTransport.hostHeaderConflict(direct, null)).isNull()
        }
    }

    @Nested
    inner class `The selection` {
        @Test
        fun `should run an overridden backend on its engine and leave the others on the default`() {
            // What is tested: `app.restclient.engine.endpoints.<alias>` - one backend switched to the JDK engine,
            //   the other left on the default.
            // Success criteria: the overridden backend's client runs on Spring's JdkClientHttpRequestFactory, the
            //   other on HttpComponentsClientHttpRequestFactory; both engines exist and both calls succeed.
            // Why it matters: the override is how a host trials an engine on one backend; one that leaked to the
            //   other backends, or did not apply, would make the trial meaningless.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/hello")).willReturn(okJson("""{"message":"hi"}""")))
            val factory =
                newFactory(
                    RestClientProperties.Engine(endpoints = mapOf("trial" to RestClientEngine.JDK)),
                    mesh = mesh("trial", "stable"),
                )

            // When
            factory.create("trial").get(Greeting::class.java) { it.uri("/hello") }
            factory.create("stable").get(Greeting::class.java) { it.uri("/hello") }

            // Then
            assertThat(factory.requestFactoryOf("trial")).isInstanceOf(JdkClientHttpRequestFactory::class.java)
            assertThat(factory.requestFactoryOf("stable")).isInstanceOf(HttpComponentsClientHttpRequestFactory::class.java)
            assertThat(factory.engineTransports.keys).containsExactly(RestClientEngine.APACHE, RestClientEngine.JDK)
        }

        @Test
        fun `should not create the Apache pool when no backend runs on Apache`() {
            // What is tested: lazy creation from the other side - every backend on the JDK engine.
            // Success criteria: the only transport is the JDK's, and no `restclient.pool.*` gauge was registered.
            // Why it matters: the default engine is not special; a pool, an evictor and gauges for an engine no
            //   backend uses would be resources and meters that describe nothing.
            //
            // Given
            val meters = SimpleMeterRegistry()

            // When
            val factory = newFactory(RestClientProperties.Engine(default = RestClientEngine.JDK), meterRegistry = meters)

            // Then
            assertThat(factory.engineTransports.keys).containsExactly(RestClientEngine.JDK)
            assertThat(factory.evictor).isNull()
            assertThat(meters.find("restclient.pool.leased").gauge()).isNull()
        }

        @Test
        fun `should refuse an override for an alias that is no mesh endpoint, naming the key`() {
            // What is tested: typo protection on the override keys.
            // Success criteria: constructing the factory fails with IllegalStateException naming the full key and
            //   listing the known aliases.
            // Why it matters: a mistyped alias would otherwise leave the intended backend on the default engine
            //   without a word - the trial would seem to run and measure nothing.
            //
            // Given
            val engines = RestClientProperties.Engine(endpoints = mapOf("tiral" to RestClientEngine.JETTY))

            // When / Then
            assertThatThrownBy { newFactory(engines, mesh = mesh("trial")) }
                .isInstanceOf(IllegalStateException::class.java)
                .hasMessageContainingAll("app.restclient.engine.endpoints.tiral", "names no mesh endpoint", "[trial]")
        }

        @Test
        fun `should refuse an engine whose library is missing, naming the key and the dependency to add`() {
            // What is tested: the classpath check of a selected engine.
            // Success criteria: with Jetty reported absent, constructing a factory that selects it by default
            //   fails naming `app.restclient.engine.default`, the backend and the Maven coordinates; a factory that
            //   does not select Jetty starts although Jetty is absent.
            // Why it matters: the engine libraries are optional dependencies a host adds itself; without the
            //   check a forgotten one surfaces as a NoClassDefFoundError at the first call, not at startup.
            //
            // Given
            val jettyMissing: (RestClientEngine) -> Boolean = { it != RestClientEngine.JETTY }

            // When / Then
            assertThatThrownBy {
                newFactory(RestClientProperties.Engine(default = RestClientEngine.JETTY), engineLibraryPresent = jettyMissing)
            }.isInstanceOf(IllegalStateException::class.java)
                .hasMessageContainingAll("app.restclient.engine.default", "'jetty'", "greet", "org.eclipse.jetty:jetty-client")
            assertThat(newFactory(RestClientProperties.Engine(), engineLibraryPresent = jettyMissing).engineTransports.keys)
                .containsExactly(RestClientEngine.APACHE)
        }

        @Test
        fun `should log the engine of every backend and what each engine applies and does not apply`() {
            // What is tested: the startup lines of the selection.
            // Success criteria: one line names the default and every backend's engine sorted by alias; one line
            //   per engine in use names its backends, what it applies and what it does not - the JDK's names
            //   max-connections among the latter.
            // Why it matters: an engine without a knob does not complain about the knob; the setting just has no
            //   effect. This line is the only place that says so before it matters.
            //
            // Given
            val logger = LoggerFactory.getLogger(ResilientRestClientFactory::class.java) as Logger
            val lines = ListAppender<ILoggingEvent>().apply { start() }
            logger.addAppender(lines)
            try {
                // When
                newFactory(
                    RestClientProperties.Engine(endpoints = mapOf("b-trial" to RestClientEngine.JDK)),
                    mesh = mesh("c-stable", "b-trial", "a-stable"),
                )

                // Then
                val messages = lines.list.map { it.formattedMessage }
                assertThat(messages).contains("HTTP engines: default=apache, per backend [a-stable=apache, b-trial=jdk, c-stable=apache]")
                assertThat(messages.single { it.startsWith("HTTP engine 'apache' for a-stable, c-stable: applies ") })
                    .contains("max-connections=100", "does not apply -")
                assertThat(messages.single { it.startsWith("HTTP engine 'jdk' for b-trial: applies ") })
                    .contains("does not apply max-connections=100", "cancellation: ")
            } finally {
                logger.detachAppender(lines)
            }
        }

        @Test
        fun `should read the pool's state off the Reactor Netty gauges it registers`() {
            // What is tested: the VALUES of the six pool gauges the Reactor Netty engine registers in the host's
            //   registry, while a call holds a connection and after it gave it back.
            // Success criteria: during the call total = active = 1, idle = pending = 0, max = max-connections (100)
            //   and max pending = Reactor Netty's 2 x max-connections (200); after it active = 0 and idle = 1.
            // Why it matters: the engine registers its own gauges instead of Reactor Netty's, so each reads one
            //   number off the pool by hand - a gauge wired to the wrong number (active reading the idle count)
            //   would report a full pool as empty, and only the registration was pinned before.
            //
            // Given: a call held for a second by the backend
            wireMock.stubFor(get(urlEqualTo("/held")).willReturn(okJson("""{"message":"held"}""").withFixedDelay(1_000)))
            val arrived = arrivalOf("/held")
            val meters = SimpleMeterRegistry()
            val client = newFactory(RestClientProperties.Engine(default = RestClientEngine.REACTOR_NETTY), meterRegistry = meters).create("greet")
            val gauge = { suffix: String ->
                meters
                    .find("reactor.netty.connection.provider$suffix")
                    .tag("name", ReactorNettyEngineTransport.POOL_NAME)
                    .gauge()
                    ?.value()
                    ?.toInt()
            }

            // When
            val call = callers.submit<Greeting> { client.get(Greeting::class.java) { it.uri("/held") } }
            assertThat(arrived.await(5, TimeUnit.SECONDS)).`as`("the call must hold its connection").isTrue()
            val during = listOf(".total.connections", ".active.connections", ".idle.connections", ".pending.connections", ".max.connections", ".max.pending.connections").associateWith(gauge)
            call.get(5, TimeUnit.SECONDS)

            // Then
            assertThat(during).containsExactlyInAnyOrderEntriesOf(
                mapOf(
                    ".total.connections" to 1,
                    ".active.connections" to 1,
                    ".idle.connections" to 0,
                    ".pending.connections" to 0,
                    ".max.connections" to 100,
                    ".max.pending.connections" to 200,
                ),
            )
            assertThat(holdsWithinPromptly { gauge(".active.connections") == 0 && gauge(".idle.connections") == 1 })
                .`as`("the connection must be back in the pool, idle")
                .isTrue()
        }

        @Test
        fun `should report a Reactor Netty disposal that does not complete and go on`() {
            // What is tested: the expiry branch of the Reactor Netty engine's bounded wait for a disposal, with a
            //   disposal that never completes and a 100 ms wait.
            // Success criteria: the wait returns without an exception within 1 s and writes one WARN naming the
            //   engine, what was not disposed and the wait.
            // Why it matters: shutdown() runs from a destroy callback. `block(Duration)` would throw an
            //   IllegalStateException out of it and skip the rest of the shutdown - the loops' disposal and the
            //   hand-off executor's; the WARN is the statement the expiry is. Unreachable with the real pool, whose
            //   disposal completes in milliseconds.
            //
            // Given
            val transport = ReactorNettyEngineTransport(EngineTransport.Settings(MeshProperties.Transport(), RestClientProperties.Pool(), null))
            val logger = LoggerFactory.getLogger("disposal-probe") as Logger
            val lines = ListAppender<ILoggingEvent>().apply { start() }
            logger.addAppender(lines)
            try {
                // When
                val (failure, elapsed) = timed { transport.awaitDisposal("connection pool", Mono.never(), logger, Duration.ofMillis(100)) }

                // Then
                assertThat(failure).isNull()
                assertThat(elapsed).isLessThan(Duration.ofSeconds(1))
                assertThat(lines.list.single { it.level == Level.WARN }.formattedMessage)
                    .isEqualTo("HTTP engine 'reactor-netty': connection pool not disposed within PT0.1S - leaving it to finish on its own")
            } finally {
                logger.detachAppender(lines)
                transport.shutdown(System.nanoTime(), System::nanoTime, Duration.ZERO, logger)
            }
        }

        @Test
        fun `should register Reactor Netty's pool gauges in the host registry and keep off its global resources`() {
            // What is tested: the Reactor Netty engine's resources - its pool gauges and its event loops.
            // Success criteria: after one call the host registry carries
            //   `reactor.netty.connection.provider.total.connections{name=restclient-reactor-netty}`, while
            //   Micrometer's global registry and Reactor Netty's global HttpResources are exactly as they were
            //   before the call; the call started no `reactor-http-*` thread, and did start the engine's own
            //   `restclient-reactor-netty-*` loops. Compared against the state before, since another test in the
            //   same JVM may have touched the global resources - which says nothing about this engine.
            // Why it matters: Reactor Netty's defaults put a client on JVM-global resources and its meters in
            //   Micrometer's global registry - shared with every other Reactor Netty client of the host, which is
            //   exactly what a library's engine must not touch.
            //
            // Given: the JVM-wide state before the call - another test in this JVM may have touched it already
            wireMock.stubFor(get(urlEqualTo("/hello")).willReturn(okJson("""{"message":"hi"}""")))
            val meters = SimpleMeterRegistry()
            val globalResources = HttpResources::class.java.getDeclaredField("httpResources").apply { isAccessible = true }
            val globalBefore = (globalResources.get(null) as AtomicReference<*>).get()
            val globalGaugesBefore =
                Metrics.globalRegistry
                    .find("reactor.netty.connection.provider.total.connections")
                    .gauges()
                    .toSet()
            val threadsBefore = Thread.getAllStackTraces().keys
            val client = newFactory(RestClientProperties.Engine(default = RestClientEngine.REACTOR_NETTY), meterRegistry = meters).create("greet")

            // When
            client.get(Greeting::class.java) { it.uri("/hello") }

            // Then: what the call added, and only that
            assertThat(
                meters
                    .find("reactor.netty.connection.provider.total.connections")
                    .tag("name", ReactorNettyEngineTransport.POOL_NAME)
                    .gauge(),
            ).isNotNull()
            assertThat(
                Metrics.globalRegistry
                    .find("reactor.netty.connection.provider.total.connections")
                    .gauges()
                    .toSet(),
            ).`as`("gauges added to Micrometer's global registry")
                .isEqualTo(globalGaugesBefore)
            assertThat((globalResources.get(null) as AtomicReference<*>).get())
                .`as`("Reactor Netty's global HttpResources")
                .isSameAs(globalBefore)
            val newThreads =
                Thread
                    .getAllStackTraces()
                    .keys
                    .filterNot { it in threadsBefore }
                    .map { it.name }
            assertThat(newThreads).noneMatch { it.startsWith("reactor-http-") }
            assertThat(newThreads).anyMatch { it.startsWith(ReactorNettyEngineTransport.POOL_NAME) }
        }

        @Test
        fun `should report one connection pool per engine with its backends for the startup guard`() {
            // What is tested: connectionPools() - what the factory tells the core's startup guard about the pools
            //   the bulkheads are sized against.
            // Success criteria: with two backends on Apache, one on Jetty and one on the JDK client, three pools:
            //   `restclient-apache` with both Apache backends and max-connections, `restclient-jetty` with its one
            //   backend and max-connections, `restclient-jdk` with its backend and no bound.
            // Why it matters: every engine has a pool of its own. A guard that summed all bulkheads against one
            //   pool would warn about a correctly sized split; one that checked the JDK client would warn about a
            //   limit that does not exist.
            //
            // Given
            val factory =
                newFactory(
                    RestClientProperties.Engine(endpoints = mapOf("c-jetty" to RestClientEngine.JETTY, "d-jdk" to RestClientEngine.JDK)),
                    mesh = mesh("a-apache", "b-apache", "c-jetty", "d-jdk"),
                )

            // When
            val pools = factory.connectionPools()

            // Then
            assertThat(pools).containsExactlyInAnyOrder(
                MeshConnectionPool("restclient-apache", 100, setOf("a-apache", "b-apache")),
                MeshConnectionPool("restclient-jetty", 100, setOf("c-jetty")),
                MeshConnectionPool("restclient-jdk", null, setOf("d-jdk")),
            )
        }
    }

    /** The authority the JDK client sends for the WireMock base URL - the triplet host of every default endpoint. */
    private fun localAuthority(): String = "localhost:${wireMock.port()}"

    /** A mesh with one endpoint per alias in [aliases], all against WireMock, all with the triplet host [host]. */
    private fun mesh(
        vararg aliases: String,
        host: String = localAuthority(),
        individualRequestTimeoutMs: Long = 1000,
    ): MeshProperties =
        MeshProperties(
            defaults =
                MeshProperties.Defaults(
                    individualRequestTimeoutMs = individualRequestTimeoutMs,
                    transport = MeshProperties.Transport(sidecarBaseUrl = wireMock.baseUrl()),
                ),
            endpoints = aliases.map { MeshProperties.EndpointConfig(triplet = "/endpoint/$host/GET/$it", alias = it) },
        )

    private fun budget(timeout: Duration): TimeLimiterConfig =
        TimeLimiterConfig
            .custom()
            .timeoutDuration(timeout)
            .cancelRunningFuture(true)
            .build()

    /** A factory with the engine selection [engines], over registries of its own, shut down after the test. */
    private fun newFactory(
        engines: RestClientProperties.Engine,
        mesh: MeshProperties = mesh("greet", "lookup", "status", "limited"),
        timeLimiter: TimeLimiterConfig = GENEROUS_BUDGET,
        bulkheads: BulkheadRegistry = BulkheadRegistry.ofDefaults(),
        maxInMemorySize: DataSize = RestClientProperties().maxInMemorySize,
        meterRegistry: MeterRegistry? = null,
        engineLibraryPresent: (RestClientEngine) -> Boolean = RestClientEngine::isPresent,
        nanoTime: () -> Long = System::nanoTime,
    ): ResilientRestClientFactory {
        val resilience =
            BlockingResilienceDecorator(CircuitBreakerRegistry.ofDefaults(), TimeLimiterRegistry.of(timeLimiter), bulkheads)
                .also { decorators += it }
        return ResilientRestClientFactory(
            RestClientProperties(maxInMemorySize = maxInMemorySize, engine = engines),
            mesh,
            resilience,
            objectMapper,
            meterRegistry = meterRegistry,
            // Deliberately no Spring context: a bare builder stands in for Boot's bean (the isolation test runs
            // the factory in a context).
            restClientBuilder = RestClient.builder(),
            engineLibraryPresent = engineLibraryPresent,
            nanoTime = nanoTime,
        ).also { factories += it }
    }

    /**
     * Waits [PROMPTLY] for the abandoned exchange of [backend] to give back its bulkhead permit (its worker
     * ended) and, where the engine counts connections, its connection.
     */
    private fun awaitReleased(
        factory: ResilientRestClientFactory,
        engine: RestClientEngine,
        bulkheads: BulkheadRegistry,
        backend: String,
    ) {
        val bulkhead = bulkheads.bulkhead(backend)
        awaitCondition("the abandoned exchange on $engine must release its bulkhead permit") {
            bulkhead.metrics.availableConcurrentCalls == bulkhead.bulkheadConfig.maxConcurrentCalls
        }
        val transport = factory.engineTransports.getValue(engine)
        if (transport.connectionsInUse() != null) {
            awaitCondition("the abandoned exchange on $engine must release its connection") { transport.connectionsInUse() == 0 }
        }
    }

    /** A mesh with one endpoint [alias] whose sidecar is [authority] (`host:port`), the triplet host being the same authority. */
    private fun meshAt(
        authority: String,
        alias: String,
        individualRequestTimeoutMs: Long = 1000,
    ): MeshProperties =
        MeshProperties(
            defaults =
                MeshProperties.Defaults(
                    individualRequestTimeoutMs = individualRequestTimeoutMs,
                    transport = MeshProperties.Transport(sidecarBaseUrl = "http://$authority"),
                ),
            endpoints = listOf(MeshProperties.EndpointConfig(triplet = "/endpoint/$authority/GET/$alias", alias = alias)),
        )

    /**
     * A backend that answers every request with a 200 declaring a 1 MiB body, sends the head and the first 4 KiB
     * at once and then one byte every 200 ms - a body that is still arriving long after any test is over, unless
     * the client drops the connection, which [connectionDropped] reports. WireMock's dribble cannot stand in: it
     * hands the body to its output buffer, and the head reaches the wire only once that buffer is full.
     */
    private class DrippingBackend(
        /** Close the connection after the first 4 KiB instead of dripping on - a peer that cuts the body off. */
        private val cutAfterFirstChunk: Boolean = false,
    ) : AutoCloseable {
        private val server = ServerSocket(0, 50, InetAddress.getLoopbackAddress())
        val authority: String = "localhost:${server.localPort}"
        val connectionDropped = CountDownLatch(1)

        init {
            thread(isDaemon = true, name = "dripping-backend-acceptor") {
                try {
                    while (true) {
                        val socket = server.accept()
                        thread(isDaemon = true, name = "dripping-backend-connection") { drip(socket) }
                    }
                } catch (_: IOException) {
                    // The server socket was closed - the test is over.
                }
            }
        }

        private fun drip(socket: Socket) =
            socket.use { connection ->
                try {
                    val request = connection.getInputStream().bufferedReader(Charsets.ISO_8859_1)
                    while (request.readLine().orEmpty().isNotEmpty()) {
                        // The request head, up to its empty line; the GET carries no body.
                    }
                    val out = connection.getOutputStream()
                    out.write(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: ${1024 * 1024}\r\n\r\n"
                            .toByteArray(Charsets.ISO_8859_1),
                    )
                    out.write(ByteArray(4096) { 'z'.code.toByte() })
                    out.flush()
                    if (cutAfterFirstChunk) return@use
                    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
                    while (System.nanoTime() < deadline) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(200))
                        out.write('z'.code)
                        out.flush()
                    }
                } catch (_: IOException) {
                    // The client went away - what the test waits for.
                    connectionDropped.countDown()
                }
            }

        override fun close() = server.close()
    }

    /**
     * A backend that answers every request with a small 200, keeping the connection alive, and counts the
     * connections it accepted - what tells one shared pool from a pool per backend. WireMock cannot say this: it
     * reports requests, not connections. The twin of the reactive module's backend of the same name.
     */
    private class AnsweringBackend : AutoCloseable {
        private val server = ServerSocket(0, 50, InetAddress.getLoopbackAddress())
        val authority: String = "${server.inetAddress.hostAddress}:${server.localPort}"

        /** The connections the backend accepted so far. */
        val connections = AtomicInteger()

        private val accepted = CopyOnWriteArrayList<Socket>()

        init {
            thread(isDaemon = true, name = "answering-backend-acceptor") {
                try {
                    while (true) {
                        val socket = server.accept()
                        connections.incrementAndGet()
                        accepted += socket
                        thread(isDaemon = true, name = "answering-backend-connection") { answer(socket) }
                    }
                } catch (_: IOException) {
                    // The server socket was closed - the test is over.
                }
            }
        }

        private fun answer(socket: Socket) =
            socket.use { connection ->
                val body = """{"message":"hi"}""".toByteArray()
                val head = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: ${body.size}\r\n\r\n"
                try {
                    val request = connection.getInputStream().bufferedReader(Charsets.ISO_8859_1)
                    while (true) {
                        // One request head per call, up to its empty line; the GETs carry no body.
                        val first = request.readLine() ?: return@use
                        if (first.isEmpty()) continue
                        while (request.readLine().orEmpty().isNotEmpty()) {
                            // The rest of the head.
                        }
                        connection.getOutputStream().apply {
                            write(head.toByteArray(Charsets.ISO_8859_1))
                            write(body)
                            flush()
                        }
                    }
                } catch (_: IOException) {
                    // The client closed the connection.
                }
            }

        override fun close() {
            server.close()
            accepted.forEach { it.close() }
        }
    }

    /**
     * A listening socket whose accept queue is full, so the kernel drops further SYNs and a connect to it hangs
     * until the client's own connect timeout. Filled by plain connects until one of them times out itself.
     */
    private class BlackholeBackend private constructor(
        private val server: ServerSocket,
        private val fillers: List<Socket>,
    ) : AutoCloseable {
        /**
         * The loopback ADDRESS the socket listens on, never `localhost`: that name may resolve to `::1` first,
         * where nothing listens, and an engine would get a refused connect at once instead of the dropped one -
         * the case would be green without its connect timeout ever running.
         */
        val authority: String = "${server.inetAddress.hostAddress}:${server.localPort}"

        override fun close() {
            fillers.forEach { it.close() }
            server.close()
        }

        companion object {
            fun open(): BlackholeBackend {
                val server = ServerSocket(0, 1, InetAddress.getLoopbackAddress())
                val fillers = mutableListOf<Socket>()
                repeat(64) {
                    val socket = Socket()
                    try {
                        socket.connect(InetSocketAddress(server.inetAddress, server.localPort), 200)
                        fillers += socket
                    } catch (_: IOException) {
                        socket.close()
                        return BlackholeBackend(server, fillers)
                    }
                }
                fillers.forEach { it.close() }
                server.close()
                error("the accept queue of a backlog-1 socket never filled - no blackhole on this platform")
            }
        }
    }

    /** The pool or client name each engine's shutdown line names. */
    private fun engineResourceName(engine: RestClientEngine): String =
        when (engine) {
            RestClientEngine.APACHE -> RestClientMetrics.POOL_NAME
            RestClientEngine.JDK -> JdkEngineTransport.CLIENT_NAME
            RestClientEngine.JETTY -> JettyEngineTransport.POOL_NAME
            RestClientEngine.REACTOR_NETTY -> ReactorNettyEngineTransport.POOL_NAME
        }

    /** Captures the factory's WARN lines for [block], which reads them through the function it is given. */
    private fun capturingFactoryWarnings(block: (warnings: () -> List<String>) -> Unit) {
        val logger = LoggerFactory.getLogger(ResilientRestClientFactory::class.java) as Logger
        val lines = ListAppender<ILoggingEvent>().apply { start() }
        logger.addAppender(lines)
        try {
            block {
                lines.list
                    .toList()
                    .filter { it.level == ch.qos.logback.classic.Level.WARN }
                    .map { it.formattedMessage }
            }
        } finally {
            logger.detachAppender(lines)
        }
    }

    /** A monotonic clock that leaps by [stepNanos] on every read, so a deadline computed from it is past by the next read. */
    private fun leapingClock(stepNanos: Long): () -> Long {
        val now = AtomicLong()
        return { now.addAndGet(stepNanos) }
    }

    /**
     * Whether [failure]'s cause chain carries the connect timeout of one of the engines: `ConnectTimeoutException`
     * (Apache, Reactor Netty), `HttpConnectTimeoutException` (JDK), or Jetty's `SocketTimeoutException` "Connect Timeout".
     */
    private fun isConnectTimeout(failure: Throwable?): Boolean =
        generateSequence(failure) { it.cause.takeIf { cause -> cause !== it } }.any { cause ->
            cause.javaClass.simpleName in setOf("ConnectTimeoutException", "HttpConnectTimeoutException") ||
                (cause is SocketTimeoutException && cause.message == "Connect Timeout")
        }

    /** Runs [call], returning what it threw (or `null`) and how long it took. */
    private fun timed(call: () -> Unit): Pair<Throwable?, Duration> {
        val startNanos = System.nanoTime()
        val failure = catchThrowable(call)
        return failure to Duration.ofNanos(System.nanoTime() - startNanos)
    }

    /** Whether [condition] holds within [PROMPTLY], polled every 10 ms - the bounded wait for a state another thread reaches. */
    private fun holdsWithinPromptly(condition: () -> Boolean): Boolean {
        val deadline = System.nanoTime() + PROMPTLY.toNanos()
        while (!condition()) {
            if (System.nanoTime() - deadline > 0) return false
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10))
        }
        return true
    }

    /** Polls [condition] every 10 ms for at most [PROMPTLY], failing with [description] - for states only a reading exposes. */
    private fun awaitCondition(
        description: String,
        condition: () -> Boolean,
    ) {
        val deadline = System.nanoTime() + PROMPTLY.toNanos()
        while (!condition()) {
            assertThat(System.nanoTime()).`as`("$description within $PROMPTLY").isLessThan(deadline)
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10))
        }
    }

    /** Response DTO with a no-arg constructor: the client's Jackson mapper has no Kotlin module. */
    class Greeting {
        var message: String? = null
    }

    /** Request DTO, serialised through its getter. */
    class LookupRequest(
        val id: String,
    )
}
