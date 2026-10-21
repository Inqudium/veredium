package eu.inqudium.veredium.restclient

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
import reactor.netty.http.HttpResources
import tools.jackson.databind.json.JsonMapper
import java.net.SocketTimeoutException
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.LockSupport

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

        /** The generous budget of every case that is not about timing - see `ResilientRestClientIntegrationTest`. */
        private val GENEROUS_BUDGET: TimeLimiterConfig = TimeLimiterConfig.custom().timeoutDuration(Duration.ofSeconds(30)).build()

        /** How long an abandoned exchange may take to give back its permit and its connection - far below every response timeout it is pitted against. */
        private val PROMPTLY: Duration = Duration.ofSeconds(2)

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

    private val objectMapper = JsonMapper.builder().build()
    private val factories = mutableListOf<ResilientRestClientFactory>()
    private val decorators = mutableListOf<BlockingResilienceDecorator>()
    private val callers = Executors.newVirtualThreadPerTaskExecutor()

    @BeforeEach
    fun resetStubs() {
        wireMock.resetAll()
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
            assertThat(elapsed).isLessThan(Duration.ofMillis(2_500))
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
        fun `should create only the selected engine, share it, and close it on shutdown`(engine: RestClientEngine) {
            // What is tested: the engine's lifecycle in the factory - created only when selected, one instance
            //   for all backends on it, closed by shutdown().
            // Success criteria: the factory holds exactly one transport, of the selected engine, for two
            //   backends; both backends call successfully; after shutdown() the engine reports itself closed
            //   (Apache's pool, the JDK client terminated, Jetty stopped, Reactor Netty's pool and loops disposed).
            // Why it matters: an engine created per backend multiplies pools and threads; one created although
            //   unused costs threads for nothing; one not closed leaks them past the context.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/hello")).willReturn(okJson("""{"message":"hi"}""")))
            val factory = newFactory(RestClientProperties.Engine(default = engine), mesh = mesh("first", "second"))
            factory.create("first").get(Greeting::class.java) { it.uri("/hello") }
            factory.create("second").get(Greeting::class.java) { it.uri("/hello") }
            assertThat(factory.engineTransports.keys).containsExactly(engine)
            val transport = factory.engineTransports.getValue(engine)

            // When
            factory.shutdown()

            // Then
            assertThat(transport.isClosed).`as`("shutdown() must close the %s engine", engine).isTrue()
        }
    }

    @Nested
    inner class `Abandoned exchanges` {
        @ParameterizedTest
        @EnumSource(RestClientEngine::class)
        fun `should release the caller at the budget and the worker promptly while waiting for the head`(engine: RestClientEngine) {
            // What is tested: the blocking decorator's contract on each engine for a TimeLimiter expiry while the
            //   worker waits for the response head.
            // Success criteria: with a 300 ms budget, an 8.4 s response timeout and a 10 s backend, the caller
            //   gets a TimeoutException within 2 s; the bulkhead permit is back - the worker has ended - and the
            //   engine's connection is released (where the engine counts it) within 2 s of that.
            // Why it matters: the TimeLimiter releases the caller whatever the engine does; only an engine that
            //   honours the worker's interrupt also frees the permit and the connection. One that did not would
            //   fill the bulkhead with calls nobody waits for until the response timeout, seconds later.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/never")).willReturn(okJson("{}").withFixedDelay(10_000)))
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

            // When
            val startNanos = System.nanoTime()
            val failure = catchThrowable { client.get(Greeting::class.java) { it.uri("/never") } }
            val elapsed = Duration.ofNanos(System.nanoTime() - startNanos)

            // Then
            assertThat(failure).isInstanceOf(TimeoutException::class.java)
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
            val inFlight = CountDownLatch(1)
            wireMock.addMockServiceRequestListener { request, _ -> if (request.url == "/never") inFlight.countDown() }
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
            val inFlight = CountDownLatch(1)
            wireMock.addMockServiceRequestListener { request, _ -> if (request.url == "/never") inFlight.countDown() }
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
        fun `should register Reactor Netty's pool gauges in the host registry and keep off its global resources`() {
            // What is tested: the Reactor Netty engine's resources - its pool gauges and its event loops.
            // Success criteria: after one call the host registry carries
            //   `reactor.netty.connection.provider.total.connections{name=restclient-reactor-netty}` and Micrometer's
            //   global registry does not; Reactor Netty's global HttpResources were never created and no
            //   `reactor-http-*` thread exists, while the exchange ran on the engine's own
            //   `restclient-reactor-netty-*` loops.
            // Why it matters: Reactor Netty's defaults put a client on JVM-global resources and its meters in
            //   Micrometer's global registry - shared with every other Reactor Netty client of the host, which is
            //   exactly what a library's engine must not touch.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/hello")).willReturn(okJson("""{"message":"hi"}""")))
            val meters = SimpleMeterRegistry()
            val client = newFactory(RestClientProperties.Engine(default = RestClientEngine.REACTOR_NETTY), meterRegistry = meters).create("greet")

            // When
            client.get(Greeting::class.java) { it.uri("/hello") }

            // Then
            assertThat(
                meters
                    .find("reactor.netty.connection.provider.total.connections")
                    .tag("name", ReactorNettyEngineTransport.POOL_NAME)
                    .gauge(),
            ).isNotNull()
            assertThat(Metrics.globalRegistry.find("reactor.netty.connection.provider.total.connections").gauges()).isEmpty()
            val globalResources = HttpResources::class.java.getDeclaredField("httpResources").apply { isAccessible = true }
            assertThat((globalResources.get(null) as AtomicReference<*>).get()).`as`("Reactor Netty's global HttpResources").isNull()
            val threadNames = Thread.getAllStackTraces().keys.map { it.name }
            assertThat(threadNames).noneMatch { it.startsWith("reactor-http-") }
            assertThat(threadNames).anyMatch { it.startsWith(ReactorNettyEngineTransport.POOL_NAME) }
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
        ).also { factories += it }
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
