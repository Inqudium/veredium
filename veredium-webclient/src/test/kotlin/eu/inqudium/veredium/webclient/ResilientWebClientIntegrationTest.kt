package eu.inqudium.veredium.webclient

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
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
import eu.inqudium.legatium.webclient.logging.ClientRequestLoggingFilter
import eu.inqudium.veredium.core.Veredium
import eu.inqudium.veredium.core.client.EmptyResponseBodyException
import eu.inqudium.veredium.core.client.ResponseBodyMetrics
import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.testsupport.CapturingAppender
import eu.inqudium.veredium.testsupport.CustomizedRegistries
import eu.inqudium.veredium.testsupport.Greeting
import eu.inqudium.veredium.testsupport.LookupRequest
import eu.inqudium.veredium.testsupport.Unserialisable
import eu.inqudium.veredium.testsupport.WireMockArrivals
import eu.inqudium.veredium.testsupport.capturing
import io.github.resilience4j.bulkhead.BulkheadConfig
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micrometer.observation.Observation
import io.micrometer.observation.ObservationHandler
import io.micrometer.observation.ObservationRegistry
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.slf4j.MDCContext
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.core.io.buffer.DataBufferLimitException
import org.springframework.http.MediaType
import org.springframework.util.unit.DataSize
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono
import reactor.test.scheduler.VirtualTimeScheduler
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.json.JsonMapper
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * End-to-end tests for the per-call pipeline of [ResilientWebClient] and the assembly/caching in
 * [ResilientWebClientFactory], driven against a real (WireMock) HTTP backend over the loopback transport.
 *
 * These cover the behaviour that only emerges when the whole chain runs together: response deserialisation,
 * request-body serialisation happening exactly once and reaching the wire, error surfacing from `retrieve()`,
 * and the three Resilience4j primitives (time limiter, circuit breaker, bulkhead) decorating the call. The
 * Resilience4j registries are built programmatically per test so each scenario configures its own budget
 * and window without an `application.yml`.
 *
 * The client uses a Jackson 3 [ObjectMapper] without the Kotlin module on the classpath, so the test
 * payload types ([Greeting], [LookupRequest]) are written to (de)serialise via the no-arg constructor and
 * bean accessors rather than a Kotlin primary constructor.
 *
 * The tests are grouped by topic: what reaches the wire, the HTTP observation, the response-size meter, error
 * statuses and empty bodies, the Resilience4j decoration, the factory's cache, the client's own log lines, the
 * caller's MDC on those lines, the pool's shutdown, and the self-tests of the in-flight barrier that keeps one
 * test's late log lines out of the next one's capture.
 */
class ResilientWebClientIntegrationTest {
    companion object {
        /**
         * The client's error line - what the log assertions pick their line by, and the terminal marker of a capture
         * while the client logger runs at its configured level (see [awaitCall]).
         */
        private const val ERROR_PREFIX = "Error during request"

        /**
         * The client's `doFinally` line - the LAST one any call can produce, whatever its outcome, and hence
         * the terminal marker whenever the client logger is capturing at DEBUG. Also the closing half of
         * [InFlightCallBarrier]'s count.
         */
        private const val SIGNAL_PREFIX = "On signalType="

        /** The client's per-subscription line, logged inside `Mono.defer` - [InFlightCallBarrier]'s opening half. */
        private const val EXECUTING_PREFIX = "Executing request"

        /** How long [tearDown] waits for a test's reactive work before calling it a leak. */
        private val BARRIER_TIMEOUT: Duration = Duration.ofSeconds(5)

        /** The default TimeLimiter budget of the test clients - see [newFactory] for why it is not 1 s. */
        private val GENEROUS_BUDGET: Duration = Duration.ofSeconds(30)

        /** A caller-side MDC key, shaped like a host's correlation id, for the transport-line MDC tests. */
        private const val CORRELATION_KEY = "CORRELATION_ID"
        private const val CORRELATION_VALUE = "Out:4f1c9e0d2a7b6635"

        private lateinit var wireMock: WireMockServer

        /** The class's one request listener - registered when the server starts, cleared before every test. */
        private val arrivals = WireMockArrivals()

        @JvmStatic
        @BeforeAll
        fun startWireMock() {
            // Dynamic port keeps this test independent of any fixed-port service on the machine and of any other
            // WireMock instance, so it can run in the default build alongside everything else.
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
    }

    private val objectMapper: ObjectMapper = JsonMapper.builder().build()

    /** Factories built during a test; their shared connection pool is disposed in [tearDown]. */
    private val factories = mutableListOf<ResilientWebClientFactory>()

    /** Counts this test's in-flight subscriptions so [tearDown] can wait them out; see [InFlightCallBarrier]. */
    private val callBarrier = InFlightCallBarrier()

    private val clientLogger = LoggerFactory.getLogger(ResilientWebClient::class.java) as Logger

    /** The factory's logger - where shutdown's drain WARN is written. */
    private val factoryLogger = LoggerFactory.getLogger(ResilientWebClientFactory::class.java) as Logger

    /** The client logger's configured level, restored in [tearDown] after the barrier raised it to DEBUG. */
    private var previousClientLevel: Level? = null

    @BeforeEach
    fun resetStubs() {
        wireMock.resetAll()
        arrivals.clear()
    }

    /**
     * Installs the barrier BEFORE the test body, so every subscription it starts is counted.
     *
     * The client logger is raised to DEBUG for the duration because both lines the barrier counts are DEBUG
     * lines. That is a deliberate change of the whole class's baseline: the tests that manage this level
     * themselves save and restore it around their own bodies, so they now restore to DEBUG instead of to the
     * configured level - which is what [tearDown] then puts back.
     */
    @BeforeEach
    fun installCallBarrier() {
        previousClientLevel = clientLogger.level
        clientLogger.level = Level.DEBUG
        clientLogger.addAppender(callBarrier.apply { start() })
    }

    /**
     * Waits for this test's reactive work to finish BEFORE disposing anything, then tears down.
     *
     * The wait is the point. A test method returning is not the same as its calls being over: the client's
     * closing line is written in `doFinally`, and Reactor propagates the terminal signal DOWNSTREAM FIRST - so
     * `block()`/`awaitSingle()` releases the test thread while the event loop still has that emission ahead
     * of it. The logger is process-wide, so without this barrier a test's last line can land inside the NEXT
     * test's capture, where a test that asserts on its own backend's events finds a line of a call it never
     * made.
     *
     * Per-test scoping (every capture in this class names its own backend) is what makes a stray event
     * harmless; this barrier is what stops it from being emitted into a foreign test at all. Both are cheap,
     * and neither alone covers the other: scoping does not help a test that shares a backend name by accident,
     * and the barrier does not help against a capture that filters nothing.
     */
    @AfterEach
    fun tearDown() {
        try {
            assertThat(callBarrier.awaitSettled(BARRIER_TIMEOUT))
                .`as`("reactive work outlived the test: %s", callBarrier)
                .isTrue()
        } finally {
            clientLogger.detachAppender(callBarrier)
            callBarrier.stop()
            clientLogger.level = previousClientLevel
            factories.forEach { it.shutdown() }
            factories.clear()
        }
    }

    @Nested
    inner class `Requests on the wire` {
        @Test
        fun `should send the Host header derived from the triplet and the library's User-Agent`() {
            // What is tested: the headers the factory puts on every request of a client that an adapter never
            //   sees - the Host header that routes the sidecar to the upstream, and the User-Agent token that
            //   names this library to the peer.
            // Success criteria: WireMock receives a request whose Host header is the triplet's host, whose
            //   User-Agent is Veredium's token rather than Reactor Netty's default (the bare `veredium` here, since
            //   the classes directory carries no manifest version), and which carries the Accept the clients
            //   always send.
            // Why it matters: Reactor Netty sets its own User-Agent when none is present; whether an explicit one
            //   survives that is a property of the transport, and a peer's access log is the one place a library
            //   can say who called.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/hosted")).willReturn(okJson("""{"message":"hi"}""")))
            val client = newClient("hosted")

            // When
            client.get(Greeting::class.java) { it.uri("/hosted") }.block()

            // Then
            wireMock.verify(
                getRequestedFor(urlEqualTo("/hosted"))
                    .withHeader("Host", equalTo("localhost"))
                    .withHeader("User-Agent", equalTo(Veredium.userAgent()))
                    .withHeader("Accept", equalTo(MediaType.APPLICATION_JSON_VALUE)),
            )
        }

        @Test
        fun `should send the host's own User-Agent when one is configured`() {
            // What is tested: app.webclient.user-agent - a host naming itself to its peers instead of this library.
            // Success criteria: WireMock receives the configured token verbatim, not the library's.
            // Why it matters: the override exists for access logs read per caller; a value that quietly lost to the
            //   library's token would make the configuration a no-op.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/named")).willReturn(okJson("""{"message":"hi"}""")))
            val client = newClient("named", userAgent = "my-service/2.3")

            // When
            client.get(Greeting::class.java) { it.uri("/named") }.block()

            // Then
            wireMock.verify(getRequestedFor(urlEqualTo("/named")).withHeader("User-Agent", equalTo("my-service/2.3")))
        }

        @Test
        fun `should deserialise a successful GET response into the requested type`() {
            // What is tested: the happy path of get() end to end - request on the wire, JSON answer, the requested
            //   type out of the coroutine bridge.
            // Success criteria: awaitSingle() returns a Greeting carrying the stubbed message, and WireMock saw the
            //   GET on the path the block applied.
            // Why it matters: every other test in this class varies one thing against this baseline; without it a
            //   failure elsewhere could not be told apart from a pipeline that never delivered a value at all.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/hello")).willReturn(okJson("""{"message":"hi there"}""")))
            val client = newClient("greet")

            // When
            val result = runBlocking { client.get(Greeting::class.java) { it.uri("/hello") }.awaitSingle() }

            // Then
            assertThat(result.message).isEqualTo("hi there")
            wireMock.verify(getRequestedFor(urlEqualTo("/hello")))
        }

        @Test
        fun `should serialise the POST body once, send it on the wire, and deserialise the response`() {
            // What is tested: that post() serialises the body itself, the JSON reaches the backend, and the
            //   response is deserialised.
            // Success criteria: the backend received exactly `{"id":"X-100"}` as the request body, and the caller
            //   gets the Greeting the backend answered with.
            // Why it matters: post() serialises once and hands the STRING to the wire (bodyValue on a String) -
            //   verifying the received body proves the wire request is the JSON and was not, e.g., encoded a second
            //   time as a JSON string literal.
            //
            // Given
            wireMock.stubFor(post(urlEqualTo("/lookup")).willReturn(okJson("""{"message":"found"}""")))
            val client = newClient("lookup")

            // When
            val result =
                runBlocking {
                    client
                        .post(Greeting::class.java, LookupRequest("X-100")) {
                            it.uri("/lookup").contentType(MediaType.APPLICATION_JSON)
                        }.awaitSingle()
                }

            // Then
            assertThat(result.message).isEqualTo("found")
            wireMock.verify(
                postRequestedFor(urlEqualTo("/lookup"))
                    .withRequestBody(equalToJson("""{"id":"X-100"}""")),
            )
        }

        @Test
        fun `should signal a serialisation failure instead of throwing while the Mono is assembled`() {
            // What is tested: that a body Jackson cannot serialise produces an ERROR SIGNAL from the returned Mono,
            //   and that post() itself returns normally.
            // Success criteria: calling post(...) throws nothing - the assertion an eager serialisation fails -
            //   while subscribing surfaces the Jackson failure.
            // Why it matters: post() promises `Mono<T>`. Throwing while assembling breaks that promise for any
            //   caller that composes the Mono and subscribes later; the failure arrives by a path they never guarded
            //   because the signature said errors come as signals. The coroutine adapters cannot see the difference
            //   (they subscribe immediately), which is exactly why it needs a test rather than a reviewer.
            //
            // Given: a body whose accessor blows up during serialisation
            val client = newClient("unserialisable")

            // When: the Mono is assembled (the returned value is deliberately unused - assembly is what is observed)
            val assemblyFailure = catchThrowable { client.post(Greeting::class.java, Unserialisable()) { it.uri("/lookup") } }

            // Then: assembly is clean ...
            assertThat(assemblyFailure).`as`("post() must not throw while assembling the Mono").isNull()
            // ... and the failure arrives as a signal on subscription
            assertThatThrownBy { client.post(Greeting::class.java, Unserialisable()) { it.uri("/lookup") }.block() }
                .hasRootCauseInstanceOf(IllegalStateException::class.java)
        }

        @Test
        fun `should stamp the backend name onto every request as the adapter-name attribute`() {
            // What is tested: that a client of the factory carries its backend name in the request attribute
            //   ClientRequestLoggingFilter.ADAPTER_NAME_ATTRIBUTE - the key legatium's client logging reads the
            //   adapter name from - on a GET and on a POST alike, without the adapter doing anything for it.
            // Success criteria: a filter placed on the builder BEFORE the factory clones it (where a host
            //   customizer would sit) sees the attribute with the backend name on each request it observes.
            //   The filter is the only vantage point: an attribute never reaches the wire, so WireMock cannot
            //   see it, and that invisibility is precisely why it is an attribute and not a header.
            // Why it matters: behind the sidecar every request goes to the same host, so the URI does
            //   not tell backends apart. The attribute is what keeps the backend name on the exchange for an
            //   observer that is not this module - a silent regression here would merge all backends into one.
            //
            // Given: a builder carrying a filter that records the attribute, and a client of the factory built on it
            val seen = CopyOnWriteArrayList<Any?>()
            val recording =
                WebClient.builder().filter { request, next ->
                    seen += request.attribute(ClientRequestLoggingFilter.ADAPTER_NAME_ATTRIBUTE).orElse(null)
                    next.exchange(request)
                }
            wireMock.stubFor(get(urlEqualTo("/named")).willReturn(okJson("""{"message":"hi"}""")))
            wireMock.stubFor(post(urlEqualTo("/named")).willReturn(okJson("""{"message":"hi"}""")))
            val client = newFactory("named", webClientBuilder = recording).create("named")

            // When: one GET and one POST
            client.get(Greeting::class.java) { it.uri("/named") }.block()
            client.post(Greeting::class.java, LookupRequest("named")) { it.uri("/named") }.block()

            // Then: both requests carried the backend name under the attribute key
            assertThat(seen).containsExactly("named", "named")
        }
    }

    @Nested
    inner class `The HTTP client observation` {
        @Test
        fun `should open an http client observation per request when observation is enabled`() {
            // What is tested: the standalone observation toggle - with it ON, the factory keeps the (recording)
            //   ObservationRegistry that arrived on the injected builder, so each outbound call opens Spring
            //   WebClient's native `http.client.requests` observation. Proves the pass-through without a tracing
            //   bridge.
            // Success criteria: exactly one `http.client.requests` observation is recorded for one GET.
            // Why it matters: the factory clones the builder it is handed and decides per toggle whether the
            //   registry on it survives; were the ON branch to drop it too, the outbound calls would carry no client
            //   span at all, and nothing but a trace viewer would show it.
            //
            // Given: a factory-built client with observation ENABLED, on a builder carrying a recording registry
            val handler = RecordingObservationHandler()
            val registry = ObservationRegistry.create().apply { observationConfig().observationHandler(handler) }
            wireMock.stubFor(get(urlEqualTo("/hello")).willReturn(okJson("""{"message":"hi"}""")))
            val client = newObservingClient("greet-obs", registry, enabled = true)

            // When: a request is made
            runBlocking { client.get(Greeting::class.java) { it.uri("/hello") }.awaitSingle() }

            // Then: one http.client.requests observation was opened against our registry
            assertThat(handler.started.map { it.name }).containsExactly("http.client.requests")
        }

        @Test
        fun `should NOT open an http client observation when observation is disabled`() {
            // What is tested: the OFF branch of the same toggle - the registry arrives on the builder exactly as in
            //   the test above, and the factory replaces it with NOOP on its clone.
            // Success criteria: a GET through the client records no observation in the registry that was on the
            //   builder.
            // Why it matters: it is the counterpart that keeps the ON test honest - a factory that ignored the toggle
            //   and always kept the registry would pass that one. And off is the code default: a host that did not
            //   opt in must not get client spans from this module's calls.
            //
            // Given: a factory-built client with observation OFF, on a builder carrying a recording registry
            val handler = RecordingObservationHandler()
            val registry = ObservationRegistry.create().apply { observationConfig().observationHandler(handler) }
            wireMock.stubFor(get(urlEqualTo("/hello")).willReturn(okJson("""{"message":"hi"}""")))
            val client = newObservingClient("greet-noobs", registry, enabled = false)

            // When: a request runs
            runBlocking { client.get(Greeting::class.java) { it.uri("/hello") }.awaitSingle() }

            // Then: no observation was recorded - the factory replaced the registry with NOOP, so the client emits no span
            assertThat(handler.started).isEmpty()
        }

        @Test
        fun `should nest the http client observation under an active parent observation across the reactive call`() {
            // What is tested: with observation ON and io.micrometer:context-propagation on the classpath, an
            //   ambient parent observation (the stand-in for an inbound `http.server.requests` server span) is
            //   carried across the REAL reactive WebClient call - so the outbound `http.client.requests`
            //   observation nests as its CHILD instead of starting a fresh root.
            // Success criteria: the recorded `http.client.requests` context's parent is the parent observation
            //   (`inbound.server`) - the outbound half of one trace: server span -> client span.
            // Why it matters: WebFlux hops Reactor threads, so without the capture the client span would be a
            //   detached root and the outbound call would not join the caller's trace. This is the regression guard
            //   for `contextCapture()` in ResilientWebClient, proven over a real HTTP call. It runs WITHOUT Reactor's
            //   automatic context propagation (WebClientReactorContextPropagationConfiguration is not constructed here);
            //   the hook itself is pinned in WebClientReactorContextPropagationConfigurationTest.
            //
            // Given: a client with observation ON, and an ACTIVE parent observation
            val handler = RecordingObservationHandler()
            val registry = ObservationRegistry.create().apply { observationConfig().observationHandler(handler) }
            wireMock.stubFor(get(urlEqualTo("/hello")).willReturn(okJson("""{"message":"hi"}""")))
            val client = newObservingClient("greet-nest", registry, enabled = true)
            val parent = Observation.start("inbound.server", registry)

            // When: the request runs inside the parent's open scope
            parent.openScope().use {
                runBlocking { client.get(Greeting::class.java) { it.uri("/hello") }.awaitSingle() }
            }
            parent.stop()

            // Then: the client observation is a CHILD of the parent - the ambient observation propagated across the
            // reactive call rather than the client span starting its own root trace
            val clientContext = handler.started.single { it.name == "http.client.requests" }
            assertThat(clientContext.parentObservation).isNotNull()
            assertThat(clientContext.parentObservation!!.contextView.name).isEqualTo("inbound.server")
        }

        @Test
        fun `should nest the http client observation under an active parent observation for a POST`() {
            // What is tested: that the trace context crosses into the exchange for POST as well, where the request
            //   is built and executed behind a flatMap rather than at assembly time.
            // Success criteria: the recorded http.client.requests observation has the ambient parent, exactly
            //   as the GET case asserts.
            // Why it matters: execute() applies contextCapture() to its own Mono, and for a POST that Mono is
            //   subscribed by an outer flatMap instead of directly by the caller. A capture shifted off the
            //   subscribing thread would quietly start a detached root trace for every POST - invisible except in a
            //   trace viewer, and only for POST.
            //
            // Given: a client with observation ON, and an ACTIVE parent observation
            val handler = RecordingObservationHandler()
            val registry = ObservationRegistry.create().apply { observationConfig().observationHandler(handler) }
            wireMock.stubFor(post(urlEqualTo("/lookup")).willReturn(okJson("""{"message":"hi"}""")))
            val client = newObservingClient("post-nest", registry, enabled = true)
            val parent = Observation.start("inbound.server", registry)

            // When: the POST runs inside the parent's open scope
            parent.openScope().use {
                runBlocking {
                    client
                        .post(Greeting::class.java, LookupRequest("X-1")) {
                            it.uri("/lookup").contentType(MediaType.APPLICATION_JSON)
                        }.awaitSingle()
                }
            }
            parent.stop()

            // Then: the client observation is a CHILD of the parent
            val clientContext = handler.started.single { it.name == "http.client.requests" }
            assertThat(clientContext.parentObservation).isNotNull()
            assertThat(clientContext.parentObservation!!.contextView.name).isEqualTo("inbound.server")
        }
    }

    @Nested
    inner class `The response-size meter` {
        @Test
        fun `should record the decoded response body size so the codec limit can be sized from evidence`() {
            // What is tested: that every response that FITS the codec buffer contributes its decoded size to
            //   `webclient.response.body.size`, tagged with the adapter target - the same value the log lines carry as `adapter_name`.
            // Success criteria: one call returning a body containing MULTI-BYTE characters produces exactly
            //   one sample whose maximum is the UTF-8 BYTE size - strictly greater than the UTF-16 length, which
            //   is what makes the assertion about the unit rather than about the number. The meter carries the
            //   `adapter` tag and the `bytes` base unit.
            // Why it matters: `maxInMemorySize` is a ceiling somebody has to choose, and this meter is the evidence
            //   to choose it from: a DataBufferLimitException reports only that the limit was passed, not by how
            //   much, and legatium's exchange event carries a body only where its body-capture mode logs one. A
            //   limit set without that evidence rejects real responses that are merely larger than a guess.
            //
            // Given: a registry wired into the factory and a response of a known, non-trivial size
            val registry = SimpleMeterRegistry()
            // Umlauts on purpose: two UTF-8 bytes each, so byte size and String.length cannot coincide. Account
            // names are customer-set free text, which is exactly where these turn up in the real payload.
            val body = """{"message":"${"\u00e4".repeat(200)}${"y".repeat(300)}"}"""
            wireMock.stubFor(get(urlEqualTo("/sized")).willReturn(okJson(body)))
            val client = newClient("sized", meterRegistry = registry)

            // When
            runBlocking { client.get(Greeting::class.java) { it.uri("/sized") }.awaitSingle() }

            // Then
            val summary =
                registry
                    .find(WebClientMetrics.RESPONSE_BODY_SIZE)
                    .tag(ResponseBodyMetrics.ADAPTER_TAG, "sized")
                    .summary()
            assertThat(summary).isNotNull()
            assertThat(summary!!.count()).isEqualTo(1)
            assertThat(summary.max()).isEqualTo(body.toByteArray(Charsets.UTF_8).size.toDouble())
            assertThat(summary.max()).isGreaterThan(body.length.toDouble())
            assertThat(summary.id.baseUnit).isEqualTo(ResponseBodyMetrics.BASE_UNIT)
        }

        @Test
        fun `should leave the response-size meter unregistered when the host wired no registry`() {
            // What is tested: that the meter is optional in the same sense the validator and the observation
            //   registry are - absent host wiring means absent meter, not a failure and not a global-registry
            //   fallback.
            // Success criteria: a client built without a MeterRegistry serves a call normally, and a fresh
            //   registry stays empty afterwards.
            // Why it matters: the factory is constructed directly in several tests and in minimal
            //   contexts. A meter that silently reached for Metrics.globalRegistry would cross-contaminate those
            //   and would tie this module to process-global state it does not own.
            //
            // Given
            val registry = SimpleMeterRegistry()
            wireMock.stubFor(get(urlEqualTo("/unmetered")).willReturn(okJson("""{"message":"ok"}""")))
            val client = newClient("unmetered")

            // When
            runBlocking { client.get(Greeting::class.java) { it.uri("/unmetered") }.awaitSingle() }

            // Then
            assertThat(registry.find(WebClientMetrics.RESPONSE_BODY_SIZE).summary()).isNull()
        }
    }

    @Nested
    inner class `Error statuses and empty bodies` {
        @Test
        fun `should surface a non-2xx response as a WebClientResponseException carrying the status`() {
            // What is tested: the error shape a caller sees for a non-2xx answer.
            // Success criteria: a 404 with a text body fails awaitSingle() with a WebClientResponseException whose
            //   status is 404 - not with a Jackson error about the body, not with an empty result.
            // Why it matters: the adapters on top branch on the status (a documented 404 is a business answer), and
            //   the breaker verdicts and the error line's severity read it off this very exception type; another
            //   shape would silently turn every rejection into an unexpected failure.
            //
            // Given: retrieve() raises for every non-2xx, so the caller sees a typed exception, not an empty body.
            wireMock.stubFor(get(urlEqualTo("/missing")).willReturn(aResponse().withStatus(404).withBody("nope")))
            val client = newClient("err")

            // When / Then
            assertThatThrownBy {
                runBlocking { client.get(Greeting::class.java) { it.uri("/missing") }.awaitSingle() }
            }.isInstanceOfSatisfying(WebClientResponseException::class.java) { ex ->
                assertThat(ex.statusCode.value()).isEqualTo(404)
            }
        }

        @Test
        fun `should fail a 204 answer with EmptyResponseBodyException after the breaker recorded a success`() {
            // What is tested: the response contract for a 2xx without a body, through the real pipeline - a 204
            //   No Content against a caller that asked for a Greeting.
            // Success criteria: the call fails with EmptyResponseBodyException naming the backend and the expected
            //   type; the breaker recorded the exchange as one SUCCESSFUL call and no failure; and the client's error
            //   line for it is WARN and carries the exception.
            // Why it matters: a Mono that completed EMPTY here would write no error line and fail awaitSingle()
            //   with a NoSuchElementException naming neither the backend nor the cause, while the blocking twin
            //   fails the same answer. Raising it AFTER the decoration is what keeps the breaker right: the
            //   backend answered promptly with a success status, which is not an outage.
            //
            // Given: a backend answering 204, and the client logger capturing (at DEBUG, set by the barrier)
            wireMock.stubFor(get(urlEqualTo("/no-content")).willReturn(aResponse().withStatus(204)))
            val circuitBreakers = CircuitBreakerRegistry.ofDefaults()
            val client = newClient("no-content", circuitBreakers = circuitBreakers)
            capturing(clientLogger, accept = naming("backend 'no-content'")) { lines ->
                // When
                val failure = catchThrowable { runBlocking { client.get(Greeting::class.java) { it.uri("/no-content") }.awaitSingle() } }

                // Then: a named failure ...
                assertThat(failure).isInstanceOfSatisfying(EmptyResponseBodyException::class.java) { empty ->
                    assertThat(empty.backendName).isEqualTo("no-content")
                    assertThat(empty.responseType).isEqualTo(Greeting::class.java)
                }
                // ... that the breaker never saw as one ...
                val metrics = circuitBreakers.circuitBreaker("no-content").metrics
                assertThat(metrics.numberOfSuccessfulCalls).isEqualTo(1)
                assertThat(metrics.numberOfFailedCalls).isZero()
                // ... and that the error line reports
                val line = lines.awaitCall().single { it.formattedMessage.startsWith(ERROR_PREFIX) }
                assertThat(line.level).isEqualTo(Level.WARN)
                assertThat(line.throwableProxy?.className).isEqualTo(EmptyResponseBodyException::class.java.name)
            }
        }

        @Test
        fun `should fail an empty 200 answer with EmptyResponseBodyException as well`() {
            // What is tested: the second shape of the same contract - a 200 whose entity is empty
            //   (Content-Length: 0), which a misbehaving backend or proxy produces where a JSON body was promised.
            // Success criteria: the call fails with EmptyResponseBodyException naming the backend, rather than
            //   completing empty or failing in Jackson.
            // Why it matters: the 204 is the honest form of "no body"; the empty 200 is the one that actually turns
            //   up in an incident, and it reaches the client by the same empty `bodyToMono` - the guard must not
            //   depend on the status code.
            //
            // Given
            wireMock.stubFor(
                get(urlEqualTo("/empty-ok")).willReturn(aResponse().withStatus(200).withHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)),
            )
            val client = newClient("empty-ok")

            // When
            val failure = catchThrowable { client.get(Greeting::class.java) { it.uri("/empty-ok") }.block() }

            // Then
            assertThat(failure)
                .isInstanceOf(EmptyResponseBodyException::class.java)
                .hasMessageContaining("'empty-ok'")
                .hasMessageContaining("Greeting")
        }
    }

    @Nested
    inner class `The resilience decoration` {
        @Test
        fun `should fail a call that exceeds the time-limiter budget on a slow backend`() {
            // What is tested: that the TimeLimiter of the decoration - and not the transport - releases the caller
            //   of a stalled call, and that the breaker counts it.
            // Success criteria: against a 3 s backend and a 300 ms budget the call fails with exactly the
            //   TimeLimiter's TimeoutException (its message names the 300 ms), and the breaker records one failure.
            //   The transport cannot be the one that fired: the endpoint's response timeout is derived from its
            //   1000 ms anchor and sits at 1400 ms (asserted as the premise), far past the budget.
            // Why it matters: the budget is the only bound on a caller's wait that the mesh ladder controls. Hence
            //   the exact type: a test that also accepted a WebClientRequestException - which the transport raises
            //   at 1400 ms - would pass with the TimeLimiter missing from the pipeline, a little later, while
            //   claiming the budget released the caller.
            //
            // Given: a 3s backend delay against a 300ms budget
            wireMock.stubFor(
                get(urlEqualTo("/slow")).willReturn(okJson("""{"message":"late"}""").withFixedDelay(3_000)),
            )
            val circuitBreakers = CircuitBreakerRegistry.ofDefaults()
            val client =
                newClient(
                    "slow",
                    timeLimiter =
                        TimeLimiterConfig
                            .custom()
                            .timeoutDuration(Duration.ofMillis(300))
                            .cancelRunningFuture(true)
                            .build(),
                    circuitBreakers = circuitBreakers,
                )
            assertThat(client.responseTimeout).`as`("the transport's own guard, which must not be the one firing").isEqualTo(Duration.ofMillis(1400))

            // When
            val failure = catchThrowable { runBlocking { client.get(Greeting::class.java) { it.uri("/slow") }.awaitSingle() } }

            // Then
            assertThat(failure).isExactlyInstanceOf(TimeoutException::class.java).hasMessageContaining("300ms")
            assertThat(circuitBreakers.circuitBreaker("slow").metrics.numberOfFailedCalls).isEqualTo(1)
        }

        @Test
        fun `should open the circuit and short-circuit further calls after the failure threshold is reached`() {
            // What is tested: that the circuit breaker observes the decorated calls and opens once the failure
            //   rate is exceeded, then rejects the next call locally.
            // Success criteria: after five 500s in a window of five, the sixth call fails with
            //   CallNotPermittedException.
            // Why it matters: this proves the breaker is actually in the call path of a real exchange and records
            //   HTTP failures, which is the whole point of the decoration (the operator order itself is pinned in
            //   ReactiveResilienceDecoratorTest).
            //
            // Given: a window of 5 calls, opening at a 50% failure rate, against an always-500 backend.
            wireMock.stubFor(get(urlEqualTo("/flaky")).willReturn(aResponse().withStatus(500)))
            val client =
                newClient(
                    "cb",
                    circuitBreaker =
                        CircuitBreakerConfig
                            .custom()
                            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                            .slidingWindowSize(5)
                            .minimumNumberOfCalls(5)
                            .failureRateThreshold(50f)
                            .waitDurationInOpenState(Duration.ofSeconds(10))
                            .build(),
                )

            // When: five calls fail (100% failure rate) and trip the breaker.
            repeat(5) {
                runCatching { runBlocking { client.get(Greeting::class.java) { it.uri("/flaky") }.awaitSingle() } }
            }

            // Then: the sixth call is rejected by the open breaker before it ever reaches the backend.
            assertThatThrownBy {
                runBlocking { client.get(Greeting::class.java) { it.uri("/flaky") }.awaitSingle() }
            }.isInstanceOf(CallNotPermittedException::class.java)
        }

        @Test
        fun `should not count a response exceeding the buffer limit against the circuit breaker`() {
            // What is tested: that a response too large for maxInMemorySize does not open the circuit - through the
            //   REAL pipeline, so the exception is the one the client actually produces rather than one written by
            //   hand in a test.
            // Success criteria: five oversized responses in a row all fail for the caller, while the breaker
            //   records zero failures and stays closed.
            // Why it matters: the unit test for this rule asserts against a WebClientResponseException that the
            //   test itself constructs with a DataBufferLimitException as cause. If the real shape ever differed -
            //   another wrapper, a different cause depth - that test would keep passing while the rule silently
            //   stopped matching. This is the one that would notice. And the rule matters because the backend is
            //   blameless here: it answered correctly, with a 200, and this client declined to buffer the answer.
            //   Counting that would open the circuit against a healthy backend.
            //
            // Given: a breaker that would open readily, a tight buffer limit, and a response well past it
            val circuitBreakerRegistry =
                CircuitBreakerRegistry.of(
                    CircuitBreakerConfig
                        .custom()
                        .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                        .slidingWindowSize(5)
                        .minimumNumberOfCalls(5)
                        .failureRateThreshold(50f)
                        .build(),
                )
            val backendName = "oversized"
            wireMock.stubFor(get(urlEqualTo("/big")).willReturn(okJson("""{"message":"${"X".repeat(4000)}"}""")))
            val meshProperties =
                MeshProperties(
                    defaults = MeshProperties.Defaults(individualRequestTimeoutMs = 1000),
                    endpoints =
                        listOf(
                            MeshProperties.EndpointConfig(
                                triplet = "/endpoint/localhost/GET/$backendName",
                                alias = backendName,
                                baseUrl = wireMock.baseUrl(),
                            ),
                        ),
                )
            val client =
                ResilientWebClientFactory(
                    WebClientProperties(maxInMemorySize = DataSize.ofBytes(200)),
                    meshProperties,
                    ReactiveResilienceDecorator(
                        circuitBreakerRegistry,
                        // Deliberately generous: the DEFAULT 1s budget makes this test timing-dependent, because a
                        // slow call under load would fail with a TimeoutException instead - which the breaker
                        // rightly DOES record, silently inverting the assertion below into a flake.
                        TimeLimiterRegistry.of(TimeLimiterConfig.custom().timeoutDuration(Duration.ofSeconds(30)).build()),
                        BulkheadRegistry.ofDefaults(),
                    ),
                    objectMapper,
                    // Deliberately no Spring context here: a bare builder stands in for Boot's bean.
                    webClientBuilder = WebClient.builder(),
                ).also { factories += it }.create(backendName)
            // The verdict is installed by the mesh customizer while Resilience4j builds the instance - applied here
            // the way the Boot configuration applies it at startup.
            CustomizedRegistries.circuitBreakers(circuitBreakerRegistry, meshProperties, WebClientVerdicts.breakerVerdicts)

            // When: five calls all trip the buffer limit
            repeat(5) {
                val failure = catchThrowable { client.get(Greeting::class.java) { it.uri("/big") }.block() }
                // Assert the REASON, not merely that it failed: if the call ever fails for another reason - a
                // timeout, say - that is a different scenario and the counter assertion below would not mean
                // what it claims. Better a loud failure naming the cause than a test that quietly stops testing.
                assertThat(failure)
                    .`as`("the call must fail on the buffer limit, not for some other reason")
                    .isNotNull()
                    .hasRootCauseInstanceOf(DataBufferLimitException::class.java)
            }

            // Then: the breaker held none of it against the backend
            val breaker = circuitBreakerRegistry.circuitBreaker(backendName)
            assertThat(breaker.metrics.numberOfFailedCalls).isZero()
            assertThat(breaker.state).isEqualTo(CircuitBreaker.State.CLOSED)
        }

        @Test
        fun `should expose the time-limiter timeout as the request budget`() {
            // What is tested: ResilientWebClient.timeLimiterTimeout - the budget read back from the TimeLimiter
            //   registry rather than held as a copy.
            // Success criteria: a client whose TimeLimiter is configured with 7 s reports 7 s.
            // Why it matters: the transport lines print this number next to the response timeout, and a reader
            //   compares the two to see which guard fired; a value from anywhere but the registry the decoration
            //   uses would make that comparison lie.
            //
            // Given
            val client =
                newClient(
                    "budget",
                    timeLimiter = TimeLimiterConfig.custom().timeoutDuration(Duration.ofSeconds(7)).build(),
                )

            // When
            val budget = client.timeLimiterTimeout

            // Then
            assertThat(budget).isEqualTo(Duration.ofSeconds(7))
        }
    }

    @Nested
    inner class `The factory's client cache` {
        @Test
        fun `should return the same cached client instance for repeated create calls`() {
            // What is tested: that create() is idempotent per backend name.
            // Success criteria: two calls for the same alias return the very same instance.
            // Why it matters: adapters call create() in their constructors; a fresh client per call would build a
            //   fresh WebClient and register its response-size meter again each time - per backend there must be
            //   exactly one.
            //
            // Given
            val factory = newFactory("product-agreements")

            // When
            val first = factory.create("product-agreements")
            val second = factory.create("product-agreements")

            // Then
            assertThat(first).isSameAs(second)
        }

        @Test
        fun `should fail fast when creating a client for an unconfigured target`() {
            // What is tested: create() for an alias no app.mesh endpoint declares.
            // Success criteria: an IllegalStateException naming the unknown alias, at create() time.
            // Why it matters: adapters create their client at startup, so this turns a typo in an alias into a
            //   failed start that names it - rather than a client with no base URL and no ladder, failing at the
            //   first call with a connection error that points nowhere near the configuration.
            //
            // Given: a factory that only knows about "configured".
            val factory = newFactory("configured")

            // When / Then
            assertThatThrownBy { factory.create("unknown-target") }
                .isInstanceOf(IllegalStateException::class.java)
                .hasMessageContaining("unknown-target")
        }
    }

    @Nested
    inner class `The client's own log lines` {
        @Test
        fun `should keep a cancelled call at DEBUG, leaving the operator signal to the exchange event`() {
            // What is tested: where a cancellation is reported on THIS side. The per-call line must be DEBUG - not
            //   merely absent from WARN, because an accidental drop to TRACE or a removal would pass that weaker
            //   assertion.
            // Success criteria: the single "Cancelled request" line from ResilientWebClient is at DEBUG.
            // Why it matters: it pins a TRADE, not a removal. A cancellation is the subscriber going
            //   away (its budget expired, its client disconnected) - normal, correlated with load, and therefore
            //   arriving in floods on WARN, the level operators alert on, exactly when the lines that matter are
            //   needed. Demoting it is only safe because legatium's exchange event carries the same fact at WARN
            //   with `adapter_outcome=cancelled` - the other half of the trade, pinned in
            //   LegatiumExchangeLoggingIntegrationTest against the real filter on Boot's builder.
            //
            // Given: a backend that does not answer within the test's window, and the client logger capturing
            wireMock.stubFor(get(urlEqualTo("/never")).willReturn(okJson("{}").withFixedDelay(30_000)))
            // Captures an asynchronously written line: dispose() returns immediately, while doOnCancel runs on the
            // event loop afterwards - so the line is awaited, not read.
            capturing(clientLogger, Level.DEBUG, accept = naming("backend 'cancelled'")) { clientEvents ->
                val client = newClient("cancelled")

                // When: the subscriber walks away before the answer arrives
                client.get(Greeting::class.java) { it.uri("/never") }.subscribe().dispose()

                // Then: the per-call line is DEBUG
                val cancelLine = clientEvents.awaitCall().single { it.formattedMessage.startsWith("Cancelled request") }
                assertThat(cancelLine.level)
                    .`as`("a cancellation is the caller's business, not an operational fault")
                    .isEqualTo(Level.DEBUG)
            }
        }

        @Test
        fun `should log a benign 4xx at DEBUG and without a stack trace`() {
            // What is tested: the second consumer of HttpExchangeErrorPolicy - the severity of the client's OWN
            //   error line. A benign 4xx (the documented "does not exist" answer) must be DEBUG, and it must carry
            //   NO stack trace.
            // Success criteria: the captured event is at DEBUG, its throwable proxy is absent, and the message
            //   carries the `expected client error` marker that names the status for a reader.
            // Why it matters: at WARN this line would make every routine 404 look like an outage. The stack trace
            //   is the second half of that: legatium's exchange event already reports the same exchange with its
            //   status, URL and body, and a trace here would add, per business case, a reactive stack that explains
            //   nothing the status and URL do not. The breaker's ignore predicate and this line derive from ONE
            //   predicate precisely so they cannot disagree; the breaker end is pinned in WebClientVerdictsTest.
            //
            // Given / When: the backend answers "not found"
            val line = errorLineFor(status = 404, backendName = "benign-4xx")

            // Then
            assertThat(line.level).isEqualTo(Level.DEBUG)
            assertThat(line.throwableProxy)
                .`as`("a business-case rejection needs no stack trace - legatium's exchange event reports the exchange")
                .isNull()
            assertThat(line.formattedMessage).contains("expected client error")
        }

        @Test
        fun `should log a 5xx at WARN and with the stack trace`() {
            // What is tested: the severity of the client's error line for a server error.
            // Success criteria: the line is WARN, carries the 500's exception as its cause, and has no
            //   `expected client error` marker.
            // Why it matters: the counterpart that keeps the test above honest - without it, a line that lost its
            //   severity AND its cause for every failure would satisfy the DEBUG/no-stack-trace assertions.
            //
            // Given / When: the backend answers 500
            val line = errorLineFor(status = 500, backendName = "server-error")

            // Then
            assertThat(line.level).isEqualTo(Level.WARN)
            assertThat(line.throwableProxy?.className).isEqualTo(WebClientResponseException.InternalServerError::class.java.name)
            assertThat(line.formattedMessage).doesNotContain("expected client error")
        }

        @Test
        fun `should keep a 408 at WARN although it is a 4xx`() {
            // What is tested: that the boundary INSIDE the 4xx range holds on this side too - 408 Request Timeout
            //   is an operational signal, not a business case.
            // Success criteria: the line is WARN with a stack trace, i.e. treated exactly like a 5xx, while the
            //   404 above proves the same code path does demote a benign status.
            // Why it matters: a demoted 408 would hide a backend timing out under load in DEBUG, the one level
            //   nobody runs in production - and it would do so silently, because the exchange still "fails
            //   correctly" from the caller's point of view. The same status set drives the circuit breaker's ignore
            //   predicate, so a drift here would mean the logs and the breaker disagree about what an outage is.
            //
            // Given / When: the backend answers 408
            val line = errorLineFor(status = 408, backendName = "request-timeout")

            // Then
            assertThat(line.level).isEqualTo(Level.WARN)
            assertThat(line.throwableProxy).isNotNull()
            assertThat(line.formattedMessage).doesNotContain("expected client error")
        }

        @Test
        fun `should carry the backend name and the URI template on a transport line under the library's field names`() {
            // What is tested: the two structured fields every transport line of this client carries, and that
            //   their NAMES are the exchange event's - `adapter_name` and `adapter_url_template` as legatium spells
            //   them - so one dashboard field groups both loggers.
            // Success criteria: the error line for a 500 carries both key-values with the backend alias and
            //   the endpoint's template; and `adapter_target_name`, a name of the module's own, is absent.
            // Why it matters: these names leave the process and are a contract with the log index (an operator's
            //   queries group by them). The module writes only these two, taken from the
            //   library's ClientLogField, so this line is the whole pin.
            //
            // Given / When: the backend answers 500
            val line = errorLineFor(status = 500, backendName = "joined")

            // Then
            val fields = line.keyValuePairs.associate { it.key to it.value }
            assertThat(fields).containsEntry("adapter_name", "joined")
            assertThat(fields).containsEntry("adapter_url_template", "/joined")
            assertThat(fields).doesNotContainKey("adapter_target_name")
        }

        @Test
        fun `should report the elapsed time in the failure log from the call's own clock`() {
            // What is tested: that the duration in the error line is measured by the call itself, on its own
            //   clock, whatever level any other logger runs at.
            // Success criteria: the WARN line renders `duration so far: <n>ms` with digits. The assertion is
            //   on the SHAPE, not on a value: the failure it catches is a literal "null" in a field that announces
            //   a number, and no timing knowledge is needed to catch that.
            // Why it matters: the WARN line is this module's only trace of the failure, and "duration
            //   so far: nullms" withheld the one thing it could still answer - whether the call failed early or ran
            //   into its budget. It also breaks any consumer that parses the field as a number.
            //
            // Given: a failing backend and the client logger capturing
            wireMock.stubFor(get(urlEqualTo("/boom")).willReturn(aResponse().withStatus(500)))
            // The WARN failure line itself is the terminal marker here: it is the line under test, and the
            // client logs it at any level it may run at.
            capturing(clientLogger, accept = naming("backend 'own-clock-duration'")) { appender ->
                val client = newClient("own-clock-duration")

                // When: the call fails
                runCatching { client.get(Greeting::class.java) { it.uri("/boom") }.block() }

                // Then: the failure line carries a measured duration, not a null
                val message = appender.awaitCall(ERROR_PREFIX).single { it.formattedMessage.startsWith(ERROR_PREFIX) }.formattedMessage
                assertThat(message).containsPattern("duration so far: \\d+ms")
            }
        }
    }

    @Nested
    inner class `The caller's MDC on the client's lines` {
        @Test
        fun `should write the error line under the subscribing caller's MDC and leave the event loop's MDC clean`() {
            // What is tested: that the transport lines - written in signal hooks on a Netty event loop - carry the
            //   MDC the caller had when it SUBSCRIBED, without Reactor's automatic context propagation (this class
            //   never enables it), and that the event loop does not keep the caller's keys afterwards.
            // Success criteria: the error line of a 500 is logged on a thread other than the caller's and carries the
            //   caller's correlation key; and an operator placed right after the client's error hook, which runs on
            //   that same event-loop thread once the hook is done, finds the key gone again.
            // Why it matters: the error line is often the only trace of a failed call on this side, and without the
            //   key it cannot be joined to the request that caused it - in the default configuration, where the
            //   HTTP observation (and with it the propagation hook) is off. A snapshot that leaked onto the event
            //   loop would be worse than none: the loop serves other callers next, and their lines would carry this
            //   caller's correlation id.
            //
            // Given: a failing backend, the client logger capturing, and the caller's key in its MDC
            wireMock.stubFor(get(urlEqualTo("/mdc-error")).willReturn(aResponse().withStatus(500)))
            val client = newClient("mdc-error")
            val afterHook = AtomicReference<Pair<Thread, Map<String, String>?>>()
            capturing(clientLogger, accept = naming("backend 'mdc-error'")) { lines ->
                MDC.put(CORRELATION_KEY, CORRELATION_VALUE)
                try {
                    // When: the caller subscribes on this thread
                    runCatching {
                        client
                            .get(Greeting::class.java) { it.uri("/mdc-error") }
                            .doOnError { afterHook.set(Thread.currentThread() to MDC.getCopyOfContextMap()) }
                            .block()
                    }
                } finally {
                    MDC.remove(CORRELATION_KEY)
                }

                // Then: the line was written elsewhere, under the caller's key ...
                val line = lines.awaitCall().single { it.formattedMessage.startsWith(ERROR_PREFIX) }
                assertThat(line.threadName).isNotEqualTo(Thread.currentThread().name)
                assertThat(line.mdcPropertyMap).containsEntry(CORRELATION_KEY, CORRELATION_VALUE)
                // ... and the thread it was written on holds no trace of the caller afterwards
                val (eventLoop, mdcAfter) = afterHook.get()
                assertThat(eventLoop.name).`as`("the premise: the probe ran on the thread that wrote the line").isEqualTo(line.threadName)
                assertThat(mdcAfter.orEmpty()).doesNotContainKey(CORRELATION_KEY)
            }
        }

        @Test
        fun `should carry a coroutine MDCContext key onto the line of a TimeLimiter timeout`() {
            // What is tested: the same promise for a coroutine caller - the shape the adapters on top have - whose
            //   keys live in a coroutine MDCContext, and for a line written on yet another thread: the TimeLimiter's
            //   timeout fires on Reactor's timer thread, which carries no MDC of its own either.
            // Success criteria: the error line of the timed-out call is the TimeoutException's, is logged on a
            //   thread other than the caller's, and carries the key the caller's MDCContext set.
            // Why it matters: MDCContext writes the keys onto whichever thread RESUMES the coroutine; the subscription
            //   inside awaitSingle() happens on it, so the snapshot sees them - this pins that the adapters' calling
            //   convention reaches the snapshot at all. The timeout line is also the one an operator searches for
            //   first when a caller reports a slow failure.
            //
            // Given: a backend far slower than a 200 ms budget, and the client logger capturing
            wireMock.stubFor(get(urlEqualTo("/mdc-slow")).willReturn(okJson("{}").withFixedDelay(2_000)))
            val client = newClient("mdc-slow", timeLimiter = TimeLimiterConfig.custom().timeoutDuration(Duration.ofMillis(200)).build())
            capturing(clientLogger, accept = naming("backend 'mdc-slow'")) { lines ->
                // When: a coroutine caller with its key in MDCContext
                runCatching {
                    runBlocking(MDCContext(mapOf(CORRELATION_KEY to CORRELATION_VALUE))) {
                        client.get(Greeting::class.java) { it.uri("/mdc-slow") }.awaitSingle()
                    }
                }

                // Then
                val line = lines.awaitCall().single { it.formattedMessage.startsWith(ERROR_PREFIX) }
                assertThat(line.throwableProxy?.className).isEqualTo(TimeoutException::class.java.name)
                assertThat(line.threadName).isNotEqualTo(Thread.currentThread().name)
                assertThat(line.mdcPropertyMap).containsEntry(CORRELATION_KEY, CORRELATION_VALUE)
            }
        }
    }

    @Nested
    inner class `Shutdown of the connection pool` {
        @Test
        fun `should drain the pool on shutdown without raising out of the destroy callback`() {
            // What is tested: that shutdown() completes normally when the pool holds a live keep-alive connection,
            //   and that calling it twice is harmless.
            // Success criteria: neither call throws, and the exchange that populated the pool did reach the
            //   backend - so there genuinely was a connection to dispose rather than an empty pool.
            // Why it matters: shutdown() runs as a @PreDestroy, where Spring can only catch what it
            //   throws and log it as a destroy method that failed. A drain deadline reported as an
            //   IllegalStateException out of block(Duration) would be indistinguishable from a real disposal error,
            //   and the last thing in the log before the process exits. The deadline is therefore an outcome,
            //   reported as a WARN naming the engine - that branch is pinned by "should WARN and complete when the
            //   pool does not drain within the derived timeout", an in-flight call by "should let an in-flight call
            //   finish before the pool is disposed, without the drain WARN".
            //
            // Given: one completed exchange, which leaves its connection in the pool (keep-alive is on)
            wireMock.stubFor(get(urlEqualTo("/bye")).willReturn(okJson("""{"message":"bye"}""")))
            val factory = newFactory("drain")
            val client = factory.create("drain")
            runBlocking { client.get(Greeting::class.java) { it.uri("/bye") }.awaitSingle() }

            // When: the context is torn down
            val firstShutdown = catchThrowable { factory.shutdown() }
            val secondShutdown = catchThrowable { factory.shutdown() }

            // Then
            assertThat(firstShutdown).`as`("draining a pool with a live connection must not raise").isNull()
            assertThat(secondShutdown).`as`("shutdown must stay idempotent - Spring may call it once, tests twice").isNull()
            wireMock.verify(getRequestedFor(urlEqualTo("/bye")))
        }

        @Test
        fun `should let an in-flight call finish before the pool is disposed, without the drain WARN`() {
            // What is tested: shutdown() while a call holds a connection - what the disposal does to it, and that
            //   the bounded wait does not report a drain failure for it.
            // Success criteria: with a request already at the backend (a WireMock request listener says so), the
            //   call has completed with the backend's answer when shutdown() returns; the factory logs no "did not
            //   drain" WARN.
            // Why it matters: it pins what shutdown() waits for. The pool is disposed with Reactor Netty's graceful
            //   disposeTimeout, so the disposal waits until the leased connection came back - a graceful shutdown
            //   lets the calls in flight finish, as on every other engine. Were the disposal to complete at once, the
            //   call would still be running when shutdown() returns; were it to cut leased connections, the answer
            //   would be lost.
            //
            // Given: an answer 1 s late - inside the endpoint's 1400 ms response timeout, so the transport lets it
            // through - a listener that fires once the request reached WireMock, and the factory logger capturing
            wireMock.stubFor(get(urlEqualTo("/in-flight")).willReturn(okJson("""{"message":"late"}""").withFixedDelay(1_000)))
            val arrived = arrivals.arrivalOf("/in-flight")
            val factory = newFactory("in-flight")
            val client = factory.create("in-flight")
            capturing(factoryLogger, accept = naming("did not drain")) { factoryLines ->
                val call = client.get(Greeting::class.java) { it.uri("/in-flight") }.toFuture()
                assertThat(arrived.await(5, TimeUnit.SECONDS)).`as`("the request must reach the backend first").isTrue()

                // When
                factory.shutdown()

                // Then: shutdown waited for the call, and the call was not cut
                assertThat(call.isDone).`as`("the disposal waits for the call in flight").isTrue()
                assertThat(call.get(0, TimeUnit.SECONDS)?.message).isEqualTo("late")
                assertThat(factoryLines.events).isEmpty()
            }
        }

        @Test
        fun `should WARN and complete when the pool does not drain within the derived timeout`() {
            // What is tested: the expiry branch of shutdown()'s bounded wait, through the seam that takes the
            //   disposal and the timer as arguments: a disposal that never completes, on a virtual clock.
            // Success criteria: the timeout is the derived one (the endpoint's response timeout + connect +
            //   pending-acquire + 1 s); one millisecond before it nothing has happened; at it, the wait COMPLETES
            //   normally - no exception - and the factory logs one WARN naming the engine and the timeout.
            // Why it matters: the branch exists so that a pool that will not drain ends shutdown with a statement,
            //   not with an IllegalStateException Spring logs as a failed destroy method - and so that it ends at
            //   all. The real pool drains in milliseconds, so without the seam the branch could only be reached by
            //   holding a connection open past a window longer than any response timeout.
            //
            // Given: a factory, a disposal that never ends, a virtual clock, and the factory logger capturing
            val factory = newFactory("stuck-pool")
            val responseTimeout = factory.create("stuck-pool").responseTimeout
            val timeout = factory.poolDisposeTimeout
            assertThat(timeout).isEqualTo(responseTimeout.plusMillis(100 + 200).plusSeconds(1))
            val clock = VirtualTimeScheduler.create()
            try {
                capturing(factoryLogger, accept = naming("did not drain")) { factoryLines ->
                    // When: the wait starts and the clock runs to just before the timeout
                    val drained = factory.drain(Mono.never(), clock).toFuture()
                    clock.advanceTimeBy(timeout.minusMillis(1))

                    // Then: still waiting, nothing said
                    assertThat(drained.isDone).isFalse()
                    assertThat(factoryLines.events).isEmpty()

                    // When: the timeout is reached
                    clock.advanceTimeBy(Duration.ofMillis(1))

                    // Then: completed normally, with the WARN
                    assertThat(drained.isDone).isTrue()
                    assertThat(drained.isCompletedExceptionally).isFalse()
                    val warn = factoryLines.events.single()
                    assertThat(warn.level).isEqualTo(Level.WARN)
                    assertThat(warn.formattedMessage).contains("HTTP engine 'reactor-netty'", "did not drain within $timeout")
                }
            } finally {
                clock.dispose()
            }
        }
    }

    @Nested
    inner class `The in-flight call barrier` {
        @Test
        fun `should count a real call, so the barrier's prefixes cannot drift from the lines the client logs`() {
            // What is tested: that `InFlightCallBarrier` observes the lines a REAL exchange produces - not lines
            //   this test wrote for it.
            // Success criteria: after one genuine call, a barrier attached to the client's own logger reports
            //   exactly one started and one finished subscription.
            // Why it matters: it is the only thing tying `EXECUTING_PREFIX` to production. The sibling
            //   self-tests below drive the barrier with log lines they build from that same constant, so they stay
            //   green if the client renames its line - the constant moves on both sides at once and they never
            //   notice. The barrier would then count 0 started against N finished, `finished >= started` would hold
            //   from the outset, and tearDown would wave every test through while the class leaked events into its
            //   successors - silently, with nothing red. (SIGNAL_PREFIX is additionally pinned by
            //   `errorLineFor` and the cancellation test, which await it after real calls; this covers the other
            //   half.) Renaming either constant fails this test and only this test.
            //
            // Given: a backend, and a barrier watching the client's own logger
            wireMock.stubFor(get(urlEqualTo("/hello")).willReturn(okJson("""{"message":"hi"}""")))
            val barrier = InFlightCallBarrier().apply { start() }
            clientLogger.addAppender(barrier)
            try {
                // When: one real exchange runs to completion
                runBlocking { newClient("barrier-count").get(Greeting::class.java) { it.uri("/hello") }.awaitSingle() }

                // Then: the barrier saw both halves of it - the subscription and its terminal signal
                assertThat(barrier.awaitSettled(BARRIER_TIMEOUT)).isTrue()
                assertThat(barrier.toString())
                    .`as`("the barrier must recognise the lines ResilientWebClient really logs")
                    .isEqualTo("1 subscription(s) started, 1 finished")
            } finally {
                clientLogger.detachAppender(barrier)
                barrier.stop()
            }
        }

        @Test
        fun `should hold the barrier until every started subscription has reported a terminal signal`() {
            // What is tested: the barrier that `tearDown` relies on - that it settles when, and only when, each
            //   counted subscription has produced its closing line.
            // Success criteria: two subscriptions with one terminal signal do NOT settle within a short
            //   budget and the barrier says so; the second signal settles it, and the wait then returns at once.
            // Why it matters: this barrier's failure mode is SILENT and flattering. A version that
            //   counted nothing - a renamed log line, a level that keeps the DEBUG lines off, a typo in a prefix -
            //   would report "settled" for every test and the whole class would leak events into its
            //   successors, with nothing failing to say so. Nothing else in this class would notice, because a
            //   barrier that always opens looks exactly like a barrier with nothing to hold back.
            //
            // Given: a barrier attached to a logger, and two started subscriptions with one of them finished
            val logger = LoggerFactory.getLogger("barrier-self-test") as Logger
            val barrier = InFlightCallBarrier().apply { start() }
            logger.level = Level.DEBUG
            logger.addAppender(barrier)
            try {
                repeat(2) { logger.debug("$EXECUTING_PREFIX to backend 'probe'") }
                logger.debug("${SIGNAL_PREFIX}onComplete for backend 'probe'")

                // When / Then: it refuses to settle while one is outstanding, and reports the imbalance
                assertThat(barrier.awaitSettled(Duration.ofMillis(50)))
                    .`as`("one subscription is still in flight")
                    .isFalse()
                assertThat(barrier.toString()).isEqualTo("2 subscription(s) started, 1 finished")

                // When: the outstanding subscription terminates
                logger.debug("${SIGNAL_PREFIX}onError for backend 'probe'")

                // Then: the barrier opens
                assertThat(barrier.awaitSettled(BARRIER_TIMEOUT)).isTrue()
            } finally {
                logger.detachAppender(barrier)
                barrier.stop()
            }
        }

        @Test
        fun `should be released by a terminal signal arriving on another thread`() {
            // What is tested: that the wait is condition-based rather than a poll or a sleep - a signal logged by a
            //   FOREIGN thread wakes the waiter.
            // Success criteria: the waiting thread blocks on an outstanding subscription and returns as soon
            //   as another thread logs the terminal line, well inside the timeout.
            // Why it matters: this is the whole mechanism in production use. Every real terminal signal is written
            //   by a Netty event loop, never by the test thread that waits in tearDown, so a barrier that only ever
            //   settled on same-thread appends would time out on every test that actually had work in flight - and
            //   the class would fail in teardown instead of waiting correctly.
            //
            // Given: one started, unfinished subscription
            val logger = LoggerFactory.getLogger("barrier-cross-thread-self-test") as Logger
            val barrier = InFlightCallBarrier().apply { start() }
            logger.level = Level.DEBUG
            logger.addAppender(barrier)
            val emitter = Executors.newSingleThreadExecutor { r -> Thread(r, "barrier-emitter") }
            try {
                logger.debug("$EXECUTING_PREFIX to backend 'probe'")

                // When: a foreign thread reports the terminal signal while this one waits
                emitter.execute { logger.debug("${SIGNAL_PREFIX}onComplete for backend 'probe'") }

                // Then: the waiter is released by that signal, not by the timeout expiring
                assertThat(barrier.awaitSettled(BARRIER_TIMEOUT))
                    .`as`("a terminal signal from the event loop must release a waiter on the test thread")
                    .isTrue()
            } finally {
                emitter.shutdownNow()
                logger.detachAppender(barrier)
                barrier.stop()
            }
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /**
     * A factory for one backend against WireMock, with its own Resilience4j registries.
     *
     * The TimeLimiter defaults to [GENEROUS_BUDGET], not to Resilience4j's 1 s: every test here makes real
     * loopback calls - the first one of the JVM included, with JIT, pool and Jackson warming up - and a busy CI
     * runner can push such a call past 1 s. It then fails with a TimeoutException nobody asked for, and a log
     * assertion (`single { ... }`) fails on a line it did not expect. Only the tests ABOUT the budget pass a
     * short one. [circuitBreakers], when given, is used instead of a registry built from [circuitBreaker], so a
     * test can read the breaker's counters afterwards.
     */
    private fun newFactory(
        backendName: String,
        timeLimiter: TimeLimiterConfig = TimeLimiterConfig.custom().timeoutDuration(GENEROUS_BUDGET).build(),
        circuitBreaker: CircuitBreakerConfig = CircuitBreakerConfig.ofDefaults(),
        bulkhead: BulkheadConfig = BulkheadConfig.ofDefaults(),
        maxInMemorySize: DataSize = WebClientProperties().maxInMemorySize,
        meterRegistry: MeterRegistry? = null,
        userAgent: String? = null,
        // Deliberately no Spring context here: a bare builder stands in for Boot's bean. A test that needs to
        // observe what the factory puts on its clients passes a builder carrying a filter of its own.
        webClientBuilder: WebClient.Builder = WebClient.builder(),
        circuitBreakers: CircuitBreakerRegistry? = null,
        observationEnabled: Boolean = false,
    ): ResilientWebClientFactory {
        val resilience =
            ReactiveResilienceDecorator(
                circuitBreakers ?: CircuitBreakerRegistry.of(circuitBreaker),
                TimeLimiterRegistry.of(timeLimiter),
                BulkheadRegistry.of(bulkhead),
            )
        val meshProperties =
            MeshProperties(
                defaults = MeshProperties.Defaults(individualRequestTimeoutMs = 1000),
                endpoints =
                    listOf(
                        MeshProperties.EndpointConfig(
                            triplet = "/endpoint/localhost/GET/$backendName",
                            alias = backendName,
                            baseUrl = wireMock.baseUrl(),
                        ),
                    ),
            )
        return ResilientWebClientFactory(
            WebClientProperties(
                maxInMemorySize = maxInMemorySize,
                userAgent = userAgent,
                observability = WebClientProperties.Observability(httpObservationEnabled = observationEnabled),
            ),
            meshProperties,
            resilience,
            objectMapper,
            meterRegistry = meterRegistry,
            webClientBuilder = webClientBuilder,
        ).also { factories += it }
    }

    /**
     * Drives one exchange that the backend answers with [status], and returns the client's own error line as a
     * captured event.
     *
     * The logger is raised to DEBUG for the duration on purpose: the benign-4xx line is emitted at exactly that
     * level, so at the default level the event would never be created and every assertion on it would pass
     * vacuously. Restored afterwards, since the level is global to the logger.
     *
     * Raising the level is also what makes the capture racy, hence [awaitCall]: at DEBUG the
     * closing `On signalType=...` line exists, and it is written AFTER `block()` has already returned.
     */
    private fun errorLineFor(
        status: Int,
        backendName: String,
    ): ILoggingEvent {
        val path = "/status-$status"
        wireMock.stubFor(get(urlEqualTo(path)).willReturn(aResponse().withStatus(status)))
        return capturing(clientLogger, Level.DEBUG, accept = naming("backend '$backendName'")) { appender ->
            val client = newClient(backendName)
            runCatching { client.get(Greeting::class.java) { it.uri(path) }.block() }
            appender.awaitCall().single { it.formattedMessage.startsWith(ERROR_PREFIX) }
        }
    }

    /** The client of [newFactory] for [backendName]; see there for the defaults. */
    private fun newClient(
        backendName: String,
        timeLimiter: TimeLimiterConfig = TimeLimiterConfig.custom().timeoutDuration(GENEROUS_BUDGET).build(),
        circuitBreaker: CircuitBreakerConfig = CircuitBreakerConfig.ofDefaults(),
        bulkhead: BulkheadConfig = BulkheadConfig.ofDefaults(),
        maxInMemorySize: DataSize = WebClientProperties().maxInMemorySize,
        meterRegistry: MeterRegistry? = null,
        userAgent: String? = null,
        circuitBreakers: CircuitBreakerRegistry? = null,
    ): ResilientWebClient =
        newFactory(
            backendName,
            timeLimiter,
            circuitBreaker,
            bulkhead,
            maxInMemorySize,
            meterRegistry,
            userAgent,
            circuitBreakers = circuitBreakers,
        ).create(backendName)

    /**
     * A client whose factory received a builder carrying [registry] - the stand-in for what Boot's
     * `ObservationWebClientCustomizer` puts on the auto-configured builder - gated by [enabled].
     */
    private fun newObservingClient(
        backendName: String,
        registry: ObservationRegistry,
        enabled: Boolean,
    ): ResilientWebClient =
        // No Spring context here: the registry is placed on the builder by hand, as Boot's customizer would.
        newFactory(
            backendName,
            webClientBuilder = WebClient.builder().observationRegistry(registry),
            observationEnabled = enabled,
        ).create(backendName)

    /**
     * The capture filter for the lines naming [marker]. The logger is a shared, process-wide one, so a
     * lingering call from an earlier test (a cancelled call, a late timeout) would otherwise be captured as well
     * and could satisfy the wait before this call's own lines arrive. Every message of interest names its backend
     * (`... backend '<name>' ...`), and backend names are unique per test.
     */
    private fun naming(marker: String): (ILoggingEvent) -> Boolean = { it.formattedMessage.contains(marker) }

    /**
     * Everything captured, once the line starting with [terminalPrefix] - the last one the call can produce - has
     * been logged, so the capture is COMPLETE rather than merely safe to read. The lines under test are written on
     * a Netty event loop, and Reactor's `doFinally` propagates the terminal signal DOWNSTREAM FIRST: the client's
     * closing `On signalType=...` line is written after `block()`/`awaitSingle()` has already resumed the test
     * thread.
     */
    private fun CapturingAppender.awaitCall(terminalPrefix: String = SIGNAL_PREFIX): List<ILoggingEvent> = awaitEvents { it.formattedMessage.startsWith(terminalPrefix) }

    /**
     * Counts subscriptions against their terminal signals, so a test can be held until its own reactive work is
     * genuinely over rather than merely until its last statement ran.
     *
     * It reads the client's own two DEBUG lines rather than instrumenting anything: `Executing request` is
     * logged INSIDE `Mono.defer`, i.e. once per SUBSCRIPTION, and `On signalType=` is logged in the matching
     * `doFinally`, i.e. once per terminal signal - completion, error and cancellation alike. Their difference is
     * the number of calls still in flight. Counting the log is what keeps this out of production code; the two
     * lines already exist for operators, and both are pinned by tests in this class.
     *
     * A call that fails BEFORE the defer - the serialisation failure, which raises in the `fromCallable`
     * upstream of `execute()` - logs neither line and is therefore counted on neither side. That is correct
     * rather than a gap: such a call never subscribed to an exchange, so it has nothing in flight.
     *
     * Waiting is condition-based, never a sleep: [awaitSettled] blocks on a signal raised by the appending
     * thread itself, so a settled test costs nothing and a leaking one is bounded by the caller's timeout.
     */
    private class InFlightCallBarrier : AppenderBase<ILoggingEvent>() {
        private val lock = ReentrantLock()
        private val settled = lock.newCondition()

        // Named startedCount, not started: AppenderBase already declares a Java field called `started` for its
        // own lifecycle (LifeCycle.isStarted()), and this counter would otherwise silently shadow it.
        private var startedCount = 0
        private var finished = 0

        override fun append(eventObject: ILoggingEvent) {
            val message = eventObject.formattedMessage
            lock.withLock {
                when {
                    message.startsWith(EXECUTING_PREFIX) -> {
                        startedCount++
                    }

                    message.startsWith(SIGNAL_PREFIX) -> {
                        finished++
                        if (finished >= startedCount) settled.signalAll()
                    }
                }
            }
        }

        /** Waits until every started subscription has reported a terminal signal. False iff [timeout] ran out. */
        fun awaitSettled(timeout: Duration): Boolean =
            lock.withLock {
                var remaining = timeout.toNanos()
                while (finished < startedCount && remaining > 0) {
                    remaining = settled.awaitNanos(remaining)
                }
                finished >= startedCount
            }

        override fun toString(): String = lock.withLock { "$startedCount subscription(s) started, $finished finished" }
    }

    private class RecordingObservationHandler : ObservationHandler<Observation.Context> {
        val started = CopyOnWriteArrayList<Observation.Context>()

        override fun supportsContext(context: Observation.Context): Boolean = true

        override fun onStart(context: Observation.Context) {
            started.add(context)
        }
    }
}
