package eu.inqudium.veredium.webclient

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.equalToJson
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.okJson
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import eu.inqudium.legatium.webclient.logging.ClientRequestLoggingFilter
import eu.inqudium.veredium.core.mesh.MeshProperties
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
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.core.io.buffer.DataBufferLimitException
import org.springframework.http.MediaType
import org.springframework.util.unit.DataSize
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientRequestException
import org.springframework.web.reactive.function.client.WebClientResponseException
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.json.JsonMapper
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
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
 */
class ResilientWebClientIntegrationTest {
    companion object {
        /** Message prefixes the log assertions key on, including [CapturingAppender]'s terminal markers. */
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

        private lateinit var wireMock: WireMockServer

        @JvmStatic
        @BeforeAll
        fun startWireMock() {
            // Dynamic port keeps this test independent of the :8080 performance harness and of any other
            // WireMock instance, so it can run in the default build alongside everything else.
            wireMock = WireMockServer(wireMockConfig().dynamicPort()).apply { start() }
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

    /** The client logger's configured level, restored in [tearDown] after the barrier raised it to DEBUG. */
    private var previousClientLevel: Level? = null

    @BeforeEach
    fun resetStubs() {
        wireMock.resetAll()
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
     * test's capture. That is not hypothetical: it failed in CI as an exchange event of the deserialisation
     * test appearing in a later test that asserts on its own backend's events, and it is the same
     * in-flight-after-return effect that made an earlier version of this class throw
     * ConcurrentModificationException while iterating an appender.
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
            previousClientLevel.let { clientLogger.level = it }
            factories.forEach { it.shutdown() }
            factories.clear()
        }
    }

    @Test
    fun `should deserialise a successful GET response into the requested type`() {
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
    fun `should open an http client observation per request when observation is enabled`() {
        // What is tested: the standalone observation toggle - with it ON, the factory keeps the (recording)
        //   ObservationRegistry that arrived on the injected builder, so each outbound call opens Spring
        //   WebClient's native `http.client.requests` observation. Proves the pass-through without a tracing
        //   bridge.
        // Success criterion: exactly one `http.client.requests` observation is recorded for one GET.
        // Why it matters: the factory builds its OWN builder (bypassing Spring's auto-configured one), so
        //   without this explicit wiring the outbound HTTP calls would carry no client span at all.
        val handler = RecordingObservationHandler()
        val registry = ObservationRegistry.create().apply { observationConfig().observationHandler(handler) }
        wireMock.stubFor(get(urlEqualTo("/hello")).willReturn(okJson("""{"message":"hi"}""")))

        // Given a factory-built client with observation ENABLED / When a request is made
        val client = newObservingClient("greet-obs", registry, enabled = true)
        runBlocking { client.get(Greeting::class.java) { it.uri("/hello") }.awaitSingle() }

        // Then one http.client.requests observation was opened against our registry
        assertThat(handler.started.map { it.name }).containsExactly("http.client.requests")
    }

    @Test
    fun `should NOT open an http client observation when observation is disabled`() {
        // Given a factory-built client with observation OFF (registry on the builder, replaced by NOOP) / When a request runs
        val handler = RecordingObservationHandler()
        val registry = ObservationRegistry.create().apply { observationConfig().observationHandler(handler) }
        wireMock.stubFor(get(urlEqualTo("/hello")).willReturn(okJson("""{"message":"hi"}""")))

        val client = newObservingClient("greet-noobs", registry, enabled = false)
        runBlocking { client.get(Greeting::class.java) { it.uri("/hello") }.awaitSingle() }

        // Then no observation was recorded - the factory replaced the registry with NOOP, so the client emits no span
        assertThat(handler.started).isEmpty()
    }

    @Test
    fun `should nest the http client observation under an active parent observation across the reactive call`() {
        // 1. What is tested: with observation ON and io.micrometer:context-propagation on the classpath, an
        //    ambient parent observation (the stand-in for an inbound `http.server.requests` server span) is
        //    carried across the REAL reactive WebClient call - so the outbound `http.client.requests`
        //    observation nests as its CHILD instead of starting a fresh root.
        // 2. Success criterion: the recorded `http.client.requests` context's parent is the parent observation
        //    (`inbound.server`) - the outbound half of the trace reconciliation
        //    (docs/server-trace-propagation-reconciliation.md): server span -> client span, one trace.
        // 3. Why it matters: WebFlux hops Reactor threads, so without context propagation the client span would
        //    be a detached root and the outbound call would not join the caller's trace. This is the regression
        //    guard for the Reactor hook + `contextCapture()` in ResilientWebClient, proven over a real HTTP call.
        val handler = RecordingObservationHandler()
        val registry = ObservationRegistry.create().apply { observationConfig().observationHandler(handler) }
        wireMock.stubFor(get(urlEqualTo("/hello")).willReturn(okJson("""{"message":"hi"}""")))
        val client = newObservingClient("greet-nest", registry, enabled = true)

        // Given an ACTIVE parent observation / When the request runs inside its open scope
        val parent = Observation.start("inbound.server", registry)
        parent.openScope().use {
            runBlocking { client.get(Greeting::class.java) { it.uri("/hello") }.awaitSingle() }
        }
        parent.stop()

        // Then the client observation is a CHILD of the parent - the ambient observation propagated across the
        // reactive call rather than the client span starting its own root trace
        val clientContext = handler.started.single { it.name == "http.client.requests" }
        assertThat(clientContext.parentObservation).isNotNull()
        assertThat(clientContext.parentObservation!!.contextView.name).isEqualTo("inbound.server")
    }

    @Test
    fun `should serialise the POST body once, send it on the wire, and deserialise the response`() {
        // What is tested: that post() serialises the body itself, the JSON reaches the backend, and the
        // response is deserialised. Why it matters: the production code serialises exactly once (the JSON
        // doubles as the diary payload) - verifying the received body proves the wire request is correct
        // and was not, e.g., double-encoded.
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
        // How success is judged: calling post(...) throws nothing - the assertion that would have failed before,
        //   when serialisation ran eagerly - while subscribing surfaces the Jackson failure.
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
    fun `should nest the http client observation under an active parent observation for a POST`() {
        // What is tested: that the trace context still crosses into the exchange for POST, now that the request
        //   is built and executed behind a flatMap rather than at assembly time.
        // How success is judged: the recorded http.client.requests observation has the ambient parent, exactly
        //   as the GET case above asserts.
        // Why it matters: this is the regression guard for the serialisation change. execute() applies
        //   contextCapture() to its own Mono, and that Mono is now subscribed by an outer flatMap instead of
        //   directly by the caller. Had that shifted the capture off the subscribing thread, POST calls would
        //   have quietly started a detached root trace - invisible except in a trace viewer, and only for POST.
        val handler = RecordingObservationHandler()
        val registry = ObservationRegistry.create().apply { observationConfig().observationHandler(handler) }
        wireMock.stubFor(post(urlEqualTo("/lookup")).willReturn(okJson("""{"message":"hi"}""")))
        val client = newObservingClient("post-nest", registry, enabled = true)

        // Given an ACTIVE parent observation / When the POST runs inside its open scope
        val parent = Observation.start("inbound.server", registry)
        parent.openScope().use {
            runBlocking {
                client
                    .post(Greeting::class.java, LookupRequest("X-1")) {
                        it.uri("/lookup").contentType(MediaType.APPLICATION_JSON)
                    }.awaitSingle()
            }
        }
        parent.stop()

        // Then the client observation is a CHILD of the parent
        val clientContext = handler.started.single { it.name == "http.client.requests" }
        assertThat(clientContext.parentObservation).isNotNull()
        assertThat(clientContext.parentObservation!!.contextView.name).isEqualTo("inbound.server")
    }

    @Test
    fun `should record the decoded response body size so the codec limit can be sized from evidence`() {
        // What is tested: that every response that FITS the codec buffer contributes its decoded size to
        //   `webclient.response.body.size`, tagged with the adapter target - the same value the log lines carry as `adapter_name`.
        // How success is judged: one call returning a body containing MULTI-BYTE characters produces exactly
        //   one sample whose maximum is the UTF-8 BYTE size - strictly greater than the UTF-16 length, which
        //   is what makes the assertion about the unit rather than about the number. The meter carries the
        //   `adapter` tag and the `bytes` base unit.
        // Why this case is important: `maxInMemorySize` is a ceiling somebody has to choose, and before this
        //   meter there was nothing to choose it from. The diary cannot serve - its capture bound is DERIVED
        //   from that very limit, so it can never show that more was needed - and a DataBufferLimitException
        //   reports only that the limit was passed, not by how much. On 2026-08-07 that gap cost two backfill
        //   rows, rejected against an 8 KB default nobody had measured against a real response.
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
                .tag(WebClientMetrics.ADAPTER_TAG, "sized")
                .summary()
        assertThat(summary).isNotNull()
        assertThat(summary!!.count()).isEqualTo(1)
        assertThat(summary.max()).isEqualTo(body.toByteArray(Charsets.UTF_8).size.toDouble())
        assertThat(summary.max()).isGreaterThan(body.length.toDouble())
        assertThat(summary.id.baseUnit).isEqualTo(WebClientMetrics.RESPONSE_BODY_BASE_UNIT)
    }

    @Test
    fun `should leave the response-size meter unregistered when the host wired no registry`() {
        // What is tested: that the meter is optional in the same sense the validator and the observation
        //   registry are - absent host wiring means absent meter, not a failure and not a global-registry
        //   fallback.
        // How success is judged: a client built without a MeterRegistry serves a call normally, and a fresh
        //   registry stays empty afterwards.
        // Why this case is important: the factory is constructed directly in several tests and in minimal
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

    @Test
    fun `should not count a response exceeding the buffer limit against the circuit breaker`() {
        // What is tested: that a response too large for maxInMemorySize does not open the circuit - through the
        //   REAL pipeline, so the exception is the one the client actually produces rather than one written by
        //   hand in a test.
        // How success is judged: five oversized responses in a row all fail for the caller, while the breaker
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
        CustomizedRegistries.circuitBreakers(circuitBreakerRegistry, meshProperties)

        // When: five calls all trip the buffer limit
        repeat(5) {
            val failure = catchThrowable { client.get(Greeting::class.java) { it.uri("/big") }.block() }
            // Assert the REASON, not merely that it failed: if the call ever fails for another reason - a
            // timeout, say - that is a different scenario and the counter assertion below would no longer mean
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
    fun `should surface a non-2xx response as a WebClientResponseException carrying the status`() {
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
    fun `should fail a call that exceeds the time-limiter budget on a slow backend`() {
        // What is tested: that the resilience timeout wiring actually cancels a stalled call. Why the
        // assertion is broad: the Netty responseTimeout (a fraction of the budget) is a tighter guard than
        // the TimeLimiter and may fire first, so either a TimeoutException or a transport
        // WebClientRequestException is a correct outcome - what matters is that the call does NOT hang or
        // return a value once the budget is blown.
        //
        // Given: a 3s backend delay against a 300ms budget.
        wireMock.stubFor(
            get(urlEqualTo("/slow")).willReturn(okJson("""{"message":"late"}""").withFixedDelay(3_000)),
        )
        val client =
            newClient(
                "slow",
                timeLimiter =
                    TimeLimiterConfig
                        .custom()
                        .timeoutDuration(Duration.ofMillis(300))
                        .cancelRunningFuture(true)
                        .build(),
            )

        // When / Then
        assertThatThrownBy {
            runBlocking { client.get(Greeting::class.java) { it.uri("/slow") }.awaitSingle() }
        }.isInstanceOfAny(TimeoutException::class.java, WebClientRequestException::class.java)
    }

    @Test
    fun `should open the circuit and short-circuit further calls after the failure threshold is reached`() {
        // What is tested: that the circuit breaker observes the decorated calls and opens once the failure
        // rate is exceeded, then rejects the next call locally. Why it matters: this proves the breaker is
        // actually in the call path and records HTTP failures, which is the whole point of the decoration.
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
    fun `should expose the time-limiter timeout as the request budget`() {
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

    @Test
    fun `should return the same cached client instance for repeated create calls`() {
        // Given
        val factory = newFactory("product-agreements")

        // When
        val first = factory.create("product-agreements")
        val second = factory.create("product-agreements")

        // Then
        assertThat(first).isSameAs(second)
    }

    @Test
    fun `should stamp the backend name onto every request as the adapter-name attribute`() {
        // What is tested: that a client of the factory carries its backend name in the request attribute
        //   ClientRequestLoggingFilter.ADAPTER_NAME_ATTRIBUTE - the key legatium's client logging reads the
        //   adapter name from - on a GET and on a POST alike, without the adapter doing anything for it.
        // How success is judged: a filter placed on the builder BEFORE the factory clones it (where a host
        //   customizer would sit) sees the attribute with the backend name on each request it observes.
        //   The filter is the only vantage point: an attribute never reaches the wire, so WireMock cannot
        //   see it, and that invisibility is precisely why it is an attribute and not a header.
        // Why this case is important: behind the sidecar every request goes to the same host, so the URI no
        //   longer tells backends apart. The attribute is what keeps the backend name on the exchange for an
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

    @Test
    fun `should fail fast when creating a client for an unconfigured target`() {
        // Given: a factory that only knows about "configured".
        val factory = newFactory("configured")

        // When / Then
        assertThatThrownBy { factory.create("unknown-target") }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("unknown-target")
    }

    @Test
    fun `should keep a cancelled call at DEBUG, leaving the operator signal to the exchange event`() {
        // What is tested: where a cancellation is reported on THIS side. The per-call line must be DEBUG - not
        //   merely absent from WARN, because an accidental drop to TRACE or a removal would pass that weaker
        //   assertion.
        // How success is judged: the single "Cancelled request" line from ResilientWebClient is at DEBUG.
        // Why this case is important: it pins a TRADE, not a removal. A cancellation is the subscriber going
        //   away (its budget expired, its client disconnected) - normal, correlated with load, and therefore
        //   arriving in floods on WARN, the level operators alert on, exactly when the lines that matter are
        //   needed. Demoting it is only safe because legatium's exchange event carries the same fact at WARN
        //   with `adapter_outcome=cancelled` - the other half of the trade, pinned in
        //   LegatiumExchangeLoggingIntegrationTest against the real filter on Boot's builder.
        //
        // Given: a backend that does not answer within the test's window, and the client logger capturing
        wireMock.stubFor(get(urlEqualTo("/never")).willReturn(okJson("{}").withFixedDelay(30_000)))
        val clientLogger = LoggerFactory.getLogger(ResilientWebClient::class.java) as Logger
        // Captures an asynchronously written line: dispose() returns immediately, while doOnCancel runs on the
        // event loop afterwards - so the line is awaited, not read.
        val clientEvents = CapturingAppender("backend 'cancelled'", SIGNAL_PREFIX).apply { start() }
        val previousClientLevel = clientLogger.level
        clientLogger.level = Level.DEBUG
        clientLogger.addAppender(clientEvents)
        try {
            val client = newClient("cancelled")

            // When: the subscriber walks away before the answer arrives
            client.get(Greeting::class.java) { it.uri("/never") }.subscribe().dispose()

            // Then: the per-call line is DEBUG
            val cancelLine = clientEvents.awaitEvents().single { it.formattedMessage.startsWith("Cancelled request") }
            assertThat(cancelLine.level)
                .`as`("a cancellation is the caller's business, not an operational fault")
                .isEqualTo(Level.DEBUG)
        } finally {
            clientLogger.detachAppender(clientEvents)
            clientLogger.level = previousClientLevel
        }
    }

    @Test
    fun `should log a benign 4xx at DEBUG and without a stack trace`() {
        // What is tested: the third consumer of HttpExchangeErrorPolicy - the severity of the client's OWN
        //   error line. A benign 4xx (the documented "does not exist" answer) must be DEBUG, and it must carry
        //   NO stack trace.
        // How success is judged: the captured event is at DEBUG, its throwable proxy is absent, and the message
        //   carries the `expected client error` marker that names the status for a reader.
        // Why it matters: this is the line that used to make every routine 404 look like an outage. The stack
        //   trace is the second half of that: the diary event already carries one for the same exchange, so
        //   attaching one here produced two per business case - for an exception whose reactive stack explains
        //   nothing that the status and URL do not. The three consumers derive from ONE predicate precisely so
        //   this line and the diary cannot disagree; the diary end is pinned in
        //   WebClientVerdictsTest.

        // When: the backend answers "not found"
        val line = errorLineFor(status = 404, backendName = "benign-4xx")

        // Then
        assertThat(line.level).isEqualTo(Level.DEBUG)
        assertThat(line.throwableProxy)
            .`as`("a business-case rejection needs no stack trace - the diary event already carries one")
            .isNull()
        assertThat(line.formattedMessage).contains("expected client error")
    }

    @Test
    fun `should log a 5xx at WARN and with the stack trace`() {
        // The counterpart that keeps the test above honest: without it, a line that lost its severity AND its
        // cause for every failure would satisfy the DEBUG/no-stack-trace assertions.

        // When
        val line = errorLineFor(status = 500, backendName = "server-error")

        // Then
        assertThat(line.level).isEqualTo(Level.WARN)
        assertThat(line.throwableProxy?.className).isEqualTo(WebClientResponseException.InternalServerError::class.java.name)
        assertThat(line.formattedMessage).doesNotContain("expected client error")
    }

    @Test
    fun `should carry the backend name and the URI template on a transport line under the library's field names`() {
        // What is tested: the two structured fields every transport line of this client carries, and that
        //   their NAMES are the exchange event's - `adapter_name` and `adapter_url_template` as legatium spells
        //   them - so one dashboard field groups both loggers.
        // How success is judged: the error line for a 500 carries both key-values with the backend alias and
        //   the endpoint's template; and the former name `adapter_target_name` is absent.
        // Why it matters: these names leave the process and are a contract with the log index (the runbook's
        //   section 7 groups by them). They used to be pinned by an enum and a template of this module's own;
        //   since the module writes only these two, taken from the library's ClientLogField, this line is the
        //   whole pin.
        //
        // When: the backend answers 500
        val line = errorLineFor(status = 500, backendName = "joined")

        // Then
        val fields = line.keyValuePairs.associate { it.key to it.value }
        assertThat(fields).containsEntry("adapter_name", "joined")
        assertThat(fields).containsEntry("adapter_url_template", "/joined")
        assertThat(fields).doesNotContainKey("adapter_target_name")
    }

    @Test
    fun `should keep a 408 at WARN although it is a 4xx`() {
        // What is tested: that the boundary INSIDE the 4xx range holds on this side too - 408 Request Timeout
        //   is an operational signal, not a business case.
        // How success is judged: the line is WARN with a stack trace, i.e. treated exactly like a 5xx, while the
        //   404 above proves the same code path does demote a benign status.
        // Why it matters: a demoted 408 would hide a backend timing out under load in DEBUG, the one level
        //   nobody runs in production - and it would do so silently, because the exchange still "fails
        //   correctly" from the caller's point of view. The same status set drives the circuit breaker's ignore
        //   predicate, so a drift here would mean the logs and the breaker disagree about what an outage is.

        // When
        val line = errorLineFor(status = 408, backendName = "request-timeout")

        // Then
        assertThat(line.level).isEqualTo(Level.WARN)
        assertThat(line.throwableProxy).isNotNull()
        assertThat(line.formattedMessage).doesNotContain("expected client error")
    }

    @Test
    fun `should report the elapsed time in the failure log from the call's own clock`() {
        // What is tested: that the duration in the error line is measured by the call itself - it used to be
        //   read off the (since removed) exchange diary, which did not exist when that logger sat above INFO.
        // How success is judged: the WARN line renders `duration so far: <n>ms` with digits. The assertion is
        //   on the SHAPE, not on a value: what was wrong here was a literal "null" in a field that announces a
        //   number, and no timing knowledge is needed to catch that.
        // Why this case is important: the WARN line is this module's only trace of the failure, and "duration
        //   so far: nullms" withheld the one thing it could still answer - whether the call failed early or ran
        //   into its budget. It also breaks any consumer that parses the field as a number.
        //
        // Given: a failing backend and the client logger capturing
        wireMock.stubFor(get(urlEqualTo("/boom")).willReturn(aResponse().withStatus(500)))
        val clientLogger = LoggerFactory.getLogger(ResilientWebClient::class.java) as Logger
        // The client logger keeps its configured level here, so the DEBUG `On signalType=` line is never
        // emitted and cannot serve as the terminal marker - the WARN failure line itself is the last one.
        val appender = CapturingAppender("backend 'own-clock-duration'", ERROR_PREFIX).apply { start() }
        clientLogger.addAppender(appender)
        try {
            val client = newClient("own-clock-duration")

            // When: the call fails
            runCatching { client.get(Greeting::class.java) { it.uri("/boom") }.block() }

            // Then: the failure line carries a measured duration, not a null
            val message = appender.awaitEvents().single { it.formattedMessage.startsWith(ERROR_PREFIX) }.formattedMessage
            assertThat(message).containsPattern("duration so far: \\d+ms")
        } finally {
            clientLogger.detachAppender(appender)
        }
    }

    @Test
    fun `should drain the pool on shutdown without raising out of the destroy callback`() {
        // What is tested: that shutdown() completes normally when the pool holds a live keep-alive connection,
        //   and that calling it twice is harmless.
        // How success is judged: neither call throws, and the exchange that populated the pool did reach the
        //   backend - so there genuinely was a connection to dispose rather than an empty pool.
        // Why this case is important: shutdown() runs as a @PreDestroy, where Spring can only catch what it
        //   throws and log it as a destroy method that failed. Its previous form reported the drain deadline as
        //   an IllegalStateException out of block(Duration) - indistinguishable from a real disposal error, and
        //   the last thing in the log before the process exits. The deadline is now an outcome, reported as a
        //   WARN naming the pool. That branch is NOT covered here: forcing it would mean holding a connection
        //   open past the derived drain window, which is longer than any response timeout by construction.
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
    fun `should count a real call, so the barrier's prefixes cannot drift from the lines the client logs`() {
        // What is tested: that [InFlightCallBarrier] observes the lines a REAL exchange produces - not lines
        //   this test wrote for it.
        // How success is judged: after one genuine call, a barrier attached to the client's own logger reports
        //   exactly one started and one finished subscription.
        // Why this case is important: it is the only thing tying [EXECUTING_PREFIX] to production. The sibling
        //   self-tests below drive the barrier with log lines they build from that same constant, so they stay
        //   green if the client renames its line - the constant moves on both sides at once and they never
        //   notice. The barrier would then count 0 started against N finished, `finished >= started` would hold
        //   from the outset, and tearDown would wave every test through while the class went back to leaking
        //   events into its successors - silently, with nothing red. (SIGNAL_PREFIX is additionally pinned by
        //   `errorLineFor` and the cancellation test, which await it after real calls; this covers the other
        //   half.) Verified by mutation: renaming either constant fails this test and only this test.

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
        // What is tested: the barrier that [tearDown] relies on - that it settles when, and only when, each
        //   counted subscription has produced its closing line.
        // How success is judged: two subscriptions with one terminal signal do NOT settle within a short
        //   budget and the barrier says so; the second signal settles it, and the wait then returns at once.
        // Why this case is important: this barrier's failure mode is SILENT and flattering. A version that
        //   counted nothing - a renamed log line, a level that keeps the DEBUG lines off, a typo in a prefix -
        //   would report "settled" for every test and the whole class would go back to leaking events into its
        //   successors, with nothing failing to say so. Nothing else in this class would notice, because a
        //   barrier that always opens looks exactly like a barrier with nothing to hold back.

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
        // How success is judged: the waiting thread blocks on an outstanding subscription and returns as soon
        //   as another thread logs the terminal line, well inside the timeout.
        // Why it matters: this is the whole mechanism in production use. Every real terminal signal is written
        //   by a Netty event loop, never by the test thread that waits in tearDown, so a barrier that only ever
        //   settled on same-thread appends would time out on every test that actually had work in flight - and
        //   the class would fail in teardown instead of waiting correctly.

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

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private fun newFactory(
        backendName: String,
        timeLimiter: TimeLimiterConfig = TimeLimiterConfig.ofDefaults(),
        circuitBreaker: CircuitBreakerConfig = CircuitBreakerConfig.ofDefaults(),
        bulkhead: BulkheadConfig = BulkheadConfig.ofDefaults(),
        maxInMemorySize: DataSize = WebClientProperties().maxInMemorySize,
        meterRegistry: MeterRegistry? = null,
        // Deliberately no Spring context here: a bare builder stands in for Boot's bean. A test that needs to
        // observe what the factory puts on its clients passes a builder carrying a filter of its own.
        webClientBuilder: WebClient.Builder = WebClient.builder(),
    ): ResilientWebClientFactory {
        val resilience =
            ReactiveResilienceDecorator(
                CircuitBreakerRegistry.of(circuitBreaker),
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
            WebClientProperties(maxInMemorySize = maxInMemorySize),
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
     * vacuously. Restored in the `finally`, since the level is global to the logger.
     *
     * Raising the level is also what makes the capture racy, hence [CapturingAppender.awaitEvents]: at DEBUG the
     * closing `On signalType=...` line exists, and it is written AFTER `block()` has already returned.
     */
    private fun errorLineFor(
        status: Int,
        backendName: String,
    ): ILoggingEvent {
        val path = "/status-$status"
        wireMock.stubFor(get(urlEqualTo(path)).willReturn(aResponse().withStatus(status)))
        val clientLogger = LoggerFactory.getLogger(ResilientWebClient::class.java) as Logger
        val appender = CapturingAppender("backend '$backendName'", SIGNAL_PREFIX).apply { start() }
        val previousLevel = clientLogger.level
        clientLogger.level = Level.DEBUG
        clientLogger.addAppender(appender)
        try {
            val client = newClient(backendName)
            runCatching { client.get(Greeting::class.java) { it.uri(path) }.block() }
            return appender.awaitEvents().single { it.formattedMessage.startsWith(ERROR_PREFIX) }
        } finally {
            clientLogger.detachAppender(appender)
            clientLogger.level = previousLevel
        }
    }

    private fun newClient(
        backendName: String,
        timeLimiter: TimeLimiterConfig = TimeLimiterConfig.ofDefaults(),
        circuitBreaker: CircuitBreakerConfig = CircuitBreakerConfig.ofDefaults(),
        bulkhead: BulkheadConfig = BulkheadConfig.ofDefaults(),
        maxInMemorySize: DataSize = WebClientProperties().maxInMemorySize,
        meterRegistry: MeterRegistry? = null,
    ): ResilientWebClient = newFactory(backendName, timeLimiter, circuitBreaker, bulkhead, maxInMemorySize, meterRegistry).create(backendName)

    /**
     * A client whose factory received a builder carrying [registry] - the stand-in for what Boot's
     * `ObservationWebClientCustomizer` puts on the auto-configured builder - gated by [enabled].
     */
    private fun newObservingClient(
        backendName: String,
        registry: ObservationRegistry,
        enabled: Boolean,
    ): ResilientWebClient {
        val resilience =
            ReactiveResilienceDecorator(
                CircuitBreakerRegistry.of(CircuitBreakerConfig.ofDefaults()),
                // Deliberately generous: the DEFAULT 1s budget makes this test timing-dependent, because a
                // slow call under load would fail with a TimeoutException instead.
                TimeLimiterRegistry.of(TimeLimiterConfig.custom().timeoutDuration(Duration.ofSeconds(30)).build()),
                BulkheadRegistry.of(BulkheadConfig.ofDefaults()),
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
        val properties =
            WebClientProperties(observability = WebClientProperties.Observability(httpObservationEnabled = enabled))
        return ResilientWebClientFactory(
            properties,
            meshProperties,
            resilience,
            objectMapper,
            // No Spring context here: the registry is placed on the builder by hand, as Boot's customizer would.
            webClientBuilder = WebClient.builder().observationRegistry(registry),
        ).also { factories += it }
            .create(backendName)
    }

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

    /**
     * Captures log events for reading from a thread OTHER than the one that logged them, which is every
     * assertion in this class: the lines under test are written on a Netty event loop, the assertions run on
     * the test thread.
     *
     * Logback's own `ListAppender` cannot be used for that. Its `list` is a plain `ArrayList`, and the reactive
     * chain keeps logging after the caller has been released: Reactor's `doFinally` propagates the terminal
     * signal DOWNSTREAM FIRST and runs its callback afterwards, so `ResilientWebClient`'s closing
     * `On signalType=...` line is written by the event loop while `block()`/`awaitSingle()` has already resumed
     * the test thread. Iterating the list right then raced the append and threw ConcurrentModificationException
     * (seen on 408 in CI, but every DEBUG-level capture in this class had it).
     *
     * Two properties fix that, and both are needed:
     * - a [CopyOnWriteArrayList], so iterating can never see a concurrent append;
     * - [awaitEvents], which waits for the line matching [terminalPrefix] - the last one the call can produce -
     *   so the capture is not merely safe to read but COMPLETE. Reading without it does not throw any more, it
     *   just silently misses whatever had not landed yet.
     *
     * [marker] scopes the capture to one call: the logger is a shared, process-wide one, so a lingering call
     * from an earlier test (a cancelled call, a late timeout) would otherwise be captured as well and could
     * release the latch before this call's own lines arrive. Every message of interest names its backend
     * (`... backend '<name>' ...`), and backend names are unique per test.
     */
    private class CapturingAppender(
        private val marker: String,
        private val terminalPrefix: String,
    ) : AppenderBase<ILoggingEvent>() {
        private val captured = CopyOnWriteArrayList<ILoggingEvent>()
        private val terminated = CountDownLatch(1)

        override fun append(eventObject: ILoggingEvent) {
            val message = eventObject.formattedMessage
            if (!message.contains(marker)) return
            // Snapshot the MDC (and other thread-bound data) NOW, while the emitting thread still carries
            // it. Logback captures the MDC property map LAZILY, on first read - and a scoped MDC (the pattern
            // legatium's emission scope uses, and the former diary used) holds its keys only for the duration
            // of the log statement. Without this, the first read of mdcPropertyMap happens later on the test
            // thread (empty MDC) and the entries are lost. A production async appender does exactly this
            // before enqueueing; this synchronous capture appender must do the same to observe what production
            // would log.
            eventObject.prepareForDeferredProcessing()
            captured += eventObject
            if (message.startsWith(terminalPrefix)) terminated.countDown()
        }

        /** Everything captured for this call, once its terminal line has actually been logged. */
        fun awaitEvents(): List<ILoggingEvent> {
            assertThat(terminated.await(5, TimeUnit.SECONDS))
                .`as`("no '$terminalPrefix' line for '$marker' was logged within 5s")
                .isTrue()
            return captured.toList()
        }

        /**
         * What has been captured so far - for asserting that something was NOT logged, where there is no
         * terminal line to wait for. The caller is responsible for having driven the call to completion first.
         */
        fun events(): List<ILoggingEvent> = captured.toList()
    }

    private class RecordingObservationHandler : ObservationHandler<Observation.Context> {
        val started = CopyOnWriteArrayList<Observation.Context>()

        override fun supportsContext(context: Observation.Context): Boolean = true

        override fun onStart(context: Observation.Context) {
            started.add(context)
        }
    }
}

/**
 * Response payload for the tests. A plain class with a no-arg constructor and a mutable property so the
 * Kotlin-module-less Jackson 3 mapper can deserialise it via the bean setter.
 */
class Greeting {
    var message: String? = null
}

/** A body Jackson cannot serialise: the accessor it must call to read the value throws. */
class Unserialisable {
    val value: String get() = throw IllegalStateException("this body cannot be serialised")
}

/** Request payload for the POST test; serialises to `{"id":...}` via its bean getter. */
class LookupRequest(
    var id: String? = null,
)
