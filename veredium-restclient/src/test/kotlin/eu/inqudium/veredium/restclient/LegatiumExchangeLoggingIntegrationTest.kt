package eu.inqudium.veredium.restclient

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.matching
import com.github.tomakehurst.wiremock.client.WireMock.okJson
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import eu.inqudium.legatium.common.BodyLogMode
import eu.inqudium.legatium.common.ClientLoggingProperties
import eu.inqudium.legatium.restclient.logging.ClientLoggingAutoConfiguration
import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.resilience.BlockingResilienceDecorator
import eu.inqudium.veredium.restclient.LegatiumExchangeLoggingIntegrationTest.Companion.REFERENCE_PREFIXES
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micrometer.observation.ObservationRegistry
import io.micrometer.tracing.Tracer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.env.YamlPropertySourceLoader
import org.springframework.boot.restclient.autoconfigure.RestClientAutoConfiguration
import org.springframework.boot.restclient.autoconfigure.RestClientObservationAutoConfiguration
import org.springframework.boot.test.context.assertj.AssertableApplicationContext
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.core.env.EnumerablePropertySource
import org.springframework.core.env.MapPropertySource
import org.springframework.core.io.ClassPathResource
import org.springframework.http.MediaType
import org.springframework.util.unit.DataSize
import org.springframework.web.client.ResourceAccessException
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.json.JsonMapper
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Supplier

/**
 * The exchange log of this module: one structured event per outbound HTTP exchange on the
 * `adapter-http-exchange` logger, written by legatium-restclient-logging's interceptor, which reaches the
 * factory's clients through Boot's `RestClient.Builder` bean and nothing else.
 *
 * 1. What is tested: the wiring end to end - a real context with Boot's RestClient auto-configuration, the
 *    library's auto-configuration and the values of the module's reference `application.yml`, a client of
 *    [ResilientRestClientFactory], a local WireMock - and the SEMANTICS the library gives the blocking stack: a
 *    4xx is `rejected` with its status, a 5xx a WARN `failure`, the transport timeout a WARN `timeout`, and a
 *    CANCELLED call - the caller interrupted, or the TimeLimiter expired - an ERROR `failure` written promptly by
 *    the worker whose socket the cancellation closed; bodies capped; the backend name as `adapter_name`; the Host
 *    header in plaintext next to the token's fingerprint; and whether a value the CALLER put in its MDC reaches
 *    an event written on the worker.
 * 2. Success criteria: per case, the captured event on `adapter-http-exchange` carries the expected level,
 *    outcome, status and fields.
 * 3. Why it matters: every runbook query and alert keys on this event's outcome vocabulary, and the blocking
 *    stack's vocabulary differs from the reactive twin's at exactly the point pinned here (no `cancelled`: a
 *    cancelled exchange is a `failure` with `Closed by interrupt` as its cause), so the difference is recorded as
 *    a test rather than rediscovered as a "bug" in production.
 *
 * Contexts run through an [ApplicationContextRunner] with the `adapter-logging` AND `app.restclient` blocks of
 * the reference `application.yml` ([ConfigurationFiles.REFERENCE], a test resource no consumer receives) as a
 * property source, so the configuration under test is the one the docs recommend - not a copy in this file. `RestClientProperties` is bound by its own [RestClientConfiguration] instead of being
 * handed in as a code-default instance, so a key like `app.restclient.observability.http-observation-enabled`
 * reaches [ResilientRestClientFactory] here by exactly the route it takes in the running application.
 *
 * `app.mesh.*` is deliberately left out of that property source: the endpoints are hand-built per case (WireMock's
 * dynamic port, a per-case TimeLimiter budget), and the reference triplets would point these tests at a real mesh.
 * No mocks: WireMock, a real context, a Logback appender.
 */
class LegatiumExchangeLoggingIntegrationTest {
    companion object {
        /**
         * legatium's exchange logger as the reference application.yml names it - the library default, which is
         * also the name its `logging.level` line addresses (the pair is pinned by LoggingLevelContractTest).
         */
        private const val EXCHANGE_LOGGER = "adapter-http-exchange"

        /**
         * The prefixes of the reference `application.yml` this test binds. `adapter-logging.` configures the
         * library under test; `app.restclient.` configures [RestClientProperties], whose
         * `observability.http-observation-enabled` is the one key whose reference value (`true`) DIFFERS from the
         * class's code default (`false`) - so loading it is what makes the two distinguishable here.
         *
         * `app.mesh.` is absent on purpose - see the class KDoc.
         */
        private val REFERENCE_PREFIXES = listOf("adapter-logging.", "app.restclient.")

        /** Name of the property source the reference keys are added under - for readable failure diagnostics. */
        private const val REFERENCE_PROPERTY_SOURCE = "reference-application-yml"

        /** Spring's RestClient client observation - the meter the wired observation must produce. */
        private const val HTTP_CLIENT_OBSERVATION = "http.client.requests"

        private const val TOKEN_HEADER = "X-Access-Token"
        private const val TOKEN_VALUE = "a-bearer-secret-that-must-never-be-logged"

        /** A caller-side MDC key/value, shaped like sync-bridge's outbound correlation id. */
        private const val CORRELATION_KEY = "CORRELATION_ID"
        private const val CORRELATION_VALUE = "Out:7cc0270abf0da42c"

        private lateinit var wireMock: WireMockServer

        /**
         * What a request to a URL triggers once it reached WireMock, per test ([arrivalOf]) - behind ONE listener for
         * the class, registered with the server: `resetAll()` does not remove a request listener, so one added per
         * test would pile up and fire for a later test's request to the same URL. Cleared before every test.
         */
        private val arrivals = ConcurrentHashMap<String, () -> Unit>()

        /** A latch that opens once a request to [url] reached WireMock in the current test. */
        private fun arrivalOf(url: String): CountDownLatch = CountDownLatch(1).also { latch -> arrivals[url] = latch::countDown }

        @JvmStatic
        @BeforeAll
        fun startWireMock() {
            wireMock =
                WireMockServer(wireMockConfig().dynamicPort())
                    .apply { start() }
                    .also { server -> server.addMockServiceRequestListener { request, _ -> arrivals[request.url]?.invoke() } }
        }

        @JvmStatic
        @AfterAll
        fun stopWireMock() {
            wireMock.stop()
        }
    }

    private val exchangeLogger = LoggerFactory.getLogger(EXCHANGE_LOGGER) as Logger
    private var previousExchangeLevel: Level? = null

    /** Runs the calls a test needs to interrupt, or to run under a caller-side MDC, off the test thread. */
    private val callers: ExecutorService = Executors.newVirtualThreadPerTaskExecutor()

    @BeforeEach
    fun setUp() {
        wireMock.resetAll()
        arrivals.clear()
        // The runner does not initialise Boot's LoggingSystem, so the reference `logging.level` block does not
        // apply here; the level is set by hand to the value that block recommends (INFO = every exchange).
        previousExchangeLevel = exchangeLogger.level
        exchangeLogger.level = Level.INFO
    }

    @AfterEach
    fun tearDown() {
        callers.shutdownNow()
        exchangeLogger.level = previousExchangeLevel
    }

    /**
     * A context the way the host assembles it: Boot's RestClient auto-configuration (the builder bean and its
     * customizers), a fully wired OBSERVATION side (a recording `ObservationRegistry` plus Boot's
     * `ObservationRestClientCustomizer`, which is what puts the registry on the builder), the library's
     * auto-configuration (its interceptor as a customizer), the reference `application.yml` for the
     * `adapter-logging` and `app.restclient` blocks, and the factory with its collaborators. One mesh endpoint,
     * `GET /things/{id}` against WireMock, carrying the token header so the masking can be observed.
     *
     * **Why the registry is hand-built and Boot's `MetricsAutoConfiguration` is NOT used.** It would supply the
     * `DefaultMeterObservationHandler` for free, but it also brings `MeterRegistryPostProcessor`, which calls
     * `Metrics.addRegistry(...)` - every context would publish its registry into the JVM-GLOBAL
     * `Metrics.globalRegistry` and never take it out again. The handler is what makes the registry live at all:
     * Micrometer counts a handler-less registry as noop, and a noop registry records nothing however well the
     * rest is wired.
     *
     * The three Resilience4j registries are beans of their own, and [BlockingResilienceDecorator] is built over
     * them with the context's `ObservationRegistry` and this module's worker name, as [RestClientConfiguration]
     * builds it in the host. That configuration itself is NOT imported: it also pulls in the core's mesh
     * wiring, whose startup guard needs Resilience4j's auto-configuration on the registries - which this
     * hand-assembled context deliberately leaves out. [BoundRestClientProperties] stands in for its one other
     * contribution, the `app.restclient` binding.
     *
     * [tracer] is the one knob that switches legatium between its two "observed" verdicts. The library decides
     * by BEAN LOOKUP on the type `io.micrometer.tracing.Tracer`, so any bean of that type is enough; contexts
     * that pass nothing stay in the untraced mode this class otherwise pins.
     */
    private fun hostContext(
        backend: String,
        timeLimiter: Duration = Duration.ofSeconds(30),
        tracer: Tracer? = null,
        maxConnections: Int = MeshProperties.Transport().maxConnections,
    ): ApplicationContextRunner {
        val meters = SimpleMeterRegistry()
        val observations = ObservationRegistry.create().apply { observationConfig().observationHandler(DefaultMeterObservationHandler(meters)) }
        val circuitBreakers = CircuitBreakerRegistry.ofDefaults()
        val timeLimiters = TimeLimiterRegistry.of(TimeLimiterConfig.custom().timeoutDuration(timeLimiter).build())
        val bulkheads = BulkheadRegistry.ofDefaults()
        return ApplicationContextRunner()
            .withInitializer { context ->
                context.environment.propertySources.addFirst(MapPropertySource(REFERENCE_PROPERTY_SOURCE, referenceProperties()))
            }.withConfiguration(
                AutoConfigurations.of(
                    RestClientAutoConfiguration::class.java,
                    // @ConditionalOnBean(ObservationRegistry) - it contributes the customizer that places the
                    // registry on Boot's builder, which is the step the module toggle then leaves alone.
                    RestClientObservationAutoConfiguration::class.java,
                    ClientLoggingAutoConfiguration::class.java,
                ),
            ).withBean(SimpleMeterRegistry::class.java, Supplier { meters })
            .withBean(ObservationRegistry::class.java, Supplier { observations })
            // The module's OWN @EnableConfigurationProperties binding, not a hand-made instance: binding from
            // the Environment is part of what this context reproduces, so a reference `app.restclient.*` value
            // that never arrives fails a test here instead of silently degrading to the code default.
            .withUserConfiguration(BoundRestClientProperties::class.java)
            .withBean(
                MeshProperties::class.java,
                Supplier {
                    MeshProperties(
                        defaults =
                            MeshProperties.Defaults(
                                individualRequestTimeoutMs = 1000,
                                transport = MeshProperties.Transport(sidecarBaseUrl = wireMock.baseUrl(), maxConnections = maxConnections),
                            ),
                        endpoints =
                            listOf(MeshProperties.EndpointConfig(triplet = "/endpoint/localhost/GET/things/{id}", alias = backend)),
                    )
                },
            ).withBean(CircuitBreakerRegistry::class.java, Supplier { circuitBreakers })
            .withBean(TimeLimiterRegistry::class.java, Supplier { timeLimiters })
            .withBean(BulkheadRegistry::class.java, Supplier { bulkheads })
            .withBean(
                BlockingResilienceDecorator::class.java,
                Supplier {
                    BlockingResilienceDecorator(circuitBreakers, timeLimiters, bulkheads, observations, RestClientConfiguration.WORKER_NAME_PREFIX)
                },
            ).withBean(ObjectMapper::class.java, Supplier { JsonMapper.builder().build() })
            .withBean(ResilientRestClientFactory::class.java)
            .withBean(RestClientObservationReport::class.java)
            .let { runner -> tracer?.let { bean -> runner.withBean(Tracer::class.java, Supplier { bean }) } ?: runner }
    }

    /** The `app.restclient` binding alone - see [hostContext] for why not [RestClientConfiguration]. */
    @EnableConfigurationProperties(RestClientProperties::class)
    class BoundRestClientProperties

    /**
     * The [REFERENCE_PREFIXES] keys of the reference `src/test/resources/application.yml`, flattened the way
     * Boot would load them. Read from the real file rather than restated here: a copy would keep passing after
     * the reference value changed, which is the one thing these assertions exist to catch.
     */
    private fun referenceProperties(): Map<String, Any> =
        YamlPropertySourceLoader()
            .load(ConfigurationFiles.REFERENCE, ClassPathResource(ConfigurationFiles.REFERENCE))
            .filterIsInstance<EnumerablePropertySource<*>>()
            .flatMap { source ->
                source.propertyNames
                    .filter { name -> REFERENCE_PREFIXES.any(name::startsWith) }
                    .map { it to source.getProperty(it)!! }
            }.toMap()
            .also { loaded ->
                // A prefix that silently matches nothing would turn every assertion below into a tautology
                // about code defaults - exactly the failure this whole test guards against.
                REFERENCE_PREFIXES.forEach { prefix ->
                    assertThat(loaded.keys)
                        .`as`("reference application.yml must carry '$prefix*' keys")
                        .anyMatch { it.startsWith(prefix) }
                }
            }

    /**
     * Attaches a recording appender to the exchange logger for the duration of [block], recording only the
     * events of [backend] - the logger is process-wide, and a late event of an earlier test's abandoned worker
     * must not be counted towards this one's.
     */
    private fun capturing(
        backend: String,
        expectedEvents: Int,
        block: (RecordingAppender) -> Unit,
    ) {
        val appender = RecordingAppender(expectedEvents, adapterName = backend).apply { start() }
        exchangeLogger.addAppender(appender)
        try {
            block(appender)
        } finally {
            exchangeLogger.detachAppender(appender)
            appender.stop()
        }
    }

    private fun AssertableApplicationContext.client(backend: String): ResilientRestClient = getBean(ResilientRestClientFactory::class.java).create(backend)

    /**
     * Runs [block] with a recorder attached to [logger], and returns whatever it returns.
     *
     * For lines written while the context REFRESHES - the factory's configuration summary, the library's wiring
     * verdict - the recorder has to be in place around `run(...)` rather than inside the context consumer, where
     * they have already been written.
     */
    private fun <T> recording(
        logger: Logger,
        block: (RecordingAppender) -> T,
    ): T {
        val appender = RecordingAppender(expected = 1).apply { start() }
        // The wiring verdicts are DEBUG lines (legatium's report; the factory's summary is INFO) and
        // logback-test.xml roots at INFO, so the logger is opened to DEBUG for the recording and restored after.
        val previousLevel = logger.level
        logger.level = Level.DEBUG
        logger.addAppender(appender)
        try {
            return block(appender)
        } finally {
            logger.detachAppender(appender)
            appender.stop()
            logger.level = previousLevel
        }
    }

    /** The one recorded line containing [needle], as its rendered message. */
    private fun RecordingAppender.lineContaining(needle: String): String = events().map { it.formattedMessage }.single { it.contains(needle) }

    private fun ILoggingEvent.field(name: String): Any? = keyValuePairs?.singleOrNull { it.key == name }?.value

    @Nested
    inner class `One event per exchange` {
        @Test
        fun `should write exactly one event on the library's logger with the backend name and the template`() {
            // What is tested: the wiring end to end for the plain case - one GET through a client of the factory
            //   in a host-shaped context produces legatium's exchange event.
            // Success criteria: the event is on `adapter-http-exchange`, INFO / success / 200, carries the backend
            //   name as `adapter_name` and the mesh TEMPLATE as `adapter_url_template`, and was written on the
            //   decorator's worker thread.
            // Why it matters: the event reaches the factory's clients only through Boot's builder bean, and the
            //   backend name only through a request attribute; either missing makes every exchange anonymous on
            //   the dashboards the runbook is built on.
            //
            // Given: a host context and a 200 answer
            wireMock.stubFor(get(urlEqualTo("/things/42")).willReturn(okJson("""{"message":"hi"}""")))
            hostContext("things").run { context ->
                capturing(backend = "things", expectedEvents = 1) { appender ->
                    // When: one GET through a client of the factory
                    context.client("things").get(Greeting::class.java) { it.uri("/things/42") }

                    // Then: one event, on the library's logger, INFO / success, naming the backend and the
                    // template - the two fields the runbook groups by - written on the decorator's worker
                    val event = appender.awaitEvents().last()
                    assertThat(event.loggerName).isEqualTo(EXCHANGE_LOGGER)
                    assertThat(event.level).isEqualTo(Level.INFO)
                    assertThat(event.formattedMessage).startsWith("Adapter http exchange GET ").contains("things -> 200")
                    assertThat(event.field("adapter_outcome")).isEqualTo("success")
                    assertThat(event.field("adapter_response_status_code")).isEqualTo(200)
                    assertThat(event.field("adapter_name")).isEqualTo("things")
                    assertThat(event.field("adapter_url_template")).isEqualTo("/things/{id}")
                    assertThat(event.threadName).startsWith("restclient-call-")
                }
            }
        }

        @Test
        fun `should write no event for a request that never became an exchange`() {
            // What is tested: a serialisation failure produces NO exchange event, because it never reached the
            //   RestClient - and therefore never the library's interceptor.
            // Success criteria: two calls, one failing while serialising, one real; exactly one event, the
            //   second. The successful call is the ordering fence: awaiting ITS event means anything the first
            //   call was going to emit has had its turn.
            // Why it matters: a log whose consumers count real calls must not count a call that never went out -
            //   a phantom exchange would inflate the request rate and, being a failure, the error rate.
            //
            // Given
            wireMock.stubFor(post(urlEqualTo("/things/42")).willReturn(okJson("""{"message":"ok"}""")))
            hostContext("phantom").run { context ->
                capturing(backend = "phantom", expectedEvents = 1) { appender ->
                    val client = context.client("phantom")

                    // When: a call that fails during serialisation, then a well-formed one
                    runCatching { client.post(Greeting::class.java, Unserialisable()) { it.uri("/things/42") } }
                    client.post(Greeting::class.java, LookupRequest("fence")) {
                        it.uri("/things/42").contentType(MediaType.APPLICATION_JSON)
                    }

                    // Then: only the real exchange was logged
                    val events = appender.awaitEvents()
                    assertThat(events).singleElement()
                    assertThat(events.single().formattedMessage).contains("POST", "phantom -> 200")
                }
            }
        }
    }

    @Nested
    inner class `Headers and the backend identity` {
        @Test
        fun `should show the Host header in plaintext and the token as a fingerprint`() {
            // What is tested: the reference `adapter-logging` header configuration - every header logged, masked
            //   by default, Host on the plaintext allowlist.
            // Success criteria: the rendered request headers name the Host with its value and the token
            //   header with a `<length>:<16 hex>` fingerprint, and the secret itself appears nowhere on the event.
            // Why it matters: the token must never reach a log index, while the Host header is how an operator
            //   reads the backend off a line; one configuration has to get both right.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/things/1")).willReturn(okJson("""{"message":"hi"}""")))
            hostContext("masked").run { context ->
                capturing(backend = "masked", expectedEvents = 1) { appender ->
                    // When
                    context.client("masked").get(Greeting::class.java) { it.uri("/things/1").header(TOKEN_HEADER, TOKEN_VALUE) }

                    // Then
                    val event = appender.awaitEvents().last()
                    val requestHeaders = event.field("adapter_request_headers") as String
                    assertThat(requestHeaders).containsIgnoringCase("Host").contains("localhost")
                    assertThat(requestHeaders).containsIgnoringCase(TOKEN_HEADER)
                    assertThat(requestHeaders).containsPattern("${TOKEN_VALUE.length}:[0-9a-f]{16}")
                    assertThat(event.formattedMessage + event.keyValuePairs.joinToString()).doesNotContain(TOKEN_VALUE)
                }
            }
        }
    }

    @Nested
    inner class `The outcome vocabulary` {
        @Test
        fun `should log a 404 as INFO rejected with its status and the error body`() {
            // What is tested: a 4xx answer is `rejected` for the library (the peer answered), carried by its
            //   status code, and its body is logged - in the reference `always` mode here, as it would be in
            //   `on-failure` (a rejection is not a success).
            // Success criteria: one INFO event with `adapter_outcome=rejected`, status 404 and the error body.
            // Why it matters: a 404 is a business answer; logged as a failure it would page someone for every
            //   routine "not found", and without its body nobody could tell which one it was.
            //
            // Given: a 404 with a JSON error body
            wireMock.stubFor(
                get(urlEqualTo("/things/404")).willReturn(aResponse().withStatus(404).withBody("""{"error":"no such thing"}""")),
            )
            hostContext("benign").run { context ->
                capturing(backend = "benign", expectedEvents = 1) { appender ->
                    // When
                    runCatching { context.client("benign").get(Greeting::class.java) { it.uri("/things/404") } }

                    // Then
                    val event = appender.awaitEvents().last()
                    assertThat(event.level).isEqualTo(Level.INFO)
                    assertThat(event.field("adapter_outcome")).isEqualTo("rejected")
                    assertThat(event.field("adapter_response_status_code")).isEqualTo(404)
                    assertThat(event.field("adapter_response_body") as String?).contains("no such thing")
                }
            }
        }

        @Test
        fun `should log a 429 as WARN rejected - the escalation set of legatium's ADR-0012`() {
            // What is tested: the level half of the 4xx classification. A 401, 403, 408 or 429 is still
            //   `rejected` on the outcome, but at WARN. The 429 stands for the set; the set itself is pinned in
            //   the library.
            // Success criteria: one WARN event, `adapter_outcome=rejected`, status 429.
            // Why it matters: a throttled call is a 4xx the operator must see - the backend is shedding load -
            //   and WARN is the level the alerts filter on.
            //
            // Given: a throttling answer
            wireMock.stubFor(
                get(urlEqualTo("/things/throttled")).willReturn(aResponse().withStatus(429).withBody("""{"error":"slow down"}""")),
            )
            hostContext("throttled").run { context ->
                capturing(backend = "throttled", expectedEvents = 1) { appender ->
                    // When
                    runCatching { context.client("throttled").get(Greeting::class.java) { it.uri("/things/throttled") } }

                    // Then
                    val event = appender.awaitEvents().single()
                    assertThat(event.level).isEqualTo(Level.WARN)
                    assertThat(event.field("adapter_outcome")).isEqualTo("rejected")
                    assertThat(event.field("adapter_response_status_code")).isEqualTo(429)
                }
            }
        }

        @Test
        fun `should log a 500 as WARN failure with the error body, and a 200 with its body in the reference always mode`() {
            // What is tested: the two ends of the outcome vocabulary on one backend - a 500 and a 200 - under the
            //   reference body mode `always`.
            // Success criteria: the 500 is a WARN `failure` carrying its error body; the 200 is INFO and carries
            //   its body too.
            // Why it matters: `failure` is what the error-rate alert counts; and the body of a success in the log
            //   is a deliberate rollout posture of the reference file, pinned so a change to it is a decision.
            //
            // Given: a failing and a succeeding endpoint on one backend
            wireMock.stubFor(get(urlEqualTo("/things/500")).willReturn(aResponse().withStatus(500).withBody("""{"error":"boom"}""")))
            wireMock.stubFor(get(urlEqualTo("/things/200")).willReturn(okJson("""{"message":"fine"}""")))
            hostContext("server").run { context ->
                capturing(backend = "server", expectedEvents = 2) { appender ->
                    val client = context.client("server")

                    // When
                    runCatching { client.get(Greeting::class.java) { it.uri("/things/500") } }
                    client.get(Greeting::class.java) { it.uri("/things/200") }

                    // Then: the 5xx is a WARN failure carrying its body; the 200 carries its body too, because
                    // the reference file logs bodies in `always` mode (the configuration case below pins that)
                    val events = appender.awaitEvents()
                    val failure = events.single { it.field("adapter_response_status_code") == 500 }
                    assertThat(failure.level).isEqualTo(Level.WARN)
                    assertThat(failure.field("adapter_outcome")).isEqualTo("failure")
                    assertThat(failure.field("adapter_response_body") as String?).contains("boom")
                    val success = events.single { it.field("adapter_response_status_code") == 200 }
                    assertThat(success.level).isEqualTo(Level.INFO)
                    assertThat(success.field("adapter_response_body") as String?).contains("fine")
                }
            }
        }

        @Test
        fun `should log the transport's response timeout as WARN timeout`() {
            // What is tested: the one client-side disposition with its own outcome value - the per-endpoint
            //   responseTimeout expiring on the worker (1400 ms at the 1 s anchor), the ladder's normal case for
            //   a stalled backend.
            // Success criteria: a backend slower than the response timeout yields one WARN event with
            //   `adapter_outcome=timeout` and no status.
            // Why it matters: this is the outcome an alert on a slow backend has to key on; the two cancellation
            //   cases below produce a different one.
            //
            // Given: a backend slower than the 1400 ms response timeout
            wireMock.stubFor(get(urlEqualTo("/things/slow")).willReturn(okJson("{}").withFixedDelay(3_000)))
            hostContext("transport-timeout").run { context ->
                capturing(backend = "transport-timeout", expectedEvents = 1) { appender ->
                    // When
                    runCatching { context.client("transport-timeout").get(Greeting::class.java) { it.uri("/things/slow") } }

                    // Then
                    val event = appender.awaitEvents().last()
                    assertThat(event.level).isEqualTo(Level.WARN)
                    assertThat(event.field("adapter_outcome")).isEqualTo("timeout")
                    assertThat(event.field("adapter_response_status_code")).isNull()
                }
            }
        }

        @Test
        fun `should log a pool-lease timeout as WARN timeout`() {
            // What is tested: the third pre-response guard - no pooled connection becoming free within the
            //   200 ms pending-acquire timeout. Apache raises ConnectionRequestTimeoutException, a bare
            //   InterruptedIOException that no JDK timeout type covers; legatium 1.2.1 matches it by class name.
            // Success criteria: with a pool of ONE connection held by a slow call, a second call fails within the
            //   lease window, and its event is WARN `adapter_outcome=timeout` without a status - not the ERROR
            //   `failure` legatium 1.2.0 wrote for it.
            // Why it matters: pool exhaustion is a saturation signal, not a backend fault, and before 1.2.1 it
            //   landed at the level alerting fires on (finding 16 of the 2026-10-21 defect analysis). This pins
            //   the upgrade where veredium wires the pool, so a downgrade or a changed exception type shows up.
            //
            // Given: a pool of one connection, held by a call to a 3 s backend that is already on the wire
            wireMock.stubFor(get(urlEqualTo("/things/hold")).willReturn(okJson("{}").withFixedDelay(3_000)))
            val inFlight = arrivalOf("/things/hold")
            hostContext("pool-lease", maxConnections = 1).run { context ->
                capturing(backend = "pool-lease", expectedEvents = 1) { appender ->
                    val client = context.client("pool-lease")
                    val holder = callers.submit<Throwable?> { runCatching { client.get(Greeting::class.java) { it.uri("/things/hold") } }.exceptionOrNull() }
                    assertThat(inFlight.await(5, TimeUnit.SECONDS)).`as`("the holding call must have the only connection").isTrue()

                    // When: a second call needs a connection the pool cannot lend
                    val failure = runCatching { client.get(Greeting::class.java) { it.uri("/things/1") } }.exceptionOrNull()

                    // Then: the caller sees the lease timeout, and the first event - the lease one; the holder's
                    // own event arrives only at its response timeout - is a WARN timeout
                    assertThat(failure).isInstanceOf(ResourceAccessException::class.java)
                    assertThat(generateSequence(failure) { it.cause }.map { it.javaClass.name }.toList())
                        .contains("org.apache.hc.core5.http.ConnectionRequestTimeoutException")
                    val event = appender.awaitEvents().first()
                    assertThat(event.level).isEqualTo(Level.WARN)
                    assertThat(event.field("adapter_outcome")).isEqualTo("timeout")
                    assertThat(event.field("adapter_response_status_code")).isNull()
                    holder.get(10, TimeUnit.SECONDS)
                }
            }
        }

        @Test
        fun `should log a TimeLimiter timeout as the cancelled worker's failure, not as a timeout`() {
            // What is tested: the resilience budget expiring BEFORE the transport gives up. The caller is
            //   released at 200 ms with a TimeoutException and the worker's future is cancelled; the interrupt
            //   closes the worker's socket, so the exchange ends at once with `Closed by interrupt` - which the
            //   library classifies as a `failure` at ERROR, not as a `timeout` and not as the reactive twin's
            //   `cancelled`.
            // Success criteria: the caller's failure is the TimeLimiter's; the event is ERROR `failure`,
            //   written on the worker thread, and its duration is well below the 1400 ms response timeout - the
            //   proof that the cancellation freed the worker rather than leaving it to the transport.
            // Why it matters: an alert on `adapter_outcome == "cancelled"` would never fire for this module, and
            //   one on `timeout` would miss every budget expiry. The resilience verdict itself stays on
            //   ResilientRestClient's transport line (`TimeoutException`); the exchange event reports what the
            //   wire saw, and this is what it saw.
            //
            // Given: a 200 ms budget against a 3 s backend
            wireMock.stubFor(get(urlEqualTo("/things/budget")).willReturn(okJson("{}").withFixedDelay(3_000)))
            hostContext("budget", timeLimiter = Duration.ofMillis(200)).run { context ->
                capturing(backend = "budget", expectedEvents = 1) { appender ->
                    // When
                    val failure =
                        runCatching { context.client("budget").get(Greeting::class.java) { it.uri("/things/budget") } }
                            .exceptionOrNull()

                    // Then: the caller saw the budget expire ...
                    assertThat(failure).isInstanceOf(java.util.concurrent.TimeoutException::class.java)
                    // ... and the worker's exchange ended with the cancellation, promptly
                    val event = appender.awaitEvents().last()
                    assertThat(event.level).isEqualTo(Level.ERROR)
                    assertThat(event.field("adapter_outcome")).isEqualTo("failure")
                    assertThat(event.field("adapter_response_status_code")).isNull()
                    assertThat(event.threadName).startsWith("restclient-call-")
                    assertThat((event.field("adapter_duration_ms") as Number).toLong())
                        .`as`("the cancellation must free the worker long before the 1400 ms response timeout")
                        .isLessThan(1000)
                }
            }
        }

        @Test
        fun `should log an interrupted caller's exchange as the cancelled worker's failure`() {
            // What is tested: the blocking counterpart of the twin's "caller's cancel" case. The interrupt
            //   releases the caller and cancels the worker's future; the worker's socket is closed by that
            //   interrupt and the library sees the exchange end at once.
            // Success criteria: the caller gets an InterruptedException; one ERROR `failure` event with no
            //   status follows, written on the worker.
            // Why it matters: the blocking stack has no `cancelled` outcome - a caller walking away surfaces as a
            //   `failure` at ERROR. An alert keyed on `cancelled` would never fire here; this records the
            //   difference to the reactive twin as a test instead of a surprise.
            //
            // Given: a backend that does not answer within the window, and a caller on its own thread; the
            // interrupt is sent only once the request is on the wire, otherwise the worker may be cancelled
            // before it ever sent anything and there would be no exchange to log
            wireMock.stubFor(get(urlEqualTo("/things/never")).willReturn(okJson("{}").withFixedDelay(30_000)))
            val inFlight = arrivalOf("/things/never")
            hostContext("walked-away").run { context ->
                capturing(backend = "walked-away", expectedEvents = 1) { appender ->
                    val client = context.client("walked-away")
                    val callerThread = AtomicReference<Thread>()
                    val started = CountDownLatch(1)
                    val outcome =
                        callers.submit<Throwable?> {
                            callerThread.set(Thread.currentThread())
                            started.countDown()
                            runCatching { client.get(Greeting::class.java) { it.uri("/things/never") } }.exceptionOrNull()
                        }
                    assertThat(started.await(5, TimeUnit.SECONDS)).isTrue()
                    assertThat(inFlight.await(5, TimeUnit.SECONDS)).`as`("the request must have reached the backend").isTrue()

                    // When: the caller walks away before the answer arrives
                    callerThread.get().interrupt()

                    // Then: the caller is released at once, and the worker's exchange ends with the cancellation
                    assertThat(outcome.get(5, TimeUnit.SECONDS)).isInstanceOf(InterruptedException::class.java)
                    val event = appender.awaitEvents().last()
                    assertThat(event.level).isEqualTo(Level.ERROR)
                    assertThat(event.field("adapter_outcome")).isEqualTo("failure")
                    assertThat(event.field("adapter_response_status_code")).isNull()
                    assertThat(event.threadName).startsWith("restclient-call-")
                }
            }
        }
    }

    @Nested
    inner class `Body capture` {
        @Test
        fun `should cap a logged body at max-body-bytes and note the total`() {
            // What is tested: the reference 16 KB cap - the LOGGED body is truncated with the library's note, the
            //   wire body is untouched.
            // Success criteria: the logged request body says `truncated` and how many bytes there were, is no
            //   longer than the cap plus the note, and WireMock received the full 20 000-character value.
            // Why it matters: the cap bounds what one log line costs; a cap that also cut the wire body would
            //   corrupt the request, and one that did not apply would put megabytes on a line.
            //
            // Given: a POST whose body is well past the cap, answered with a 500 - logged in the reference
            //   `always` mode, and it would be in `on-failure` too
            val longValue = "X".repeat(20_000)
            wireMock.stubFor(post(urlEqualTo("/things/42")).willReturn(aResponse().withStatus(500).withBody("{}")))
            hostContext("capped").run { context ->
                val cap = context.getBean(ClientLoggingProperties::class.java).maxBodyBytes
                capturing(backend = "capped", expectedEvents = 1) { appender ->
                    // When
                    runCatching {
                        context
                            .client("capped")
                            .post(Greeting::class.java, LookupRequest(longValue)) {
                                it.uri("/things/42").contentType(MediaType.APPLICATION_JSON)
                            }
                    }

                    // Then: the logged body is bounded by the cap plus the note, and says how much there was
                    val logged = appender.awaitEvents().last().field("adapter_request_body") as String
                    assertThat(logged).contains("truncated").containsPattern("\\d+ bytes total")
                    assertThat(logged.length).isLessThan(cap + 64)
                    // ... while the whole body reached the backend
                    wireMock.verify(
                        WireMock
                            .postRequestedFor(urlEqualTo("/things/42"))
                            .withRequestBody(WireMock.containing(longValue)),
                    )
                }
            }
        }
    }

    @Nested
    inner class `The reference configuration` {
        @Test
        fun `should bind the adapter-logging block of the reference application yml as written`() {
            // What is tested: the `adapter-logging` block of the reference file, bound by the library's own
            //   properties class in a host-shaped context.
            // Success criteria: enabled, the library's default logger name, every header included, bodies in
            //   `always` mode, the 16 KB cap.
            // Why it matters: the runbook's queries assume these values; a key that does not bind (a typo, a
            //   renamed property after a library upgrade) silently falls back to the library default.
            //
            // Given / When: the context bound the reference file
            hostContext("config").run { context ->
                val properties = context.getBean(ClientLoggingProperties::class.java)

                // Then: the values the runbook relies on
                assertThat(properties.enabled).isTrue()
                assertThat(properties.loggerName).isEqualTo(EXCHANGE_LOGGER)
                assertThat(properties.requestHeaders.includes).containsExactly("*")
                assertThat(properties.responseHeaders.includes).containsExactly("*")
                assertThat(properties.logRequestBody).isEqualTo(BodyLogMode.ALWAYS)
                assertThat(properties.logResponseBody).isEqualTo(BodyLogMode.ALWAYS)
                assertThat(properties.maxBodyBytes).isEqualTo(16_384)
            }
        }

        @Test
        fun `should bind the app-restclient block of application yml instead of falling back to code defaults`() {
            // What is tested: that `app.restclient.*` of the reference application.yml actually REACHES
            //   RestClientProperties in a host-shaped context - binding through the module's own
            //   `@EnableConfigurationProperties`, not through an instance this test constructs.
            // Success criteria: `observability.http-observation-enabled` is asserted against the code
            //   default in the same test - it is the one key whose reference value differs, so it is the only
            //   one that can tell a real binding apart from a silent fallback - and the rest of the block binds
            //   to the values the file states.
            // Why it matters: a block that never binds leaves the host on code defaults while its file says
            //   otherwise - observation off, validation off - and nothing reports the difference.
            //
            // Given / When: the context bound the reference file
            hostContext("restclient-config").run { context ->
                val properties = context.getBean(RestClientProperties::class.java)

                // Then: the one key whose reference value contradicts the code default
                assertThat(properties.observability.httpObservationEnabled)
                    .`as`("app.restclient.observability.http-observation-enabled, bound from the reference application.yml")
                    .isTrue()
                assertThat(RestClientProperties().observability.httpObservationEnabled)
                    .`as`("the code default the binding had to override for the assertion above to mean anything")
                    .isFalse()
                // ... and the rest of the block, pinned so a drift in the reference file is visible here too
                assertThat(properties.maxInMemorySize).isEqualTo(DataSize.ofKilobytes(256))
                assertThat(properties.validation.enabled).isTrue()
                assertThat(properties.validation.failOnViolation).isFalse()
            }
        }
    }

    /**
     * What the reference `http-observation-enabled: true` actually produces once a context can act on it.
     *
     * By default this context is wired OBSERVED BUT NOT TRACED: `micrometer-tracing` is on the test classpath,
     * but no tracing auto-configuration is registered here, so the context holds no `io.micrometer.tracing.Tracer`
     * bean - which is exactly the bean legatium looks up to choose between its two "observed" verdicts. The
     * second verdict is reached with the smallest thing that is a `Tracer` AT ALL - [Tracer.NOOP] - which also
     * makes the honest point that a Tracer BEAN traces nothing by itself.
     */
    @Nested
    inner class `The observation wiring` {
        @Test
        fun `should report the toggle as plain on, with the library agreeing that the observation is wired`() {
            // What is tested: that the reference `http-observation-enabled: true` reaches a context which can ACT
            //   on it, and that the two independent verdicts written at startup now tell the same story.
            // Success criteria: the context's ObservationRegistry records (is not noop); the module's startup
            //   line reports the toggle `on (Boot's registry stays on the builder)`; legatium's line reports that
            //   it found Boot's client observation.
            // Why it matters: the two lines are written by different code from different evidence; an operator
            //   reads them side by side at startup, and a context in which they contradicted each other would
            //   send that reading the wrong way.
            //
            // Given: recorders on both loggers, in place before the context refreshes
            val reportLogger = LoggerFactory.getLogger(RestClientObservationReport::class.java) as Logger
            val libraryLogger = LoggerFactory.getLogger(ClientLoggingAutoConfiguration::class.java) as Logger
            recording(reportLogger) { reportLines ->
                recording(libraryLogger) { libraryLines ->
                    // When
                    hostContext("observation-toggle").run { context ->
                        // The premise, asserted rather than assumed: a registry exists AND records.
                        assertThat(context).hasSingleBean(ObservationRegistry::class.java)
                        assertThat(context.getBean(ObservationRegistry::class.java).isNoop)
                            .`as`("the context's ObservationRegistry must have a handler, else it records nothing")
                            .isFalse()
                    }

                    // Then: the module's verdict - configured AND effective
                    assertThat(reportLines.lineContaining("http-observation-enabled"))
                        .contains("app.restclient.observability.http-observation-enabled=true")
                        .contains("on (Boot's registry stays on the builder)")
                        .doesNotContain("nothing is observed")

                    // ... and the library's, reached by a completely different route
                    assertThat(libraryLines.lineContaining("client observation"))
                        .contains("found Boot's client observation")
                        .doesNotContain("found no client observation")
                }
            }
        }

        @Test
        fun `should record an http client observation per call, tagged with the mesh path template`() {
            // What is tested: the toggle END TO END - a real call through a client of this factory OPENS the
            //   `http.client.requests` observation on the worker and it lands in the host's MeterRegistry, with
            //   its `uri` tag the mesh endpoint's low-cardinality TEMPLATE rather than the expanded path.
            // Success criteria: no timer before the call; exactly one `http.client.requests` timer after it,
            //   tagged `uri=/things/{id}`.
            // Why it matters: an expanded path as the tag mints one series per id - a cardinality explosion in the
            //   host's metrics backend; the template stamped on every request is the only guard.
            //
            // Given: a backend and a context with the observation fully wired
            wireMock.stubFor(get(urlEqualTo("/things/7")).willReturn(okJson("""{"message":"hi"}""")))
            hostContext("observed").run { context ->
                val meters = context.getBean(SimpleMeterRegistry::class.java)
                assertThat(meters.find(HTTP_CLIENT_OBSERVATION).timer())
                    .`as`("nothing may be recorded before the call")
                    .isNull()

                // When: one real call through a client of the factory
                context.client("observed").get(Greeting::class.java) { it.uri("/things/7") }

                // Then: the observation ran and was recorded, under the path TEMPLATE
                val timer = meters.find(HTTP_CLIENT_OBSERVATION).timer()
                assertThat(timer).`as`("the '%s' observation must have been recorded", HTTP_CLIENT_OBSERVATION).isNotNull()
                assertThat(timer!!.count()).isEqualTo(1L)
                assertThat(timer.id.getTag("uri")).isEqualTo("/things/{id}")
            }
        }

        @Test
        fun `should generate its own correlation header because the observation is wired without tracing`() {
            // What is tested: the OBSERVABLE consequence of legatium's "observed, not traced" verdict - the
            //   request WireMock received carries the library's configured correlation header with a generated
            //   value and NO `traceparent`.
            // Success criteria: the premise holds (no Tracer bean), and WireMock received the correlation header
            //   with a non-empty value and no `traceparent` header.
            // Why it matters: without tracing, the correlation header is the only id that joins this call to the
            //   peer's logs; its absence would leave the two sides of an incident unrelatable.
            //
            // Given: this context wires the observation but no tracer
            wireMock.resetRequests()
            wireMock.stubFor(get(urlEqualTo("/things/9")).willReturn(okJson("""{"message":"hi"}""")))
            hostContext("untraced").run { context ->
                assertThat(context)
                    .`as`("the premise: no tracing auto-configuration here, so legatium takes its untraced branch")
                    .doesNotHaveBean(Tracer::class.java)
                val correlationHeader = context.getBean(ClientLoggingProperties::class.java).correlationIdHeader

                // When
                context.client("untraced").get(Greeting::class.java) { it.uri("/things/9") }

                // Then
                wireMock.verify(
                    getRequestedFor(urlEqualTo("/things/9"))
                        .withHeader(correlationHeader, matching(".+"))
                        .withoutHeader("traceparent"),
                )
            }
        }

        @Test
        fun `should flip to the traced verdict on the mere presence of a Tracer bean, which traces nothing`() {
            // What is tested: the OTHER branch of the same verdict, and what it is - and is not - evidence of.
            //   legatium chooses it by looking up a bean of type `io.micrometer.tracing.Tracer`, so the minimal
            //   [Tracer.NOOP] bean is enough to reach it; nothing else about the context changes.
            // Success criteria: legatium's startup line flips to "with Micrometer Tracing wired" and says no
            //   correlation header is generated - while the wire still carries the generated header and no
            //   `traceparent`, because a NOOP tracer starts no span.
            // Why it matters: it records what the verdict is evidence of - a bean, not a working trace - so an
            //   operator reading "tracing wired" knows to check for an actual `traceparent` on the wire.
            //
            // Given: the same context as the untraced case, plus the smallest possible Tracer bean
            wireMock.resetRequests()
            wireMock.stubFor(get(urlEqualTo("/things/11")).willReturn(okJson("""{"message":"hi"}""")))
            val libraryLogger = LoggerFactory.getLogger(ClientLoggingAutoConfiguration::class.java) as Logger
            lateinit var correlationHeader: String
            recording(libraryLogger) { libraryLines ->
                // When
                hostContext("traced", tracer = Tracer.NOOP).run { context ->
                    assertThat(context)
                        .`as`("the premise: one Tracer bean, and it is the one this case put there")
                        .hasSingleBean(Tracer::class.java)
                    correlationHeader = context.getBean(ClientLoggingProperties::class.java).correlationIdHeader

                    context.client("traced").get(Greeting::class.java) { it.uri("/things/11") }
                }

                // Then: the verdict flipped - same lookup, other branch
                assertThat(libraryLines.lineContaining("client observation"))
                    .contains("found Boot's client observation with Micrometer Tracing wired")
                    .contains("no $correlationHeader is generated")
                    .doesNotContain("but no Micrometer Tracing")
            }

            // ... while the wire is unmoved: a Tracer bean starts no span, so there is no traceparent to
            // follow and legatium still generates the request id and sends its correlation header
            wireMock.verify(
                getRequestedFor(urlEqualTo("/things/11"))
                    .withHeader(correlationHeader, matching(".+"))
                    .withoutHeader("traceparent"),
            )
        }
    }

    /**
     * Does a value the CALLER put in its MDC reach the event that the library writes on the worker thread?
     *
     * On the reactive stack this was a documented gap a host had to close with a `ThreadLocalAccessor` of its
     * own. Here it is a guarantee of the module: [BlockingResilienceDecorator] copies the caller's MDC onto the
     * worker for the duration of the call, and the library's interceptor - which captures the MDC at wiring, on
     * that worker - sees it there. No accessor, no host recipe.
     */
    @Nested
    inner class `The caller's MDC` {
        @Test
        fun `should carry a caller's MDC value onto the event although it is written on the worker`() {
            // What is tested: the decorator's MDC hand-off as legatium sees it - a key the caller put in its MDC
            //   appears on the exchange event written on the worker thread.
            // Success criteria: the event was written on a `restclient-call-` thread and its MDC holds the
            //   caller's correlation key and value.
            // Why it matters: correlation keys are how a log search finds every line of one request; an event
            //   written on a fresh thread without them would fall out of every such search.
            //
            // Given: a caller carrying a correlation id in its MDC, on a thread of its own
            wireMock.stubFor(get(urlEqualTo("/things/mdc")).willReturn(okJson("""{"message":"hi"}""")))
            hostContext("mdc-join").run { context ->
                capturing(backend = "mdc-join", expectedEvents = 1) { appender ->
                    // When: the call runs on a dedicated thread, under that MDC
                    callers
                        .submit {
                            MDC.put(CORRELATION_KEY, CORRELATION_VALUE)
                            try {
                                context.client("mdc-join").get(Greeting::class.java) { it.uri("/things/mdc") }
                            } finally {
                                MDC.clear()
                            }
                        }.get(5, TimeUnit.SECONDS)

                    // Then: the event, written on the worker, carries the caller's key
                    val event = appender.awaitEvents().last()
                    assertThat(event.threadName).startsWith("restclient-call-")
                    assertThat(event.mdcPropertyMap).containsEntry(CORRELATION_KEY, CORRELATION_VALUE)
                }
            }
        }
    }

    /**
     * Captures events written on another thread: the library emits on the decorator's worker, and for an
     * abandoned call that happens after the caller has already returned. Snapshots the MDC at append time,
     * because the library's emission scope holds its keys only for the duration of the log statement, and
     * Logback reads the MDC lazily. [awaitEvents] waits for [expected] events, so a capture is complete, not
     * merely safe to read.
     *
     * [adapterName] scopes an exchange-logger capture to one test's backend: the logger is shared by every
     * test in the JVM, and an abandoned worker of an EARLIER test (its caller long released, its socket closed
     * by an interrupt) may write its event while this test is capturing - counted, it would satisfy or break
     * [expected] for the wrong reason. Backend names are unique per test. `null` records everything, for the
     * startup-line loggers, whose lines carry no `adapter_name`.
     */
    private class RecordingAppender(
        private val expected: Int,
        private val adapterName: String? = null,
    ) : AppenderBase<ILoggingEvent>() {
        private val captured = CopyOnWriteArrayList<ILoggingEvent>()
        private val arrived = CountDownLatch(expected)

        override fun append(eventObject: ILoggingEvent) {
            if (adapterName != null && eventObject.keyValuePairs.orEmpty().none { it.key == "adapter_name" && it.value == adapterName }) return
            eventObject.prepareForDeferredProcessing()
            captured += eventObject
            arrived.countDown()
        }

        fun awaitEvents(): List<ILoggingEvent> {
            assertThat(arrived.await(5, TimeUnit.SECONDS))
                .`as`("expected $expected exchange event(s) within 5s, got ${captured.size}")
                .isTrue()
            return captured.toList()
        }

        fun events(): List<ILoggingEvent> = captured.toList()
    }
}
