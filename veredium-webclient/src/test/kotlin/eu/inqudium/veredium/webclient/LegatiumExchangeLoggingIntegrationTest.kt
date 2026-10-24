package eu.inqudium.veredium.webclient

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.containing
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.matching
import com.github.tomakehurst.wiremock.client.WireMock.okJson
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import eu.inqudium.legatium.common.BodyLogMode
import eu.inqudium.legatium.common.ClientLoggingProperties
import eu.inqudium.legatium.webclient.logging.ClientLoggingAutoConfiguration
import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.testsupport.CapturingAppender
import eu.inqudium.veredium.testsupport.ConfigurationFiles
import eu.inqudium.veredium.testsupport.Greeting
import eu.inqudium.veredium.testsupport.LookupRequest
import eu.inqudium.veredium.testsupport.Unserialisable
import eu.inqudium.veredium.testsupport.capturing
import eu.inqudium.veredium.testsupport.field
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import io.micrometer.context.ContextRegistry
import io.micrometer.context.ThreadLocalAccessor
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micrometer.observation.ObservationRegistry
import io.micrometer.tracing.Tracer
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.slf4j.MDCContext
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
import org.springframework.boot.test.context.assertj.AssertableApplicationContext
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.boot.webclient.autoconfigure.WebClientAutoConfiguration
import org.springframework.boot.webclient.autoconfigure.WebClientObservationAutoConfiguration
import org.springframework.core.env.MapPropertySource
import org.springframework.http.MediaType
import org.springframework.util.unit.DataSize
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.json.JsonMapper
import java.time.Duration
import java.util.concurrent.Executors
import java.util.function.Supplier

/**
 * The exchange log of this module: one structured event per outbound HTTP exchange on the
 * `adapter-http-exchange` logger, written by legatium-webclient-logging's filter, which reaches the factory's
 * clients through Boot's `WebClient.Builder` bean and nothing else.
 *
 * The class drives the wiring end to end - a real context with Boot's WebClient auto-configuration, the
 * library's auto-configuration and the module's reference `application.yml` values, a client of
 * [ResilientWebClientFactory], a local WireMock - and pins the SEMANTICS the library gives the reactive stack: a
 * 4xx is `rejected` with its status (INFO, WARN for the escalation set), a 5xx a WARN `failure`, a caller's cancel
 * and a TimeLimiter timeout both `cancelled`; bodies as the reference configuration logs them (`always`) and
 * capped; the backend name as `adapter_name`; the Host header in plaintext next to the token's fingerprint; and
 * whether a value a coroutine caller puts in its `MDCContext` reaches the event. The module's own `HttpExchange`
 * logger stays silent throughout. This is the ONLY per-exchange event. Every log query and every alert keys on
 * its outcome vocabulary, which differs from the module's own transport lines at exactly the points pinned here,
 * so the meaning is recorded as a test rather than rediscovered as a "bug" in production.
 *
 * Contexts run through an [ApplicationContextRunner] with the `adapter-logging` AND `app.webclient` blocks of
 * the reference configuration ([ConfigurationFiles.REFERENCE], a test resource no consumer receives) as a property source, so the
 * configuration under test is the one the module documents - not a copy in this file. [WebClientProperties] is
 * bound from the Environment by its own `@EnableConfigurationProperties` (the binding half of
 * [VerediumWebClientAutoConfiguration]) instead of being handed in as a code-default instance, so a key like
 * `app.webclient.observability.http-observation-enabled`
 * reaches [ResilientWebClientFactory] here by exactly the route it takes in the running application - the
 * difference between a default and a CONFIGURED value is visible, not papered over.
 *
 * `app.mesh.*` is deliberately left out of that property source: the endpoints are hand-built per case (WireMock's
 * dynamic port, a per-case TimeLimiter budget), and the reference triplets would point these tests at hosts that
 * do not exist.
 * No mocks: WireMock, a real context, a Logback appender.
 */
class LegatiumExchangeLoggingIntegrationTest {
    companion object {
        /** legatium's default exchange logger - the module keeps the default (application.yml). */
        private const val EXCHANGE_LOGGER = "adapter-http-exchange"

        /**
         * A logger name in the module's own package that nothing writes to: the per-exchange event is
         * legatium's alone, on [EXCHANGE_LOGGER], and a second one under the module's name would split every
         * query over the exchanges in two.
         */
        private const val SILENT_MODULE_LOGGER = "eu.inqudium.veredium.webclient.HttpExchange"

        /**
         * The prefixes of the reference `application.yml` this test binds. `adapter-logging.` configures the
         * library under test; `app.webclient.` configures [WebClientProperties], whose
         * `observability.http-observation-enabled` is a key whose reference value (`true`) DIFFERS from the
         * class's code default (`false`) - so loading it is what makes the two distinguishable here. (The
         * validation keys differ from their code defaults too; the toggle is the one these contexts act on.)
         *
         * `app.mesh.` is absent on purpose - see the class KDoc.
         */
        private val REFERENCE_PREFIXES = arrayOf("adapter-logging.", "app.webclient.")

        /** Name of the property source the reference keys are added under - for readable failure diagnostics. */
        private const val REFERENCE_PROPERTY_SOURCE = "reference-application-yml"

        /** The fence call of [fencedEvents] - a path of every case's endpoint template, and a status no case uses. */
        private const val FENCE_PATH = "/things/fence"
        private const val FENCE_STATUS = 202

        /** Spring's WebClient client observation - the meter the wired observation must produce. */
        private const val HTTP_CLIENT_OBSERVATION = "http.client.requests"

        private const val TOKEN_HEADER = "X-Access-Token"
        private const val TOKEN_VALUE = "a-bearer-secret-that-must-never-be-logged"

        /** A caller-side MDC key/value, shaped like a host's outbound correlation id. */
        private const val CORRELATION_KEY = "CORRELATION_ID"
        private const val CORRELATION_VALUE = "Out:7cc0270abf0da42c"

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

    private val exchangeLogger = LoggerFactory.getLogger(EXCHANGE_LOGGER) as Logger
    private val silentModuleLogger = LoggerFactory.getLogger(SILENT_MODULE_LOGGER) as Logger
    private val moduleLoggerLines = CapturingAppender()
    private var previousExchangeLevel: Level? = null

    @BeforeEach
    fun setUp() {
        wireMock.resetAll()
        wireMock.stubFor(get(urlEqualTo(FENCE_PATH)).willReturn(okJson("""{"message":"fence"}""").withStatus(FENCE_STATUS)))
        // The runner does not initialise Boot's LoggingSystem, so the reference `logging.level` block does not
        // apply here; the level is set by hand to the value that block sets (INFO = every exchange).
        previousExchangeLevel = exchangeLogger.level
        exchangeLogger.level = Level.INFO
        silentModuleLogger.addAppender(moduleLoggerLines.apply { start() })
    }

    @AfterEach
    fun tearDown() {
        silentModuleLogger.detachAppender(moduleLoggerLines)
        moduleLoggerLines.stop()
        exchangeLogger.level = previousExchangeLevel
        // Invariant: nothing in this class writes to the module's own exchange logger name.
        assertThat(moduleLoggerLines.events).`as`("the module's own HttpExchange logger must stay silent").isEmpty()
    }

    @Nested
    inner class `One event per exchange` {
        @Test
        fun `should write exactly one event on the library's logger with the backend name and the template`() {
            // What is tested: the baseline of the whole class - one GET through a client of the factory, in a
            //   host-shaped context, produces the library's exchange event.
            // Success criteria: the call wrote one event, counted once a later call's event is in (a fence); it is
            //   on `adapter-http-exchange`, INFO, `success` with status 200, its
            //   message names the backend in place of the URL, and it carries `adapter_name` = the backend and
            //   `adapter_url_template` = the mesh endpoint's template.
            // Why it matters: those two fields are what a log query groups by and what joins the event to this
            //   module's transport lines; both reach the library only through request attributes this module
            //   sets, so a lost attribute would leave every event of every backend indistinguishable.
            //
            // Given: a host context and a 200 answer
            wireMock.stubFor(get(urlEqualTo("/things/42")).willReturn(okJson("""{"message":"hi"}""")))
            hostContext("things").run { context ->
                capturing(backend = "things") { appender ->
                    // When: one GET through a client of the factory
                    context.client("things").get(Greeting::class.java) { it.uri("/things/42") }.block()

                    // Then: one event, on the library's logger, INFO / success, naming the backend and the
                    // template - the two fields a log query groups by
                    val event = appender.fencedEvents(context, "things").single()
                    assertThat(event.loggerName).isEqualTo(EXCHANGE_LOGGER)
                    assertThat(event.level).isEqualTo(Level.INFO)
                    assertThat(event.formattedMessage).startsWith("Adapter http exchange GET ").contains("things -> 200")
                    assertThat(event.field("adapter_outcome")).isEqualTo("success")
                    assertThat(event.field("adapter_response_status_code")).isEqualTo(200)
                    assertThat(event.field("adapter_name")).isEqualTo("things")
                    assertThat(event.field("adapter_url_template")).isEqualTo("/things/{id}")
                }
            }
        }

        @Test
        fun `should write no event for a request that never became an exchange`() {
            // What is tested: a serialisation failure produces NO exchange event, because it never reached the
            //   WebClient - and therefore never the library's filter.
            // Success criteria: two calls, one failing while serialising, one real; exactly one event, the
            //   second. The successful call is the ordering fence: awaiting ITS event means anything the first
            //   call was going to emit has had its turn.
            // Why it matters: the contract is one event per HTTP EXCHANGE; a phantom exchange in a log whose
            //   consumers count real calls would be a regression the library cannot see - only this module can
            //   move the serialisation into the exchange, so only this module can pin that it does not.
            //
            // Given
            wireMock.stubFor(post(urlEqualTo("/things/42")).willReturn(okJson("""{"message":"ok"}""")))
            hostContext("phantom").run { context ->
                capturing(backend = "phantom") { appender ->
                    val client = context.client("phantom")

                    // When: a call that fails during serialisation, then a well-formed one
                    runCatching { client.post(Greeting::class.java, Unserialisable()) { it.uri("/things/42") }.block() }
                    client
                        .post(Greeting::class.java, LookupRequest("fence")) {
                            it.uri("/things/42").contentType(MediaType.APPLICATION_JSON)
                        }.block()

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
            // What is tested: the reference `adapter-logging` header configuration - every header logged on both
            //   directions, X-Access-Token masked by name, every other header - Host included - in plaintext.
            // Success criteria: the rendered request headers name the Host with its value and the token
            //   header with a `<length>:<16 hex>` fingerprint, and the secret itself appears nowhere on the event.
            // Why it matters: the Host header is the second view of the backend next to adapter_name, and the
            //   token is the one value that must never reach the log - the reference configuration masks it by
            //   name. An operator reads both.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/things/1")).willReturn(okJson("""{"message":"hi"}""")))
            hostContext("masked").run { context ->
                capturing(backend = "masked") { appender ->
                    // When
                    context.client("masked").get(Greeting::class.java) { it.uri("/things/1") }.block()

                    // Then
                    val event = appender.fencedEvents(context, "masked").single()
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
            // What is tested: the library's semantics for a 4xx - the answer is `rejected`
            //   (the peer answered, and refused), carried by its status code, with its error body logged.
            // Success criteria: the single event of the backend is INFO, `adapter_outcome=rejected`, status 404,
            //   and its response body contains the backend's error text.
            // Why it matters: a query on `adapter_outcome == "failure"` does not find a resolver miss - it is a
            //   query on the status code. Pinned so nobody reads the vocabulary as a bug.
            //
            // Given: a 404 with a JSON error body
            wireMock.stubFor(
                get(urlEqualTo("/things/404")).willReturn(aResponse().withStatus(404).withBody("""{"error":"no such thing"}""")),
            )
            hostContext("benign").run { context ->
                capturing(backend = "benign") { appender ->
                    // When
                    runCatching { context.client("benign").get(Greeting::class.java) { it.uri("/things/404") }.block() }

                    // Then
                    val event = appender.fencedEvents(context, "benign").single()
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
            //   `rejected` on the outcome, but at WARN: these four are about the application's own standing with
            //   the peer, which only an operator can resolve. The 429 stands for the set; the set itself is
            //   pinned in the library (`ClassificationTest`).
            // Success criteria: the single event is WARN with `adapter_outcome=rejected` and status 429.
            // Why it matters: an operator's query for the operational 4xx (408/429) and the module's own
            //   breaker policy (`HttpExchangeErrorPolicy`, which counts 408/429 as failures and the other 4xx as
            //   successes) both read the STATUS, not the outcome - the outcome does not separate a 429 from a 404.
            //   This test pins that the level does, so `logging.level.adapter-http-exchange: WARN` keeps the 429s.
            //
            // Given: a throttling answer
            wireMock.stubFor(
                get(urlEqualTo("/things/throttled")).willReturn(aResponse().withStatus(429).withBody("""{"error":"slow down"}""")),
            )
            hostContext("throttled").run { context ->
                capturing(backend = "throttled") { appender ->
                    // When
                    runCatching { context.client("throttled").get(Greeting::class.java) { it.uri("/things/throttled") }.block() }

                    // Then
                    val event = appender.awaitEvents().single()
                    assertThat(event.level).isEqualTo(Level.WARN)
                    assertThat(event.field("adapter_outcome")).isEqualTo("rejected")
                    assertThat(event.field("adapter_response_status_code")).isEqualTo(429)
                }
            }
        }

        @Test
        fun `should log a 500 as WARN failure with the error body, and a 200 as an INFO success`() {
            // What is tested: the two ends of the outcome scale on one backend - a 5xx and a 2xx.
            // Success criteria: the 500's event is WARN `failure` and carries the error body; the 200's event is
            //   INFO and carries its body as well, because the reference configuration logs bodies `always`.
            // Why it matters: WARN on the exchange stream is what operators alert on; a 5xx below it would hide a
            //   failing backend, a 2xx above it would bury the failures. The body half pins what the reference
            //   configuration's `always` means, so a change to `on-failure` is a visible decision.
            //
            // Given: a failing and a succeeding endpoint on one backend
            wireMock.stubFor(get(urlEqualTo("/things/500")).willReturn(aResponse().withStatus(500).withBody("""{"error":"boom"}""")))
            wireMock.stubFor(get(urlEqualTo("/things/200")).willReturn(okJson("""{"message":"fine"}""")))
            hostContext("server").run { context ->
                capturing(backend = "server") { appender ->
                    val client = context.client("server")

                    // When
                    runCatching { client.get(Greeting::class.java) { it.uri("/things/500") }.block() }
                    client.get(Greeting::class.java) { it.uri("/things/200") }.block()

                    // Then: the 5xx is a WARN failure carrying its body; the 200 an INFO success - with its body
                    // too, since the reference configuration logs bodies `always`
                    val events = appender.awaitEvents(count = 2)
                    val failure = events.single { it.field("adapter_response_status_code") == 500 }
                    assertThat(failure.level).isEqualTo(Level.WARN)
                    assertThat(failure.field("adapter_outcome")).isEqualTo("failure")
                    assertThat(failure.field("adapter_response_body") as String?).contains("boom")
                    val success = events.single { it.field("adapter_response_status_code") == 200 }
                    assertThat(success.level).isEqualTo(Level.INFO)
                    assertThat(success.field("adapter_response_body")).isNotNull()
                }
            }
        }

        @Test
        fun `should log a caller's cancel as WARN cancelled`() {
            // What is tested: the library's half of the cancellation trade - a subscriber that walks away before
            //   the answer arrives.
            // Success criteria: the backend's one event is WARN `cancelled` and has no status code (nothing was
            //   received).
            // Why it matters: this module logs its own cancellation line at DEBUG on purpose
            //   (ResilientWebClientIntegrationTest), which is only safe because THIS event carries the fact at
            //   WARN. Were it lower, a flood of abandoned calls would be invisible at the operator's level.
            //
            // Given: a backend that does not answer within the window
            wireMock.stubFor(get(urlEqualTo("/things/never")).willReturn(okJson("{}").withFixedDelay(30_000)))
            hostContext("walked-away").run { context ->
                capturing(backend = "walked-away") { appender ->
                    // When: the subscriber walks away before the answer arrives
                    context
                        .client("walked-away")
                        .get(Greeting::class.java) { it.uri("/things/never") }
                        .subscribe()
                        .dispose()

                    // Then: the library reports the cancel at WARN, with no status (nothing was received)
                    val event = appender.fencedEvents(context, "walked-away").single()
                    assertThat(event.level).isEqualTo(Level.WARN)
                    assertThat(event.field("adapter_outcome")).isEqualTo("cancelled")
                    assertThat(event.field("adapter_response_status_code")).isNull()
                }
            }
        }

        @Test
        fun `should log a TimeLimiter timeout as cancelled, not as timeout`() {
            // What is tested: the resilience budget expiring. The R4j TimeLimiter sits OUTSIDE the WebClient
            //   call and cancels it, so the library sees a cancel - `cancelled`, not `timeout` (which the
            //   library reserves for the connector's own timeouts).
            // Success criteria: with a 200 ms budget against a 2 s backend, the backend's one event is WARN
            //   `cancelled`.
            // Why it matters: an alert on `adapter_outcome == "timeout"` never fires for the budget that governs
            //   every call here. The resilience verdict itself stays
            //   on ResilientWebClient's transport line.
            //
            // Given: a 200 ms budget against a 2 s backend
            wireMock.stubFor(get(urlEqualTo("/things/slow")).willReturn(okJson("{}").withFixedDelay(2_000)))
            hostContext("budget", timeLimiter = Duration.ofMillis(200)).run { context ->
                capturing(backend = "budget") { appender ->
                    // When
                    runCatching { context.client("budget").get(Greeting::class.java) { it.uri("/things/slow") }.block() }

                    // Then
                    val event = appender.fencedEvents(context, "budget").single()
                    assertThat(event.level).isEqualTo(Level.WARN)
                    assertThat(event.field("adapter_outcome")).isEqualTo("cancelled")
                }
            }
        }
    }

    @Nested
    inner class `Body capture` {
        @Test
        fun `should cap a logged body at max-body-bytes and note the total`() {
            // What is tested: the reference configuration's 16 KB cap - the LOGGED body is truncated with the
            //   library's note, the wire body is untouched.
            // Success criteria: a 20 000-character request body is logged truncated, with the "N bytes total"
            //   note, in less than the cap plus the note; and WireMock received the whole value.
            // Why it matters: the cap bounds memory per captured exchange and the size of every log line; a cap
            //   that also truncated the WIRE body would corrupt requests, and one that did not apply would put
            //   whole customer payloads into the log index.
            //
            // Given: a POST whose body is well past the cap, answered with a 500 (logged under `always` and
            // `on-failure` alike)
            val longValue = "X".repeat(20_000)
            wireMock.stubFor(post(urlEqualTo("/things/42")).willReturn(aResponse().withStatus(500).withBody("{}")))
            hostContext("capped").run { context ->
                val cap = context.getBean(ClientLoggingProperties::class.java).maxBodyBytes
                capturing(backend = "capped") { appender ->
                    // When
                    runCatching {
                        context
                            .client("capped")
                            .post(Greeting::class.java, LookupRequest(longValue)) {
                                it.uri("/things/42").contentType(MediaType.APPLICATION_JSON)
                            }.block()
                    }

                    // Then: the logged body is bounded by the cap plus the note, and says how much there was
                    val logged = appender.awaitEvents().last().field("adapter_request_body") as String
                    assertThat(logged).contains("truncated").containsPattern("\\d+ bytes total")
                    assertThat(logged.length).isLessThan(cap + 64)
                    // ... while the whole body reached the backend
                    wireMock.verify(postRequestedFor(urlEqualTo("/things/42")).withRequestBody(containing(longValue)))
                }
            }
        }
    }

    @Nested
    inner class `The reference configuration` {
        @Test
        fun `should bind the adapter-logging block of the reference application yml as written`() {
            // What is tested: that the reference configuration's `adapter-logging` block reaches the library's
            //   ClientLoggingProperties in a host-shaped context.
            // Success criteria: the bound properties carry the reference values - enabled, the default logger
            //   name, every header selected on both directions with an empty plaintext list, bodies `always`, the
            //   16 KB cap.
            // Why it matters: every other case of this class reads its expectations off these values; a block
            //   that silently failed to bind would leave them running on the library's defaults (bodies `never`)
            //   while still passing wherever the defaults happen to agree.
            //
            // Given / When: the context bound the reference file
            hostContext("config").run { context ->
                val properties = context.getBean(ClientLoggingProperties::class.java)

                // Then: the values the other cases rely on
                assertThat(properties.enabled).isTrue()
                assertThat(properties.loggerName).isEqualTo(EXCHANGE_LOGGER)
                assertThat(properties.requestHeaders.includes).containsExactly("*")
                assertThat(properties.requestHeaders.unmasked).isEmpty()
                assertThat(properties.responseHeaders.includes).containsExactly("*")
                assertThat(properties.logRequestBody).isEqualTo(BodyLogMode.ALWAYS)
                assertThat(properties.logResponseBody).isEqualTo(BodyLogMode.ALWAYS)
                assertThat(properties.maxBodyBytes).isEqualTo(16_384)
            }
        }

        @Test
        fun `should bind the app-webclient block of application yml instead of falling back to code defaults`() {
            // What is tested: that `app.webclient.*` of the reference application.yml actually REACHES
            //   WebClientProperties in a host-shaped context - binding from the Environment through the module's own
            //   @EnableConfigurationProperties, not through an instance this test constructs.
            // Success criteria: every value the reference file sets is the BOUND value, and
            //   `observability.http-observation-enabled` is asserted against the code default in the same test -
            //   a key whose reference value differs from it, so it can tell a real binding apart from a silent
            //   fallback. (The two validation keys differ from their code defaults as well - off and fail - so
            //   their assertions below carry the same evidence.)
            // Why it matters: the toggle is opt-in and defaults to false. Every way it can fail to arrive - a
            //   property source that never carries it, a properties bean handed in ready-made, a shadowed
            //   application.yml - ends in the SAME observable state as "the operator did not switch it on", with
            //   no error anywhere. Only an assertion on the configured-vs-default difference distinguishes them.
            //
            // Given / When: the context bound the reference file
            hostContext("webclient-config").run { context ->
                val properties = context.getBean(WebClientProperties::class.java)

                // Then: the toggle, whose reference value contradicts the code default
                assertThat(properties.observability.httpObservationEnabled)
                    .`as`("app.webclient.observability.http-observation-enabled, bound from the reference application.yml")
                    .isTrue()
                assertThat(WebClientProperties().observability.httpObservationEnabled)
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
     * By default this context is wired OBSERVED BUT NOT TRACED, and that is a deliberate position rather than
     * an omission: `micrometer-tracing` and `micrometer-tracing-bridge-otel` are on the classpath, but no
     * tracing auto-configuration is registered here, so the context holds no `io.micrometer.tracing.Tracer`
     * bean - which is exactly the bean legatium looks up to choose between its two "observed" verdicts. Wiring
     * a real OTel tracer instead would reach for `GlobalOpenTelemetry`, a JVM-wide singleton that may be set
     * once per process - the same class of shared-state trap as `Metrics.globalRegistry`
     * (see `hostContext`), and one that would surface in whichever test class happened to run next.
     *
     * The mode is worth pinning precisely BECAUSE it is the untraced one: it is the mode in which legatium
     * generates the request id itself and puts it on the wire as its correlation header.
     *
     * **The traced verdict, without a tracing stack.** The second verdict is reached here with the smallest
     * thing that is a `Tracer` AT ALL - [Tracer.NOOP], handed to `hostContext(tracer = ...)`. It is enough
     * because the verdict is a BEAN-TYPE lookup, it invents no behaviour, and it touches no JVM-wide state. It
     * also makes the honest point that the log line alone cannot make: a Tracer BEAN traces nothing by itself -
     * spans and a `traceparent` come from the observation HANDLERS an auto-configuration would register, and
     * this context has none. The traced mode ON THE WIRE - trace id as request id, no correlation header - is
     * NOT covered in this repository: it needs a full tracing stack (a tracer bridge and an exporter), which no
     * test context here carries.
     */
    @Nested
    inner class `The observation wiring` {
        @Test
        fun `should report the toggle as plain on, with the library agreeing that the observation is wired`() {
            // What is tested: that the reference `http-observation-enabled: true` reaches a context which can ACT
            //   on it, and that the two independent verdicts written at startup now tell the same story.
            // Success criteria: the module's observation report writes the unqualified "on" (a recording
            //   registry stayed on the builder), and legatium - which decides by BEAN lookup on
            //   ObservationWebClientCustomizer - reports that it found Boot's client observation.
            // Why it matters: these two lines are read together when a client span is missing, and they answer
            //   different questions - the report's other line reports `management.observations.enable.*`, which
            //   are PROPERTIES defaulting to true when unset, while legatium reports BEANS. In a context without
            //   the observation wired they read as a contradiction ("the central filter is on" next to "found no
            //   client observation") while both are true. Pinning them as a PAIR is what keeps that pair honest: neither line may claim a wiring the context does not have.
            //
            // Given: recorders on both loggers, in place before the context refreshes
            val reportLogger = LoggerFactory.getLogger(WebClientObservationReport::class.java) as Logger
            val libraryLogger = LoggerFactory.getLogger(ClientLoggingAutoConfiguration::class.java) as Logger
            recording(reportLogger) { reportLines ->
                recording(libraryLogger) { libraryLines ->
                    // When
                    hostContext("observation-toggle").run { context ->
                        // The premise, asserted rather than assumed: a registry exists AND records - Micrometer
                        // counts a handler-less registry as noop, so presence alone would not be enough.
                        assertThat(context).hasSingleBean(ObservationRegistry::class.java)
                        assertThat(context.getBean(ObservationRegistry::class.java).isNoop)
                            .`as`("the context's ObservationRegistry must have a handler, else it records nothing")
                            .isFalse()
                    }

                    // Then: the report's verdict - configured AND effective
                    assertThat(reportLines.lineContaining("http-observation-enabled"))
                        .contains("app.webclient.observability.http-observation-enabled=true")
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
            // What is tested: the toggle END TO END - not that a registry is wired, but that a real call through
            //   a client of this factory actually OPENS the `http.client.requests` observation and that it lands
            //   in the host's MeterRegistry.
            // Success criteria: after one GET, a timer by that name exists with count 1, and its `uri` tag
            //   is the mesh endpoint's low-cardinality TEMPLATE rather than the expanded path.
            // Why it matters: everything else about this toggle can be right while nothing is recorded - the
            //   property binds, the registry exists, the customizer runs, and the observation still opens
            //   nowhere. Only a recorded meter proves it. The tag matters just as much: ResilientWebClient
            //   stamps the template onto every request (URI_TEMPLATE_ATTRIBUTE) precisely so Spring's client
            //   observation records it here; unstamped, each path variable would mint its own time series.
            //
            // Given: a backend and a context with the observation fully wired
            wireMock.stubFor(get(urlEqualTo("/things/7")).willReturn(okJson("""{"message":"hi"}""")))
            hostContext("observed").run { context ->
                val meters = context.getBean(SimpleMeterRegistry::class.java)
                assertThat(meters.find(HTTP_CLIENT_OBSERVATION).timer())
                    .`as`("nothing may be recorded before the call")
                    .isNull()

                // When: one real call through a client of the factory
                context.client("observed").get(Greeting::class.java) { it.uri("/things/7") }.block()

                // Then: the observation ran and was recorded, under the path TEMPLATE
                val timer = meters.find(HTTP_CLIENT_OBSERVATION).timer()
                assertThat(timer).`as`("the '%s' observation must have been recorded", HTTP_CLIENT_OBSERVATION).isNotNull()
                assertThat(timer!!.count()).isEqualTo(1L)
                assertThat(timer.id.getTag("uri")).isEqualTo("/things/{id}")
            }
        }

        @Test
        fun `should generate its own correlation header because the observation is wired without tracing`() {
            // What is tested: the OBSERVABLE consequence of legatium's "observed, not traced" verdict. The
            //   verdict itself is a log line; what it promises is a header choice, and that reaches the backend.
            // Success criteria: no `Tracer` bean in the context (the premise), and the request WireMock
            //   received carries the library's configured correlation header with a generated value and NO
            //   `traceparent` - which is what "the module generates the request id and sends <header> on every
            //   call that carries no traceparent" means on the wire.
            // Why it matters: the two modes differ in what downstream sees.
            //   A host that adds a tracing bridge flips this silently - correlation header gone, traceparent
            //   instead - and any backend or log query keyed on the correlation header would break with no test
            //   to catch it. The header name is read from the library's own properties rather than restated, so
            //   a change to `adapter-logging.correlation-id-header` cannot make this pass vacuously.
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
                context.client("untraced").get(Greeting::class.java) { it.uri("/things/9") }.block()

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
            //   `Tracer.NOOP` bean is enough to reach it; nothing else about the context changes.
            // Success criteria: with that one bean added, the same line that read "but no Micrometer
            //   Tracing" in the without-tracing case (its own correlation header) now reads "with Micrometer
            //   Tracing wired", names the correlation header as the one that is NOT generated - and the very same
            //   call still goes out with that header and without a traceparent.
            // Why it matters: the line is a statement about WIRING, decided once at startup by a bean lookup;
            //   the header on the wire is decided PER REQUEST from the outgoing traceparent
            //   (`ClientIdentity.resolve`). They agree in production because the tracing auto-configuration
            //   that supplies the Tracer also registers the observation HANDLERS that inject the traceparent -
            //   a Tracer bean alone registers none, and then the promise the line makes is not kept. Anyone
            //   debugging a missing trace id by reading that line needs this pinned: the line proves a bean
            //   exists, never that a span was started.
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

                    context.client("traced").get(Greeting::class.java) { it.uri("/things/11") }.block()
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
     * Does a value the CALLER put in its coroutine's MDC reach the event that the library writes on an
     * event-loop thread?
     *
     * The library's route (legatium ADR-0010): it holds the Reactor Context of the subscription and restores from it whatever the
     * `ThreadLocalAccessor`s registered with Micrometer's context propagation know how to restore. A coroutine
     * `MDCContext` is a coroutine element, not a Reactor Context entry - so the join depends on an accessor for
     * the key, which is what these two cases establish: the gap without one, the recipe with one.
     */
    @Nested
    inner class `The caller's MDC` {
        private val callerThread = Executors.newSingleThreadExecutor { r -> Thread(r, "mdc-caller-thread") }

        @AfterEach
        fun stopCallerThread() {
            callerThread.shutdownNow()
        }

        @Test
        fun `should NOT carry a coroutine MDCContext value onto the event when no accessor is registered`() {
            // What is tested: the gap half - a coroutine MDCContext key and no ThreadLocalAccessor for it.
            // Success criteria: the event is written on a thread other than the caller's and does NOT carry the
            //   key.
            // Why it matters: it documents what a host gets by default, so the missing correlation id on the
            //   exchange event is a known gap with a known fix (the next case) rather than a surprise. Unlike
            //   this module's own transport lines, which snapshot the caller's MDC themselves
            //   (ResilientWebClientIntegrationTest), the library restores only what an accessor knows.
            //
            // Given: a caller carrying a correlation id in its coroutine MDC, and no accessor for the key
            wireMock.stubFor(get(urlEqualTo("/things/mdc")).willReturn(okJson("""{"message":"hi"}""")))
            hostContext("mdc-gap").run { context ->
                capturing(backend = "mdc-gap") { appender ->
                    // When: the call runs on a dedicated thread, under MDCContext
                    runBlocking(callerThread.asCoroutineDispatcher() + MDCContext(mapOf(CORRELATION_KEY to CORRELATION_VALUE))) {
                        context.client("mdc-gap").get(Greeting::class.java) { it.uri("/things/mdc") }.awaitSingle()
                    }

                    // Then: the event is written elsewhere, and the caller's key is not on it - the documented
                    // gap a host closes by registering an accessor (next case)
                    val event = appender.awaitEvents().last()
                    assertThat(event.threadName).doesNotStartWith("mdc-caller-thread")
                    assertThat(event.mdcPropertyMap).doesNotContainKey(CORRELATION_KEY)
                }
            }
        }

        @Test
        fun `should carry a coroutine MDCContext value onto the event once an accessor for the key is registered`() {
            // What is tested: the recipe. With a ThreadLocalAccessor for the key registered at Micrometer's
            //   ContextRegistry, ResilientWebClient's `contextCapture()` lifts the caller's MDC value into the
            //   Reactor Context at subscription, and the library's restorer puts it back around its emission.
            // Success criteria: the event, written on a thread other than the caller's, carries the caller's
            //   key and value.
            // Why it matters: this is what a host has to do for CORRELATION_ID to be on the exchange
            //   event - two steps, both named by this test, neither of them a change in this module.
            //
            // Given: an accessor for the key, registered for the duration of the test (the registry is JVM-wide)
            val registry = ContextRegistry.getInstance()
            registry.registerThreadLocalAccessor(MdcKeyAccessor(CORRELATION_KEY))
            try {
                wireMock.stubFor(get(urlEqualTo("/things/mdc")).willReturn(okJson("""{"message":"hi"}""")))
                hostContext("mdc-join").run { context ->
                    capturing(backend = "mdc-join") { appender ->
                        // When
                        runBlocking(callerThread.asCoroutineDispatcher() + MDCContext(mapOf(CORRELATION_KEY to CORRELATION_VALUE))) {
                            context.client("mdc-join").get(Greeting::class.java) { it.uri("/things/mdc") }.awaitSingle()
                        }

                        // Then: the event, written on another thread, carries the caller's key
                        val event = appender.awaitEvents().last()
                        assertThat(event.threadName).doesNotStartWith("mdc-caller-thread")
                        assertThat(event.mdcPropertyMap).containsEntry(CORRELATION_KEY, CORRELATION_VALUE)
                    }
                }
            } finally {
                registry.removeThreadLocalAccessor(CORRELATION_KEY)
            }
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /**
     * A context the way the host assembles it: Boot's WebClient auto-configuration (the builder bean and its
     * customizers), a fully wired OBSERVATION side (a recording `ObservationRegistry` plus Boot's
     * `ObservationWebClientCustomizer`, which is what puts the registry on the builder), the library's
     * auto-configuration (its filter as a customizer), the reference `application.yml` for the `adapter-logging`
     * and `app.webclient` blocks, and the factory with its collaborators. One mesh endpoint, `GET /things/{id}`
     * against WireMock, carrying the token header so the masking can be observed.
     *
     * **Why the registry is hand-built and Boot's `MetricsAutoConfiguration` is NOT used.** It would supply the
     * `DefaultMeterObservationHandler` for free, but it also brings `MeterRegistryPostProcessor`, which calls
     * `Metrics.addRegistry(...)` - every context would publish its registry into the JVM-GLOBAL
     * `Metrics.globalRegistry` and never take it out again. Reactor Netty's `.metrics(true)` (both the shared
     * pool and the per-request transport) publishes there too, so after a few dozen contexts it is writing into
     * an accumulation of closed registries and calls start failing with `Connection prematurely closed BEFORE
     * response` - in OTHER test classes, long after this one finished.
     *
     * The handler is what makes the registry live at all: Micrometer counts a handler-less registry as noop
     * (`SimpleObservationRegistry.isNoop()` ORs in `getObservationHandlers().isEmpty()`), and a noop registry
     * records nothing however well the rest is wired.
     *
     * [tracer] is the one knob that switches legatium between its two "observed" verdicts. The library decides
     * by BEAN LOOKUP on the type `io.micrometer.tracing.Tracer` (`ClientObservationWiring.describe`), so any
     * bean of that type is enough; contexts that pass nothing stay in the untraced mode this class otherwise
     * pins. It registers no observation HANDLER - see the nested class KDoc for why that distinction is the
     * point rather than a shortcut.
     */
    private fun hostContext(
        backend: String,
        timeLimiter: Duration = Duration.ofSeconds(30),
        tracer: Tracer? = null,
    ): ApplicationContextRunner {
        // One per context, shared between the bean definition and the observation handler below so a test can
        // read back the meter the observation produced. A local rather than a field: each `hostContext(...)`
        // call backs exactly one `run { }`, so nothing carries over between cases.
        val meters = SimpleMeterRegistry()
        return ApplicationContextRunner()
            .withInitializer { context ->
                context.environment.propertySources.addFirst(MapPropertySource(REFERENCE_PROPERTY_SOURCE, ConfigurationFiles.referenceProperties(*REFERENCE_PREFIXES)))
            }.withConfiguration(
                AutoConfigurations.of(
                    WebClientAutoConfiguration::class.java,
                    // @ConditionalOnBean(ObservationRegistry) - it contributes the customizer that places the
                    // registry on Boot's builder, which is the step the module toggle then leaves alone.
                    WebClientObservationAutoConfiguration::class.java,
                    ClientLoggingAutoConfiguration::class.java,
                ),
            ).withBean(SimpleMeterRegistry::class.java, Supplier { meters })
            .withBean(
                ObservationRegistry::class.java,
                Supplier {
                    ObservationRegistry.create().apply {
                        observationConfig().observationHandler(DefaultMeterObservationHandler(meters))
                    }
                },
            )
            // The module's OWN @EnableConfigurationProperties binding, not a hand-made instance - see
            // `BoundWebClientProperties` for why not `VerediumWebClientAutoConfiguration`.
            .withUserConfiguration(BoundWebClientProperties::class.java)
            // The startup report on the observation toggle, as the context would scan it - the toggle-report test
            // ("should report the toggle as plain on ...") reads its line next to legatium's.
            .withBean(WebClientObservationReport::class.java)
            .withBean(
                MeshProperties::class.java,
                Supplier {
                    MeshProperties(
                        defaults = MeshProperties.Defaults(individualRequestTimeoutMs = 1000),
                        endpoints =
                            listOf(
                                MeshProperties.EndpointConfig(
                                    triplet = "/endpoint/localhost/GET/things/{id}",
                                    alias = backend,
                                    baseUrl = wireMock.baseUrl(),
                                    headers = mapOf(TOKEN_HEADER to TOKEN_VALUE),
                                ),
                            ),
                    )
                },
            ).withBean(
                ReactiveResilienceDecorator::class.java,
                Supplier {
                    ReactiveResilienceDecorator(
                        CircuitBreakerRegistry.ofDefaults(),
                        TimeLimiterRegistry.of(TimeLimiterConfig.custom().timeoutDuration(timeLimiter).build()),
                        BulkheadRegistry.ofDefaults(),
                    )
                },
            ).withBean(ObjectMapper::class.java, Supplier { JsonMapper.builder().build() })
            .withBean(ResilientWebClientFactory::class.java)
            .let { runner -> if (tracer == null) runner else runner.withBean(Tracer::class.java, Supplier { tracer }) }
    }

    /**
     * Attaches a capturing appender to the exchange logger for the duration of [block], capturing only the events
     * of [backend] - the `adapter_name` of the case's own context. The exchange logger is process-wide, so a late
     * event of an earlier case (a cancelled exchange completing on its event loop after that case returned)
     * would otherwise count towards this one and release the wait before this case's own event arrived. The
     * library emits on the thread that completes the body, after `block()` may already have returned, so a case
     * reads its events through [CapturingAppender.awaitEvents].
     */
    private fun capturing(
        backend: String,
        block: (CapturingAppender) -> Unit,
    ) = capturing(exchangeLogger, accept = { event -> event.field("adapter_name") == backend }, block = block)

    private fun AssertableApplicationContext.client(backend: String): ResilientWebClient = getBean(ResilientWebClientFactory::class.java).create(backend)

    /**
     * The events [backend]'s calls so far wrote, once a FENCE has passed: one more call of the backend, made after
     * them and answered [FENCE_STATUS], whose own event is awaited and then left out. The library writes an event
     * when its exchange ends, possibly after `block()` returned, so the event of the case's call alone could be
     * awaited but never shown to be the ONLY one - a second, later event would still be on its way. By the time the
     * later exchange's event is in, anything the case's call was going to write has had its turn.
     */
    private fun CapturingAppender.fencedEvents(
        context: AssertableApplicationContext,
        backend: String,
    ): List<ILoggingEvent> {
        context.client(backend).get(Greeting::class.java) { it.uri(FENCE_PATH) }.block()
        return awaitEvents { it.field("adapter_response_status_code") == FENCE_STATUS }
            .filterNot { it.field("adapter_response_status_code") == FENCE_STATUS }
    }

    /**
     * Runs [block] with a recorder attached to [logger], and returns whatever it returns.
     *
     * For lines written while the context REFRESHES - the toggle lines of [WebClientObservationReport], the
     * library's wiring verdict - the recorder has to be in place around `run(...)` rather than inside the context consumer, where
     * they have already been written. [CapturingAppender.events] is read without awaiting for the same reason:
     * the refresh happens on this thread, so by the time `run(...)` returns the lines are already captured.
     */
    private fun <T> recording(
        logger: Logger,
        block: (CapturingAppender) -> T,
    ): T =
        // The wiring verdicts are DEBUG lines (legatium's report, ADR-0014; the observation report's are INFO) and
        // logback-test.xml roots at INFO, so the logger is opened to DEBUG for the recording and restored after -
        // the same hand-set level the exchange logger gets in setUp, for the same reason: the runner does not
        // initialise Boot's LoggingSystem.
        capturing(logger, Level.DEBUG, block = block)

    /** The one recorded line containing [needle], as its rendered message. */
    private fun CapturingAppender.lineContaining(needle: String): String = events.map { it.formattedMessage }.single { it.contains(needle) }

    /** A ThreadLocalAccessor over one SLF4J MDC key - the shape of accessor a host registers per key. */
    private class MdcKeyAccessor(
        private val mdcKey: String,
    ) : ThreadLocalAccessor<String> {
        override fun key(): Any = mdcKey

        override fun getValue(): String? = MDC.get(mdcKey)

        override fun setValue(value: String) = MDC.put(mdcKey, value)

        override fun setValue() = MDC.remove(mdcKey)
    }
}
