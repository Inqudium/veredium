package eu.inqudium.veredium.restclient

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
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
import com.github.tomakehurst.wiremock.core.Options
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import eu.inqudium.legatium.restclient.logging.ClientRequestLoggingInterceptor
import eu.inqudium.veredium.core.Veredium
import eu.inqudium.veredium.core.client.EmptyResponseBodyException
import eu.inqudium.veredium.core.client.ResponseBodyMetrics
import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.resilience.BlockingResilienceDecorator
import eu.inqudium.veredium.core.validation.ResponseValidationProperties
import eu.inqudium.veredium.testsupport.CapturingAppender
import eu.inqudium.veredium.testsupport.CustomizedRegistries
import eu.inqudium.veredium.testsupport.Greeting
import eu.inqudium.veredium.testsupport.LookupRequest
import eu.inqudium.veredium.testsupport.Unserialisable
import eu.inqudium.veredium.testsupport.WireMockArrivals
import eu.inqudium.veredium.testsupport.capturing
import eu.inqudium.veredium.testsupport.eventually
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
import jakarta.validation.ConstraintViolationException
import jakarta.validation.Validation
import jakarta.validation.ValidatorFactory
import jakarta.validation.constraints.Size
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
import org.springframework.http.MediaType
import org.springframework.util.unit.DataSize
import org.springframework.web.client.HttpClientErrorException
import org.springframework.web.client.HttpServerErrorException
import org.springframework.web.client.ResourceAccessException
import org.springframework.web.client.RestClient
import org.springframework.web.client.RestClientResponseException
import tools.jackson.core.JacksonException
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.json.JsonMapper
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.concurrent.CancellationException
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/**
 * End-to-end tests for the per-call pipeline of [ResilientRestClient] and the assembly/caching in
 * [ResilientRestClientFactory], driven against a real (WireMock) HTTP backend over the loopback transport.
 *
 * These cover the behaviour that only emerges when the whole chain runs together: response deserialisation,
 * request-body serialisation happening exactly once and reaching the wire, error surfacing from the exchange,
 * the bounded body read, and the three Resilience4j primitives (time limiter, circuit breaker, bulkhead)
 * decorating the call across the decorator's thread hand-off. The Resilience4j registries are built
 * programmatically per test so each scenario configures its own budget and window without an `application.yml`.
 *
 * The client uses a Jackson 3 [ObjectMapper] without the Kotlin module on the classpath, so the test
 * payload types ([Greeting], [LookupRequest]) are written to (de)serialise via the no-arg constructor and
 * bean accessors rather than a Kotlin primary constructor.
 */
class ResilientRestClientIntegrationTest {
    companion object {
        /** Message prefixes the log assertions key on, including the terminal line a capture waits for. */
        private const val ERROR_PREFIX = "Error during request"

        /**
         * The client's closing line, written in `execute`'s `finally` - the LAST one any call produces, whatever
         * its outcome, and hence the terminal marker whenever the client logger is capturing at DEBUG.
         */
        private const val COMPLETED_PREFIX = "Completed request"

        /** The factory's drain line: marker within the message, and the prefix it starts with. */
        private const val DRAIN_MARKER = "did not drain"
        private const val DRAIN_PREFIX = "HTTP engine"

        /**
         * The bound on a wait for a state the transport only exposes as a reading - a gauge, a permit count -
         * where there is no event to await; a fixed sleep would either flake or waste the time.
         */
        private val AWAIT: Duration = Duration.ofSeconds(5)

        private lateinit var wireMock: WireMockServer

        /** What a request to a URL triggers once it reached WireMock, per test - the class's one listener on the server. */
        private val arrivals = WireMockArrivals()

        @JvmStatic
        @BeforeAll
        fun startWireMock() {
            // Dynamic port keeps this test independent of any other WireMock instance, so it can run in the
            // default build alongside everything else. Responses carry a Content-Length rather than being
            // chunked, the shape a JSON API answers in - and the shape the buffer-limit line reads the header from.
            // Asynchronous responses: a delayed answer waits on WireMock's scheduler instead of a container thread,
            // so the 30 s delays the abandoned calls leave behind hold no thread a later case needs.
            wireMock =
                WireMockServer(
                    wireMockConfig()
                        .dynamicPort()
                        .useChunkedTransferEncoding(Options.ChunkedEncodingPolicy.NEVER)
                        .asynchronousResponseEnabled(true)
                        .asynchronousResponseThreads(16),
                ).apply { start() }
                    .also { server -> server.addMockServiceRequestListener(arrivals) }
        }

        @JvmStatic
        @AfterAll
        fun stopWireMock() {
            wireMock.stop()
        }
    }

    private val objectMapper: ObjectMapper = JsonMapper.builder().build()

    /** Factories built during a test; their shared connection pool is closed in [tearDown]. */
    private val factories = mutableListOf<ResilientRestClientFactory>()

    /** Decorators built during a test; their worker executors are shut down in [tearDown]. */
    private val decorators = mutableListOf<BlockingResilienceDecorator>()

    /** Runs the calls a test needs to interrupt from a thread other than the test's. */
    private val callers: ExecutorService = Executors.newVirtualThreadPerTaskExecutor()

    /** The client's own logger, whose lines the log-line tests capture. */
    private val clientLogger = LoggerFactory.getLogger(ResilientRestClient::class.java) as Logger

    @BeforeEach
    fun resetStubs() {
        wireMock.resetAll()
        arrivals.clear()
    }

    @AfterEach
    fun tearDown() {
        callers.shutdownNow()
        factories.forEach { it.shutdown() }
        factories.clear()
        decorators.forEach { it.shutdown() }
        decorators.clear()
        if (validatorFactoryHolder.isInitialized()) validatorFactoryHolder.value.close()
    }

    @Nested
    inner class `The synchronous exchange` {
        @Test
        fun `should deserialise a successful GET response into the requested type`() {
            // What is tested: the happy path of the whole pipeline - a GET through a factory-built client, the
            //   exchange on the decorator's worker, the bounded read and the deserialisation into the DTO.
            // Success criteria: the decoded value carries the backend's message, and WireMock received the request.
            // Why it matters: every other case in this class varies one step of this path; if the plain path broke,
            //   their failures would point at the wrong step.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/hello")).willReturn(okJson("""{"message":"hi there"}""")))
            val client = newClient("greet")

            // When
            val result = client.get(Greeting::class.java) { it.uri("/hello") }

            // Then
            assertThat(result.message).isEqualTo("hi there")
            wireMock.verify(getRequestedFor(urlEqualTo("/hello")))
        }

        @Test
        fun `should send the Host header derived from the triplet and the library's User-Agent`() {
            // What is tested: the headers the factory puts on every request of a client that an adapter never
            //   sees - the Host header that routes the sidecar to the upstream, and the User-Agent token that
            //   names this library to the peer.
            // Success criteria: WireMock, listening on localhost, receives a request whose Host header names the
            //   triplet's host, not localhost, whose User-Agent is Veredium's token rather than Apache's default
            //   (the bare `veredium` here, since the classes directory carries no manifest version), and which
            //   carries the Accept the clients always send.
            // Why it matters: Apache HttpClient derives a Host header from the request URI on its own;
            //   whether an explicit one survives that is a property of the transport, not of this module's code, and
            //   the sidecar routes on nothing else. A transport change that dropped it would break every backend
            //   with a 404 from the sidecar and no error on this side.
            //
            // Given: an endpoint whose triplet names a foreign host, served by the local WireMock
            wireMock.stubFor(get(urlEqualTo("/hosted")).willReturn(okJson("""{"message":"hi"}""")))
            val meshProperties = meshOf(wireMock.baseUrl(), "api.example.com", "hosted")
            val client = newFactory("hosted", meshProperties = meshProperties).create("hosted")

            // When
            client.get(Greeting::class.java) { it.uri("/hosted") }

            // Then
            wireMock.verify(
                getRequestedFor(urlEqualTo("/hosted"))
                    .withHeader("Host", equalTo("api.example.com"))
                    .withHeader("User-Agent", equalTo(Veredium.userAgent()))
                    .withHeader("Accept", equalTo(MediaType.APPLICATION_JSON_VALUE)),
            )
        }

        @Test
        fun `should send the host's own User-Agent when one is configured`() {
            // What is tested: app.restclient.user-agent - a host naming itself to its peers instead of this library.
            // Success criteria: WireMock receives the configured token verbatim, not the library's.
            // Why it matters: the override exists for access logs read per caller; a value that quietly lost to the
            //   library's token would make the configuration a no-op.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/named")).willReturn(okJson("""{"message":"hi"}""")))
            val client = newClient("named", userAgent = "my-service/2.3")

            // When
            client.get(Greeting::class.java) { it.uri("/named") }

            // Then
            wireMock.verify(getRequestedFor(urlEqualTo("/named")).withHeader("User-Agent", equalTo("my-service/2.3")))
        }

        @Test
        fun `should serialise the POST body once, send it on the wire, and deserialise the response`() {
            // What is tested: that post() serialises the body itself, the JSON reaches the backend verbatim, and the
            //   response is deserialised.
            // Success criteria: the decoded response carries the backend's message, and WireMock received exactly
            //   the JSON object `{"id":"X-100"}`.
            // Why it matters: the production code serialises exactly once and hands the string to the wire -
            //   verifying the received body proves the wire request is correct and was not, e.g., re-serialised into
            //   a JSON string literal by a converter.
            //
            // Given
            wireMock.stubFor(post(urlEqualTo("/lookup")).willReturn(okJson("""{"message":"found"}""")))
            val client = newClient("lookup")

            // When
            val result =
                client.post(Greeting::class.java, LookupRequest("X-100")) {
                    it.uri("/lookup").contentType(MediaType.APPLICATION_JSON)
                }

            // Then
            assertThat(result.message).isEqualTo("found")
            wireMock.verify(
                postRequestedFor(urlEqualTo("/lookup"))
                    .withRequestBody(equalToJson("""{"id":"X-100"}""")),
            )
        }

        @Test
        fun `should fail a POST whose body cannot be serialised before anything reaches the wire`() {
            // What is tested: that a body Jackson cannot serialise fails the call BEFORE an exchange exists.
            // Success criteria: post(...) throws with the Jackson failure as its root cause, and WireMock
            //   received nothing.
            // Why it matters: the serialisation sits outside the exchange on purpose, so legatium's interceptor never
            //   sees the request and no phantom exchange event is written for a call that never went out. In a
            //   blocking contract that is simply an exception out of the method - which is what a caller expects.
            //
            // Given: a body whose accessor blows up during serialisation
            wireMock.stubFor(post(urlEqualTo("/lookup")).willReturn(okJson("""{"message":"found"}""")))
            val client = newClient("unserialisable")

            // When / Then
            assertThatThrownBy { client.post(Greeting::class.java, Unserialisable()) { it.uri("/lookup") } }
                .hasRootCauseInstanceOf(IllegalStateException::class.java)
            wireMock.verify(0, postRequestedFor(urlEqualTo("/lookup")))
        }

        @Test
        fun `should surface a 4xx response as Spring's status exception carrying the status and the body`() {
            // What is tested: the status handling of the exchange function, which replaces `retrieve()`'s default
            //   status handler - a 404 must surface exactly as Spring's own handler would raise it.
            // Success criteria: the exception is `HttpClientErrorException.NotFound`, carries status 404 and the
            //   body, and its message has Spring's shape (status, reason phrase, quoted body).
            // Why it matters: adapters and hosts catch Spring's status-specific types and read the error body off
            //   them; a different type or a missing body would break every caller's error handling silently.
            //
            // Given: a backend that answers "not found" with a body
            wireMock.stubFor(get(urlEqualTo("/missing")).willReturn(aResponse().withStatus(404).withBody("nope")))
            val client = newClient("err")

            // When / Then
            assertThatThrownBy { client.get(Greeting::class.java) { it.uri("/missing") } }
                .isInstanceOf(HttpClientErrorException.NotFound::class.java)
                .isInstanceOfSatisfying(RestClientResponseException::class.java) { ex ->
                    assertThat(ex.statusCode.value()).isEqualTo(404)
                    assertThat(ex.responseBodyAsString).isEqualTo("nope")
                    // Spring's own message shape: status, reason phrase, and the body quoted
                    assertThat(ex.message).startsWith("404 Not Found: \"nope\"")
                }
        }

        @Test
        fun `should return the same cached client instance for repeated create calls`() {
            // What is tested: the factory's per-name cache.
            // Success criteria: two `create` calls for the same backend return the same instance.
            // Why it matters: every client owns an HttpClient over the shared pool; a factory that built a new one
            //   per call would leak transports and register the client's meters again each time.
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
        fun `should stamp the backend name onto every request as the adapter-name attribute`() {
            // What is tested: that a client of the factory carries its backend name in the request attribute
            //   ClientRequestLoggingInterceptor.ADAPTER_NAME_ATTRIBUTE - the key legatium's client logging reads the
            //   adapter name from - on a GET and on a POST alike, without the adapter doing anything for it.
            // Success criteria: an interceptor placed on the builder BEFORE the factory clones it (where a host
            //   customizer would sit) sees the attribute with the backend name on each request it observes. The
            //   interceptor is the only vantage point: an attribute never reaches the wire, so WireMock cannot see
            //   it, and that invisibility is precisely why it is an attribute and not a header.
            // Why it matters: behind the sidecar every request goes to the same host, so the URI no
            //   longer tells backends apart. The attribute is what keeps the backend name on the exchange for an
            //   observer that is not this module - a silent regression here would merge all backends into one.
            //
            // Given: a builder carrying an interceptor that records the attribute, and a client of the factory built on it
            val seen = CopyOnWriteArrayList<Any?>()
            val recording =
                RestClient.builder().requestInterceptor { request, body, execution ->
                    seen += request.attributes[ClientRequestLoggingInterceptor.ADAPTER_NAME_ATTRIBUTE]
                    execution.execute(request, body)
                }
            wireMock.stubFor(get(urlEqualTo("/named")).willReturn(okJson("""{"message":"hi"}""")))
            wireMock.stubFor(post(urlEqualTo("/named")).willReturn(okJson("""{"message":"hi"}""")))
            val client = newFactory("named", restClientBuilder = recording).create("named")

            // When: one GET and one POST
            client.get(Greeting::class.java) { it.uri("/named") }
            client.post(Greeting::class.java, LookupRequest("named")) { it.uri("/named") }

            // Then: both requests carried the backend name under the attribute key
            assertThat(seen).containsExactly("named", "named")
        }

        @Test
        fun `should fail fast when creating a client for an unconfigured target`() {
            // What is tested: `create` for a name no mesh endpoint derives.
            // Success criteria: an IllegalStateException naming the unknown backend.
            // Why it matters: a typo in an adapter's backend name must fail where it is made - at startup, with
            //   the name - not as a confusing 404 or connection error at the first call.
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
    inner class `The observation and the meters` {
        @Test
        fun `should open an http client observation per request when observation is enabled`() {
            // What is tested: the standalone observation toggle - with it ON, the factory keeps the (recording)
            //   ObservationRegistry that arrived on the injected builder, so each outbound call opens Spring
            //   RestClient's native `http.client.requests` observation. Proves the pass-through without a tracing
            //   bridge.
            // Success criteria: exactly one `http.client.requests` observation is recorded for one GET.
            // Why it matters: the observation is the client span and the `http.client.requests` timer of every
            //   call; a factory that dropped the registry would leave a host without either, and nothing would fail.
            //
            // Given: a factory-built client with observation ENABLED
            val handler = RecordingObservationHandler()
            val registry = ObservationRegistry.create().apply { observationConfig().observationHandler(handler) }
            wireMock.stubFor(get(urlEqualTo("/hello")).willReturn(okJson("""{"message":"hi"}""")))
            val client = newObservingClient("greet-obs", registry, enabled = true)

            // When: a request is made
            client.get(Greeting::class.java) { it.uri("/hello") }

            // Then: one http.client.requests observation was opened against our registry
            assertThat(handler.started.map { it.name }).containsExactly("http.client.requests")
        }

        @Test
        fun `should NOT open an http client observation when observation is disabled`() {
            // What is tested: the other position of the toggle - OFF replaces the registry the builder carries with
            //   NOOP for this factory's clients.
            // Success criteria: a GET records no observation at all against the registry the builder carried.
            // Why it matters: the toggle is the operator's way to switch the client spans off for this module
            //   without touching Boot's wiring for every other client; without this case a toggle that did nothing
            //   would pass the case with observation enabled.
            //
            // Given: a factory-built client with observation OFF (registry on the builder, replaced by NOOP)
            val handler = RecordingObservationHandler()
            val registry = ObservationRegistry.create().apply { observationConfig().observationHandler(handler) }
            wireMock.stubFor(get(urlEqualTo("/hello")).willReturn(okJson("""{"message":"hi"}""")))
            val client = newObservingClient("greet-noobs", registry, enabled = false)

            // When: a request runs
            client.get(Greeting::class.java) { it.uri("/hello") }

            // Then: no observation was recorded - the factory replaced the registry with NOOP, so the client emits no span
            assertThat(handler.started).isEmpty()
        }

        @Test
        fun `should nest the http client observation under an active parent observation across the thread hand-off`() {
            // What is tested: with observation ON, an ambient parent observation (the stand-in for an inbound
            //   `http.server.requests` server span) is carried across the decorator's thread hand-off - so the
            //   outbound `http.client.requests` observation, which Spring opens on the WORKER thread, nests as its
            //   CHILD instead of starting a fresh root.
            // Success criteria: the recorded `http.client.requests` context's parent is the parent observation
            //   (`inbound.server`) - the outbound half of the trace reconciliation: server span -> client span, one
            //   trace.
            // Why it matters: the hand-off puts the HTTP call on a thread that has no current observation of its
            //   own. Without the decorator reopening the caller's scope there, every client span would be a detached
            //   root and the outbound call would not join the caller's trace. This is the regression guard for that
            //   propagation, proven over a real HTTP call.
            //
            // Given: an observing client and an ACTIVE parent observation
            val handler = RecordingObservationHandler()
            val registry = ObservationRegistry.create().apply { observationConfig().observationHandler(handler) }
            wireMock.stubFor(get(urlEqualTo("/hello")).willReturn(okJson("""{"message":"hi"}""")))
            val client = newObservingClient("greet-nest", registry, enabled = true)

            // When: the request runs inside the parent's open scope
            val parent = Observation.start("inbound.server", registry)
            parent.openScope().use {
                client.get(Greeting::class.java) { it.uri("/hello") }
            }
            parent.stop()

            // Then the client observation is a CHILD of the parent
            val clientContext = handler.started.single { it.name == "http.client.requests" }
            assertThat(clientContext.parentObservation).isNotNull()
            assertThat(clientContext.parentObservation!!.contextView.name).isEqualTo("inbound.server")
        }

        @Test
        fun `should nest the http client observation under an active parent observation for a POST`() {
            // What is tested: the POST counterpart of the GET nesting case - the parent observation crosses the
            //   hand-off for a request that is built and executed by the POST path.
            // Success criteria: the `http.client.requests` context of the POST has `inbound.server` as its parent.
            // Why it matters: the POST serialises its body and builds its spec differently; a propagation that held
            //   only for the GET path would split every write call off its caller's trace.
            //
            // Given: an observing client and an ACTIVE parent observation
            val handler = RecordingObservationHandler()
            val registry = ObservationRegistry.create().apply { observationConfig().observationHandler(handler) }
            wireMock.stubFor(post(urlEqualTo("/lookup")).willReturn(okJson("""{"message":"hi"}""")))
            val client = newObservingClient("post-nest", registry, enabled = true)

            // When: the POST runs inside the parent's open scope
            val parent = Observation.start("inbound.server", registry)
            parent.openScope().use {
                client.post(Greeting::class.java, LookupRequest("X-1")) {
                    it.uri("/lookup").contentType(MediaType.APPLICATION_JSON)
                }
            }
            parent.stop()

            // Then the client observation is a CHILD of the parent
            val clientContext = handler.started.single { it.name == "http.client.requests" }
            assertThat(clientContext.parentObservation).isNotNull()
            assertThat(clientContext.parentObservation!!.contextView.name).isEqualTo("inbound.server")
        }

        @Test
        fun `should record the decoded response body size so the buffer limit can be sized from evidence`() {
            // What is tested: that every response that FITS the buffer contributes its decoded size to
            //   `restclient.response.body.size`, tagged with the adapter - the same value the log lines carry as
            //   `adapter_name`.
            // Success criteria: one call returning a body containing MULTI-BYTE characters produces exactly
            //   one sample whose maximum is the UTF-8 BYTE size - strictly greater than the UTF-16 length, which
            //   is what makes the assertion about the unit rather than about the number. The meter carries the
            //   `adapter` tag and the `bytes` base unit.
            // Why it matters: `max-in-memory-size` counts bytes; a meter in UTF-16 chars would under-report every
            //   non-ASCII body, and a limit sized from it would reject the bodies it was meant to admit. For a UTF-8
            //   body the client samples the byte count it read without decoding - this pins that the shortcut
            //   reports the same number the encoder would.
            //
            // Given: a registry wired into the factory and a response of a known, non-trivial size
            val registry = SimpleMeterRegistry()
            // Umlauts on purpose: two UTF-8 bytes each, so byte size and String.length cannot coincide.
            val body = """{"message":"${"ä".repeat(200)}${"y".repeat(300)}"}"""
            wireMock.stubFor(get(urlEqualTo("/sized")).willReturn(okJson(body)))
            val client = newClient("sized", meterRegistry = registry)

            // When
            client.get(Greeting::class.java) { it.uri("/sized") }

            // Then
            val summary =
                registry
                    .find(RestClientMetrics.RESPONSE_BODY_SIZE)
                    .tag(ResponseBodyMetrics.ADAPTER_TAG, "sized")
                    .summary()
            assertThat(summary).isNotNull()
            assertThat(summary!!.count()).isEqualTo(1)
            assertThat(summary.max()).isEqualTo(body.toByteArray(Charsets.UTF_8).size.toDouble())
            assertThat(summary.max()).isGreaterThan(body.length.toDouble())
            assertThat(summary.id.baseUnit).isEqualTo(ResponseBodyMetrics.BASE_UNIT)
        }

        @Test
        fun `should bind the pool's meters to the host registry under the pool name`() {
            // What is tested: the counterpart of the reactive twin's `.metrics(true)` on the pool - Apache's pool
            //   stats reach the host's MeterRegistry under this module's prefix, tagged with its pool name.
            // Success criteria: the three gauges Apache's ConnPoolMeters binds (leased, available, pending)
            //   exist under `restclient.pool.*`, all carrying the DEFAULT pool name as `pool`, and nothing else is bound.
            // Why it matters: pool exhaustion is the one transport condition the bulkheads are sized against, and
            //   this is the only place it becomes visible.
            //
            // Given
            val registry = SimpleMeterRegistry()
            wireMock.stubFor(get(urlEqualTo("/pooled")).willReturn(okJson("""{"message":"ok"}""")))
            val client = newClient("pooled", meterRegistry = registry)

            // When
            client.get(Greeting::class.java) { it.uri("/pooled") }

            // Then
            val poolMeters = registry.meters.filter { it.id.name.startsWith("${RestClientMetrics.POOL_METER_PREFIX}.pool.") }
            assertThat(poolMeters.map { it.id.name })
                .containsExactlyInAnyOrder("restclient.pool.leased", "restclient.pool.available", "restclient.pool.pending")
            assertThat(poolMeters.map { it.id.getTag(RestClientMetrics.POOL_TAG) }).containsOnly(RestClientMetrics.POOL_NAME)
            assertThat(RestClientMetrics.POOL_NAME).isEqualTo("restclient-apache")
        }

        @Test
        fun `should leave the response-size meter unregistered when the host wired no registry`() {
            // What is tested: that the meter is optional in the same sense the validator and the observation
            //   registry are - absent host wiring means absent meter, not a failure and not a global-registry
            //   fallback.
            // Success criteria: a call through a client built without a registry succeeds, and an unrelated
            //   registry holds no response-size summary afterwards.
            // Why it matters: a fallback to Micrometer's global registry would publish meters a host never asked
            //   for, from every test and every context, into a JVM-wide singleton.
            //
            // Given
            val registry = SimpleMeterRegistry()
            wireMock.stubFor(get(urlEqualTo("/unmetered")).willReturn(okJson("""{"message":"ok"}""")))
            val client = newClient("unmetered")

            // When
            client.get(Greeting::class.java) { it.uri("/unmetered") }

            // Then
            assertThat(registry.find(RestClientMetrics.RESPONSE_BODY_SIZE).summary()).isNull()
        }
    }

    @Nested
    inner class `The buffer limit` {
        @Test
        fun `should not count a response exceeding the buffer limit against the circuit breaker`() {
            // What is tested: that a response too large for maxInMemorySize does not open the circuit - through the
            //   REAL pipeline, so the exception is the one the client actually produces rather than one written by
            //   hand in a test.
            // Success criteria: five oversized responses in a row all fail for the caller with the limit
            //   exception, while the breaker records zero failures and stays closed.
            // Why it matters: the backend is blameless here: it answered correctly, with a 200, and this client
            //   declined to buffer the answer. Counting that would open the circuit against a healthy backend.
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
            val meshProperties = meshFor(backendName)
            val client =
                newFactory(
                    backendName,
                    circuitBreakers = circuitBreakerRegistry,
                    maxInMemorySize = DataSize.ofBytes(200),
                    meshProperties = meshProperties,
                ).create(backendName)
            // The rule lives in the per-alias customizer, so it has to have been applied - as it is at startup.
            CustomizedRegistries.circuitBreakers(circuitBreakerRegistry, meshProperties, RestClientVerdicts.breakerVerdicts)

            // When: five calls all trip the buffer limit
            repeat(5) {
                val failure = catchThrowable { client.get(Greeting::class.java) { it.uri("/big") } }
                // Assert the REASON, not merely that it failed: if the call ever fails for another reason - a
                // timeout, say - that is a different scenario and the counter assertion below would not mean
                // what it claims.
                assertThat(failure)
                    .`as`("the call must fail on the buffer limit, not for some other reason")
                    .isInstanceOf(ResponseBodyLimitExceededException::class.java)
            }

            // Then: the breaker held none of it against the backend
            val breaker = circuitBreakerRegistry.circuitBreaker(backendName)
            assertThat(breaker.metrics.numberOfFailedCalls).isZero()
            assertThat(breaker.state).isEqualTo(CircuitBreaker.State.CLOSED)
        }

        @Test
        fun `should name the limit and the declared Content-Length on the buffer-limit line`() {
            // What is tested: the one line that carries the two numbers an operator needs - the configured limit and
            //   the length the peer declared - for a response the size meter cannot sample.
            // Success criteria: after an oversized answer, the client logger holds a WARN line naming
            //   `limit=200 bytes` and the body's Content-Length.
            // Why it matters: the size meter only sees bodies that fit, so this line is the only record of how far a
            //   rejected body missed - and the property to raise.
            //
            // Given: a client that declines compression, so the response arrives with the Content-Length the peer
            // declared. Under the default `Accept-Encoding: gzip` Apache decodes the body and STRIPS the header, and
            // the line then reads `Content-Length=unknown` - the documented reading for a decoded body, not a defect.
            val body = """{"message":"${"X".repeat(4000)}"}"""
            wireMock.stubFor(get(urlEqualTo("/big")).willReturn(okJson(body)))
            capturingLinesNaming(clientLogger, "backend 'limit-line'", Level.DEBUG) { appender ->
                val client =
                    newFactory(
                        "limit-line",
                        maxInMemorySize = DataSize.ofBytes(200),
                        restClientBuilder = RestClient.builder().defaultHeader("Accept-Encoding", "identity"),
                    ).create("limit-line")

                // When
                runCatching { client.get(Greeting::class.java) { it.uri("/big") } }

                // Then
                val limitLine = appender.awaitThrough().single { it.formattedMessage.startsWith("Response body exceeded the buffer limit") }
                assertThat(limitLine.level).isEqualTo(Level.WARN)
                assertThat(limitLine.formattedMessage)
                    .contains("limit=200 bytes")
                    .contains("Content-Length=${body.toByteArray().size}")
                    .contains("app.restclient.max-in-memory-size")
            }
        }

        @Test
        fun `should keep a 5xx a server error even when its error body exceeds the buffer limit`() {
            // What is tested: the boundary between the two readings of the limit - an oversized ERROR body is
            //   truncated, not rejected, so the breaker still sees a genuine 5xx.
            // Success criteria: the exception is the server-error type, its body is cut at the limit, and no
            //   limit exception sits anywhere in its cause chain.
            // Why it matters: a limit exception under a 5xx would make the breaker's cause walk discount a genuine
            //   outage as a client-side refusal.
            //
            // Given: a 500 with a body far past a tight limit
            wireMock.stubFor(get(urlEqualTo("/broken")).willReturn(aResponse().withStatus(500).withBody("E".repeat(4000))))
            val client = newClient("big-error", maxInMemorySize = DataSize.ofBytes(200))

            // When
            val failure = catchThrowable { client.get(Greeting::class.java) { it.uri("/broken") } }

            // Then
            assertThat(failure).isInstanceOf(HttpServerErrorException.InternalServerError::class.java)
            assertThat((failure as RestClientResponseException).responseBodyAsByteArray).hasSize(200)
            assertThat(RestClientVerdicts.clientSideFailure.isClientSideFailure(failure)).isFalse()
        }
    }

    @Nested
    inner class `The resilience primitives` {
        @Test
        fun `should fail a call that exceeds the time-limiter budget on a slow backend`() {
            // What is tested: that the TimeLimiter wiring releases the caller on a stalled call - at ITS budget, well
            //   before the transport's own response timeout would have ended the exchange.
            // Success criteria: with a 300 ms budget, a 1400 ms response timeout and a 3 s backend, the call fails
            //   with exactly a TimeoutException, after clearly less than the response timeout, and the breaker
            //   counts it as one failure.
            // Why it matters: the response timeout alone would also fail the call - after 1.4 s, with a
            //   ResourceAccessException. Accepting either would let the test pass with the TimeLimiter gone from the
            //   pipeline, which is exactly the regression it exists to catch: the budget is the caller's deadline,
            //   the transport timeout only the worker's.
            //
            // Given: a 3 s backend delay against a 300 ms budget, and a breaker the test can read
            wireMock.stubFor(
                get(urlEqualTo("/slow")).willReturn(okJson("""{"message":"late"}""").withFixedDelay(3_000)),
            )
            val circuitBreakers = CircuitBreakerRegistry.ofDefaults()
            val client =
                newFactory(
                    "slow",
                    circuitBreakers = circuitBreakers,
                    timeLimiter =
                        TimeLimiterConfig
                            .custom()
                            .timeoutDuration(Duration.ofMillis(300))
                            .cancelRunningFuture(true)
                            .build(),
                ).create("slow")
            assertThat(client.responseTimeout).isEqualTo(Duration.ofMillis(1400))

            // When
            val startNanos = System.nanoTime()
            val failure = catchThrowable { client.get(Greeting::class.java) { it.uri("/slow") } }
            val elapsed = Duration.ofNanos(System.nanoTime() - startNanos)

            // Then
            assertThat(failure).isExactlyInstanceOf(TimeoutException::class.java)
            assertThat(elapsed)
                .`as`("the TimeLimiter must release the caller long before the transport's response timeout")
                .isLessThan(client.responseTimeout)
            assertThat(circuitBreakers.circuitBreaker("slow").metrics.numberOfFailedCalls).isEqualTo(1)
        }

        @Test
        fun `should fail with the transport's own timeout when the response timeout is the tighter bound`() {
            // What is tested: the ladder's normal case - the per-endpoint responseTimeout sits BELOW the TimeLimiter
            //   budget and trips first, so the failure is the transport's (a SocketTimeoutException under Spring's
            //   ResourceAccessException), which the breaker records and legatium classifies as `timeout`.
            // Success criteria: with a 300ms anchor (responseTimeout 700ms) and a generous 30s budget, a 3s
            //   backend fails with a ResourceAccessException caused by a SocketTimeoutException, and the breaker
            //   counts one failure.
            // Why it matters: this is the path that releases the WORKER as well as the caller. The TimeLimiter alone
            //   would leave the worker waiting for the whole 3s; the transport timeout is what frees the connection
            //   and the bulkhead permit.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/stalled")).willReturn(okJson("{}").withFixedDelay(3_000)))
            val circuitBreakers = CircuitBreakerRegistry.ofDefaults()
            val client =
                newFactory(
                    "stalled",
                    circuitBreakers = circuitBreakers,
                    timeLimiter = GENEROUS_BUDGET,
                    meshProperties = meshFor("stalled", individualRequestTimeoutMs = 300),
                ).create("stalled")
            assertThat(client.responseTimeout).isEqualTo(Duration.ofMillis(700))

            // When
            val failure = catchThrowable { client.get(Greeting::class.java) { it.uri("/stalled") } }

            // Then
            assertThat(failure)
                .isInstanceOf(ResourceAccessException::class.java)
                .hasCauseInstanceOf(SocketTimeoutException::class.java)
            assertThat(circuitBreakers.circuitBreaker("stalled").metrics.numberOfFailedCalls).isEqualTo(1)
        }

        @Test
        fun `should open the circuit and short-circuit further calls after the failure threshold is reached`() {
            // What is tested: that the circuit breaker observes the decorated calls and opens once the failure
            //   rate is exceeded, then rejects the next call locally.
            // Success criteria: after five 500s in a window of five, the sixth call fails with
            //   CallNotPermittedException and WireMock saw exactly five requests.
            // Why it matters: this proves the breaker is actually in the call path and records HTTP failures, which
            //   is the whole point of the decoration.
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
                runCatching { client.get(Greeting::class.java) { it.uri("/flaky") } }
            }

            // Then: the sixth call is rejected by the open breaker before it ever reaches the backend.
            assertThatThrownBy { client.get(Greeting::class.java) { it.uri("/flaky") } }
                .isInstanceOf(CallNotPermittedException::class.java)
            wireMock.verify(5, getRequestedFor(urlEqualTo("/flaky")))
        }

        @Test
        fun `should expose the time-limiter timeout as the request budget`() {
            // What is tested: `timeLimiterTimeout`, the budget a client publishes (BaseAdapter reads it).
            // Success criteria: it is the TimeLimiter's configured 7 s, read through the decorator's registry.
            // Why it matters: an adapter derives its own deadlines from this number; a value from anywhere but the
            //   registry the call actually runs under would be a second, drifting budget.
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
    inner class `The client's log lines` {
        @Test
        fun `should keep an interrupted call at DEBUG, leaving the operator signal to the exchange event`() {
            // What is tested: where a cancellation is reported on THIS side. The per-call line must be DEBUG - not
            //   merely absent from WARN, because an accidental drop to TRACE or a removal would pass that weaker
            //   assertion.
            // Success criteria: a caller interrupted while waiting gets an InterruptedException, and the single
            //   "Cancelled request" line from ResilientRestClient is at DEBUG.
            // Why it matters: it pins a TRADE, not a removal. A cancellation is the caller going away
            //   (its budget expired, its scope cancelled) - normal, correlated with load, and therefore arriving in
            //   floods on WARN, the level operators alert on, exactly when the lines that matter are needed. The
            //   cancelled worker's exchange still ends in legatium's event, as a WARN `cancelled` - the other half of the
            //   trade, pinned in LegatiumExchangeLoggingIntegrationTest.
            //
            // Given: a backend that does not answer within the test's window, and the client logger capturing
            wireMock.stubFor(get(urlEqualTo("/never")).willReturn(okJson("{}").withFixedDelay(30_000)))
            val inFlight = arrivals.arrivalOf("/never")
            capturingLinesNaming(clientLogger, "backend 'cancelled'", Level.DEBUG) { clientEvents ->
                val client = newClient("cancelled", timeLimiter = TimeLimiterConfig.custom().timeoutDuration(Duration.ofMinutes(5)).build())
                val callerThread = AtomicReference<Thread>()
                val started = CountDownLatch(1)
                val outcome =
                    callers.submit<Throwable?> {
                        callerThread.set(Thread.currentThread())
                        started.countDown()
                        catchThrowable { client.get(Greeting::class.java) { it.uri("/never") } }
                    }
                assertThat(started.await(5, TimeUnit.SECONDS)).isTrue()
                assertThat(inFlight.await(5, TimeUnit.SECONDS)).`as`("the request must have reached the backend").isTrue()

                // When: the caller walks away before the answer arrives
                callerThread.get().interrupt()

                // Then: the caller sees the interruption, and the per-call line is DEBUG
                assertThat(outcome.get(5, TimeUnit.SECONDS)).isInstanceOf(InterruptedException::class.java)
                val cancelLine = clientEvents.awaitThrough().single { it.formattedMessage.startsWith("Cancelled request") }
                assertThat(cancelLine.level)
                    .`as`("a cancellation is the caller's business, not an operational fault")
                    .isEqualTo(Level.DEBUG)
                assertThat(cancelLine.formattedMessage).containsPattern("duration so far: \\d+ms")
            }
        }

        @Test
        fun `should log a benign 4xx at DEBUG and without a stack trace`() {
            // What is tested: the severity of the client's OWN error line - the second consumer of
            //   HttpExchangeErrorPolicy. A benign 4xx (the documented "does not exist" answer) must be DEBUG, and it
            //   must carry NO stack trace.
            // Success criteria: the captured event is at DEBUG, its throwable proxy is absent, and the message
            //   carries the `expected client error` marker that names the status for a reader.
            // Why it matters: this is the line that would otherwise make every routine 404 look like an outage. The
            //   two consumers derive from ONE predicate precisely so this line and the breaker cannot disagree; the
            //   breaker end is pinned in RestClientVerdictsTest.
            //
            // Given / When: the backend answers "not found"
            val line = errorLineFor(status = 404, backendName = "benign-4xx")

            // Then
            assertThat(line.level).isEqualTo(Level.DEBUG)
            assertThat(line.throwableProxy)
                .`as`("a business-case rejection needs no stack trace - the exchange event already carries the status")
                .isNull()
            assertThat(line.formattedMessage).contains("expected client error")
        }

        @Test
        fun `should log a 5xx at WARN and with the stack trace`() {
            // What is tested: the error line for a 500.
            // Success criteria: WARN, with the server-error exception as its throwable, and without the
            //   `expected client error` marker.
            // Why it matters: the counterpart that keeps the benign-4xx case honest - without it, a line that lost its
            //   severity AND its cause for every failure would satisfy the DEBUG/no-stack-trace assertions.
            //
            // Given / When: the backend answers 500
            val line = errorLineFor(status = 500, backendName = "server-error")

            // Then
            assertThat(line.level).isEqualTo(Level.WARN)
            assertThat(line.throwableProxy?.className).isEqualTo(HttpServerErrorException.InternalServerError::class.java.name)
            assertThat(line.formattedMessage).doesNotContain("expected client error")
        }

        @Test
        fun `should carry the backend name and the URI template on a transport line under the library's field names`() {
            // What is tested: the two structured fields every transport line of this client carries, and that
            //   their NAMES are the exchange event's - `adapter_name` and `adapter_url_template` as legatium spells
            //   them - so one dashboard field groups both loggers.
            // Success criteria: the error line for a 500 carries both key-values with the backend alias and
            //   the endpoint's template.
            // Why it matters: these two fields are what joins a transport line to legatium's exchange event on a
            //   dashboard; a different name, or the expanded path instead of the template, breaks the join.
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
        fun `should keep a 408 at WARN although it is a 4xx`() {
            // What is tested: that the boundary INSIDE the 4xx range holds on this side too - 408 Request Timeout
            //   is an operational signal, not a business case.
            // Success criteria: the line is WARN with a stack trace, i.e. treated exactly like a 5xx, while the
            //   404 of the benign-4xx case proves the same code path does demote a benign status.
            // Why it matters: a demoted 408 would hide a backend timing out under load in DEBUG, the one level
            //   nobody runs in production - and it would do so silently, because the exchange still "fails
            //   correctly" from the caller's point of view. The same status set drives the circuit breaker's record
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
        fun `should report the elapsed time in the failure log from the call's own clock`() {
            // What is tested: that the duration in the error line is measured by the call itself, on the clock
            //   the client was built with.
            // Success criteria: with a clock that advances 1234 ms on every read, the WARN line renders exactly
            //   `duration so far: 1234ms` - the call's start and its failure, one read apart.
            // Why it matters: the duration is what tells a timeout from a fast failure when reading the line; it is
            //   measured by the call itself so it is there whatever the logging configuration. A line that rendered
            //   "null", or a duration taken from another clock or another start than the call's, would read the
            //   same in shape - only the exact value tells.
            //
            // Given: a failing backend, a client on a stepping clock, and the client logger capturing
            wireMock.stubFor(get(urlEqualTo("/boom")).willReturn(aResponse().withStatus(500)))
            val clock = AtomicLong()
            val endpoint = meshFor("own-clock-duration").resolveAll().values.single()
            val client =
                ResilientRestClient(
                    RestClient.builder().baseUrl(endpoint.baseUrl).build(),
                    "own-clock-duration",
                    endpoint,
                    BlockingResilienceDecorator(CircuitBreakerRegistry.ofDefaults(), TimeLimiterRegistry.of(GENEROUS_BUDGET), BulkheadRegistry.ofDefaults())
                        .also { decorators += it },
                    objectMapper,
                    maxInMemoryBytes = RestClientProperties().maxInMemorySize.toBytes().toInt(),
                    nanoTime = { clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(1_234)) },
                )
            // The client logger keeps its configured level here, so the DEBUG closing line is never emitted and
            // cannot serve as the terminal marker - the WARN failure line itself is the last one.
            capturingLinesNaming(clientLogger, "backend 'own-clock-duration'") { appender ->
                // When: the call fails
                runCatching { client.get(Greeting::class.java) { it.uri("/boom") } }

                // Then: the failure line carries the call's own measurement
                val message = appender.awaitThrough(ERROR_PREFIX).single { it.formattedMessage.startsWith(ERROR_PREFIX) }.formattedMessage
                assertThat(message).contains("duration so far: 1234ms")
            }
        }
    }

    @Nested
    inner class `The response validation` {
        @Test
        fun `should fail the call on a schema violation when validation is enforced for the backend`() {
            // What is tested: the validation step after deserialisation, in its enforcing posture - a response that
            //   breaks its DTO's constraints fails the call with every violation named.
            // Success criteria: a body whose `message` is longer than the DTO allows produces a
            //   ConstraintViolationException naming the property; the backend's 200 was received and the breaker
            //   saw a success (the violation sits outside the decoration).
            // Why it matters: a violation is a contract problem, not an outage; it must reach the caller as a typed
            //   failure and must NOT open the circuit against a backend that answered.
            //
            // Given: validation on and enforcing for this backend
            val circuitBreakers = CircuitBreakerRegistry.ofDefaults()
            wireMock.stubFor(get(urlEqualTo("/validated")).willReturn(okJson("""{"message":"far too long for the schema"}""")))
            val client =
                newFactory(
                    "validated",
                    circuitBreakers = circuitBreakers,
                    validation = ResponseValidationProperties(enabled = true, failOnViolation = true),
                    validatorFactory = validatorFactory,
                ).create("validated")

            // When
            val failure = catchThrowable { client.get(ConstrainedGreeting::class.java) { it.uri("/validated") } }

            // Then
            assertThat(failure)
                .isInstanceOf(ConstraintViolationException::class.java)
                .hasMessageContaining("violates its own schema")
                .hasMessageContaining("message")
            assertThat(circuitBreakers.circuitBreaker("validated").metrics.numberOfFailedCalls).isZero()
            assertThat(circuitBreakers.circuitBreaker("validated").metrics.numberOfSuccessfulCalls).isEqualTo(1)
        }

        @Test
        fun `should return the response unchanged and log the violation at WARN in report-only mode`() {
            // What is tested: the rollout posture - `fail-on-violation=false` reports and returns.
            // Success criteria: the same violating body is returned to the caller, and the client logger holds
            //   one WARN line naming the violation and the property that switches enforcement on.
            // Why it matters: report-only is how a backend is adopted against real traffic; it is only useful if the
            //   violation is visible AND the caller is unaffected.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/reported")).willReturn(okJson("""{"message":"far too long for the schema"}""")))
            capturingLinesNaming(clientLogger, "backend 'reported'", Level.DEBUG) { appender ->
                val client =
                    newFactory(
                        "reported",
                        validation = ResponseValidationProperties(enabled = true, failOnViolation = false),
                        validatorFactory = validatorFactory,
                    ).create("reported")

                // When
                val result = client.get(ConstrainedGreeting::class.java) { it.uri("/reported") }

                // Then
                assertThat(result.message).isEqualTo("far too long for the schema")
                val warning = appender.awaitThrough().single { it.level == Level.WARN }
                assertThat(warning.formattedMessage)
                    .contains("violates its own schema")
                    .contains("reported only, not enforced")
                    .contains("app.restclient.validation.fail-on-violation=false")
            }
        }
    }

    @Nested
    inner class `The shutdown` {
        @Test
        fun `should report a pool that does not drain within the deadline and close it anyway`() {
            // What is tested: the deadline branch of shutdown() - a connection still leased when the drain window
            //   has passed is reported as a WARN naming the engine, and the pool is closed regardless.
            // Success criteria: with a worker parked in a slow exchange (the connection stays leased) and a
            //   clock that is past the deadline on its next read, shutdown() returns, writes exactly one WARN
            //   "did not drain within", and does not throw.
            // Why it matters: the branch is unreachable with a real clock short of holding a
            //   connection open longer than the derived window - longer than any response timeout. Only an injected
            //   clock makes it a deterministic test; without one, the one line a shutdown under load would write
            //   would never be seen to work.
            //
            // Given: a slow backend, a factory on a leaping clock, and a call whose connection is leased
            wireMock.stubFor(get(urlEqualTo("/slow")).willReturn(okJson("""{"message":"late"}""").withFixedDelay(5_000)))
            val meters = SimpleMeterRegistry()
            val factory =
                newFactory(
                    "undrained",
                    timeLimiter = GENEROUS_BUDGET,
                    meterRegistry = meters,
                    nanoTime = leapingClock(TimeUnit.HOURS.toNanos(1)),
                )
            val client = factory.create("undrained")
            val inFlight = callers.submit<Throwable?> { catchThrowable { client.get(Greeting::class.java) { it.uri("/slow") } } }
            awaitLeasedConnection(meters)

            try {
                capturingDrainLine { appender ->
                    // When: the context is torn down while the connection is still leased
                    val outcome = catchThrowable { factory.shutdown() }

                    // Then: no exception out of the destroy callback, and the outcome stated as a WARN
                    assertThat(outcome).isNull()
                    val warning = appender.awaitThrough(DRAIN_PREFIX).single()
                    assertThat(warning.level).isEqualTo(Level.WARN)
                    assertThat(warning.formattedMessage)
                        .contains("did not drain within")
                        .contains("exchange(s) still in flight")
                        .contains("'${RestClientEngine.APACHE.key}'")
                }
            } finally {
                // Closing the pool dropped the leased connection, so the parked worker fails rather than waiting 5 s
                inFlight.get(10, TimeUnit.SECONDS)
            }
        }

        @Test
        fun `should stop waiting for the pool when the shutdown thread is interrupted`() {
            // What is tested: the interrupt exit of the drain loop - shutdown() on a thread whose interrupt flag is
            //   set does not wait for the deadline (and does not spin until it), reports the interrupt as the reason,
            //   and leaves the flag set for the caller.
            // Success criteria: with a connection leased and the derived drain window at least a second long,
            //   shutdown() returns well inside that window, the WARN names the interrupt, and the flag is still set
            //   afterwards.
            // Why it matters: parkNanos returns immediately on an interrupted thread and does NOT clear
            //   the flag, so without an explicit exit the loop would burn a core for the whole window - during a
            //   container shutdown, when that core is wanted for the drain of everything else.
            //
            // Given: a slow backend and a call whose connection is leased
            wireMock.stubFor(get(urlEqualTo("/slower")).willReturn(okJson("""{"message":"late"}""").withFixedDelay(5_000)))
            val meters = SimpleMeterRegistry()
            val factory =
                newFactory(
                    "interrupted-drain",
                    timeLimiter = GENEROUS_BUDGET,
                    meterRegistry = meters,
                )
            val client = factory.create("interrupted-drain")
            val inFlight = callers.submit<Throwable?> { catchThrowable { client.get(Greeting::class.java) { it.uri("/slower") } } }
            awaitLeasedConnection(meters)

            try {
                capturingDrainLine { appender ->
                    // When: shutdown runs on a thread that is already interrupted
                    val shutdown =
                        callers.submit<Boolean> {
                            Thread.currentThread().interrupt()
                            factory.shutdown()
                            Thread.currentThread().isInterrupted
                        }

                    // Then: it returns well inside the derived window (2.7 s here, a full second of slack alone) - 2 s,
                    // the bound the engine cases use for the same exit, leaves room for a loaded runner and still fails
                    // a drain that waited the window out or spun until the backend's answer at 5 s ...
                    val flagStillSet = shutdown.get(2, TimeUnit.SECONDS)
                    // ... leaves the interrupt for the caller ...
                    assertThat(flagStillSet).`as`("the interrupt flag must survive shutdown()").isTrue()
                    // ... and says why it stopped waiting
                    val warning = appender.awaitThrough(DRAIN_PREFIX).single()
                    assertThat(warning.level).isEqualTo(Level.WARN)
                    assertThat(warning.formattedMessage).contains("because the shutdown thread was interrupted")
                }
            } finally {
                inFlight.get(10, TimeUnit.SECONDS)
            }
        }

        @Test
        fun `should close the pool on shutdown without raising out of the destroy callback`() {
            // What is tested: that shutdown() completes normally when the pool holds a live keep-alive connection,
            //   and that calling it twice is harmless.
            // Success criteria: neither call throws; the exchange that populated the pool did reach the backend -
            //   so there genuinely was a connection to close rather than an empty pool - and after the first call the
            //   pool is closed.
            // Why it matters: shutdown() runs as a @PreDestroy, where Spring can only catch what it
            //   throws and log it as a destroy method that failed. The drain deadline is an outcome, reported as a
            //   WARN naming the engine; that branch is covered by the case of a pool that does not drain within the
            //   deadline, on an injected clock.
            //
            // Given: one completed exchange, which leaves its connection in the pool (keep-alive is on)
            wireMock.stubFor(get(urlEqualTo("/bye")).willReturn(okJson("""{"message":"bye"}""")))
            val factory = newFactory("drain")
            val client = factory.create("drain")
            client.get(Greeting::class.java) { it.uri("/bye") }

            // When: the context is torn down
            val firstShutdown = catchThrowable { factory.shutdown() }
            val closed = factory.engineTransports.getValue(RestClientEngine.APACHE).isClosed
            val secondShutdown = catchThrowable { factory.shutdown() }

            // Then
            assertThat(firstShutdown).`as`("closing a pool with a live connection must not raise").isNull()
            assertThat(closed).`as`("the first shutdown() must close the pool").isTrue()
            assertThat(secondShutdown).`as`("shutdown must stay idempotent - Spring may call it once, tests twice").isNull()
            wireMock.verify(getRequestedFor(urlEqualTo("/bye")))
        }

        @Test
        fun `should wait for an in-flight exchange to finish before closing the pool`() {
            // What is tested: the drain loop of shutdown() on a real clock - a connection leased by a call still
            //   waiting for its answer keeps shutdown() waiting until the exchange completes and returns the lease.
            // Success criteria: the in-flight call, whose backend delays its answer by 500 ms and has its connection
            //   leased when shutdown() is entered, SUCCEEDS with the backend's value; the pool holds no leased
            //   connection afterwards; shutdown() waited more than 100 ms; and no "did not drain" WARN was written.
            // Why it matters: Apache's close discards leased connections on the spot, so without the wait a graceful
            //   shutdown would cut off every call in flight - the other drain cases (deadline passed, interrupted,
            //   nothing leased) all pass with the loop deleted, this one does not.
            //
            // Given: a backend that answers after 500 ms, and a call whose connection is leased
            wireMock.stubFor(get(urlEqualTo("/finishing")).willReturn(okJson("""{"message":"finished"}""").withFixedDelay(500)))
            val meters = SimpleMeterRegistry()
            val factory = newFactory("draining", meterRegistry = meters)
            val client = factory.create("draining")
            val inFlight = callers.submit<Greeting> { client.get(Greeting::class.java) { it.uri("/finishing") } }
            awaitLeasedConnection(meters)

            capturingDrainLine { appender ->
                // When: the context is torn down while the exchange is still on the wire
                val startNanos = System.nanoTime()
                factory.shutdown()
                val waited = Duration.ofNanos(System.nanoTime() - startNanos)

                // Then: the call completed normally, shutdown waited for it, and nothing was reported
                assertThat(inFlight.get(5, TimeUnit.SECONDS).message).isEqualTo("finished")
                assertThat(leased(meters)).isZero()
                assertThat(waited).`as`("shutdown() must have waited for the exchange").isGreaterThan(Duration.ofMillis(100))
                assertThat(appender.events).`as`("a pool that drained in time is not worth a line").isEmpty()
            }
        }
    }

    @Nested
    inner class `The asynchronous form` {
        @Test
        fun `should deliver a GET response on the future without blocking the caller`() {
            // What is tested: getAsync returns before the backend has answered and completes with the deserialised
            //   value once it has - the same pipeline as get(), minus the wait on the caller's thread.
            // Success criteria: the future is not done right after the call (the backend holds the answer for a
            //   moment), then completes with the decoded body; the request reached the wire once.
            // Why it matters: an asynchronous form that secretly waited for the hand-off before returning would pass
            //   on a fast backend and serialise every caller composing futures.
            //
            // Given: a backend that answers after a short delay
            wireMock.stubFor(get(urlEqualTo("/later")).willReturn(okJson("""{"message":"eventually"}""").withFixedDelay(300)))
            val client = newClient("async-get", timeLimiter = GENEROUS_BUDGET)

            // When
            val future = client.getAsync(Greeting::class.java) { it.uri("/later") }

            // Then
            assertThat(future.isDone).`as`("the caller must get the future back before the answer arrives").isFalse()
            assertThat(future.get(5, TimeUnit.SECONDS).message).isEqualTo("eventually")
            wireMock.verify(1, getRequestedFor(urlEqualTo("/later")))
        }

        @Test
        fun `should send a POST body on the wire and deliver the response on the future`() {
            // What is tested: postAsync end to end - serialisation, the wire, the decoded value on the future.
            // Success criteria: the future completes with the backend's message, and WireMock received the JSON body
            //   exactly once.
            // Why it matters: the asynchronous POST builds its spec on the caller's thread and finishes on the
            //   waiter; a body lost or sent twice between the two would not show in the GET cases.
            //
            // Given
            wireMock.stubFor(post(urlEqualTo("/lookup")).willReturn(okJson("""{"message":"found"}""")))
            val client = newClient("async-post")

            // When
            val result =
                client
                    .postAsync(Greeting::class.java, LookupRequest(id = "42")) {
                        it.uri("/lookup").contentType(MediaType.APPLICATION_JSON)
                    }.get(5, TimeUnit.SECONDS)

            // Then
            assertThat(result.message).isEqualTo("found")
            wireMock.verify(1, postRequestedFor(urlEqualTo("/lookup")).withRequestBody(equalToJson("""{"id":"42"}""")))
        }

        @Test
        fun `should deliver a 4xx on the future as Spring's status exception, unwrapped`() {
            // What is tested: the failure contract of the future - the SAME exception the blocking form throws, so a
            //   handler can match on its type; get() adds only the ExecutionException every CompletableFuture adds.
            // Success criteria: `handle` sees `HttpClientErrorException.NotFound` itself, and `get()` throws an
            //   ExecutionException whose cause is that same instance.
            // Why it matters: a future failing with a wrapper - a CompletionException, a decorator's own type -
            //   would make every handler written against the blocking form miss.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/missing")).willReturn(aResponse().withStatus(404)))
            val client = newClient("async-404")

            // When
            val future = client.getAsync(Greeting::class.java) { it.uri("/missing") }
            val seenByHandler = future.handle { _, t -> t }.get(5, TimeUnit.SECONDS)
            val thrownByGet = catchThrowable { future.get(5, TimeUnit.SECONDS) }

            // Then
            assertThat(seenByHandler).isInstanceOf(HttpClientErrorException.NotFound::class.java)
            assertThat(thrownByGet).isInstanceOf(ExecutionException::class.java).hasCause(seenByHandler)
        }

        @Test
        fun `should fail a POST whose body cannot be serialised on the future, before anything reaches the wire`() {
            // What is tested: the one failure the blocking form throws out of the method arrives, in the asynchronous
            //   form, where every other outcome does - on the future - and still before any exchange exists.
            // Success criteria: the future is already failed when handed back, its cause is the Jackson failure
            //   with the accessor's exception underneath, and WireMock received nothing.
            // Why it matters: an asynchronous caller reads every outcome off the future; a serialisation failure
            //   thrown out of postAsync would escape the caller's composition, and one sent to the wire would be a
            //   phantom exchange.
            //
            // Given
            wireMock.stubFor(post(urlEqualTo("/lookup")).willReturn(okJson("""{"message":"found"}""")))
            val client = newClient("async-unserialisable")

            // When
            val future = client.postAsync(Greeting::class.java, Unserialisable()) { it.uri("/lookup") }

            // Then
            assertThat(future.isCompletedExceptionally).isTrue()
            assertThat(catchThrowable { future.get() }).hasRootCauseInstanceOf(IllegalStateException::class.java)
            wireMock.verify(0, postRequestedFor(urlEqualTo("/lookup")))
        }

        @Test
        fun `should deliver a deserialisation failure on the future without charging the breaker`() {
            // What is tested: deserialisation runs on the waiter, after the decoration, in the asynchronous form as
            //   well - so a body that does not parse fails the future but is not a backend failure for the breaker.
            // Success criteria: the future fails with a Jackson exception; the breaker counted the exchange as
            //   a success (the backend answered 200), not as a failure.
            // Why it matters: deserialisation running inside the decoration in the async form only would open the
            //   circuit against a healthy backend for a client-side problem.
            //
            // Given: a 200 whose body is not JSON, and a breaker the test can read
            wireMock.stubFor(get(urlEqualTo("/garbage")).willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody("not json at all")))
            val circuitBreakers = CircuitBreakerRegistry.of(CircuitBreakerConfig.ofDefaults())
            val client = newFactory("async-garbage", circuitBreakers = circuitBreakers).create("async-garbage")

            // When
            val outcome = client.getAsync(Greeting::class.java) { it.uri("/garbage") }.handle { _, t -> t }.get(5, TimeUnit.SECONDS)

            // Then
            assertThat(outcome).isInstanceOf(JacksonException::class.java)
            assertThat(circuitBreakers.circuitBreaker("async-garbage").metrics.numberOfFailedCalls).isZero()
            assertThat(circuitBreakers.circuitBreaker("async-garbage").metrics.numberOfSuccessfulCalls).isEqualTo(1)
        }

        @Test
        fun `should abandon the exchange and log the cancellation at DEBUG when the future is cancelled`() {
            // What is tested: cancel() on the future the client handed out reaches the decorator's future and through
            //   it the worker - the exchange is abandoned, not merely detached from - and this side reports it the
            //   way it reports an interrupted blocking caller: one "Cancelled request" line at DEBUG.
            // Success criteria: the future is cancelled; the bulkhead permit comes back and the pool's lease is
            //   returned within a bounded wait, although the backend holds its answer for 30 s and neither the
            //   TimeLimiter (5 min) nor the transport's response timeout (>20 s here) would end the exchange in that
            //   window; the client's closing line reports outcome=cancelled, and the single cancellation line is at
            //   DEBUG.
            // Why it matters: a CompletableFuture's cancel() interrupts nothing by itself; without the pass-through a
            //   cancelled call would hold its worker, its permit and its connection until the budget. Only the
            //   released permit and lease prove the abandonment - the future's state and the log line would look the
            //   same if the worker ran on.
            //
            // Given: a backend that does not answer within the test's window, timeouts that would not end the
            // exchange either, a bulkhead and a pool the test can read, and the client logger capturing
            wireMock.stubFor(get(urlEqualTo("/never-async")).willReturn(okJson("{}").withFixedDelay(30_000)))
            val inFlight = arrivals.arrivalOf("/never-async")
            val bulkheads = BulkheadRegistry.ofDefaults()
            val meters = SimpleMeterRegistry()
            capturingLinesNaming(clientLogger, "backend 'cancelled-async'", Level.DEBUG) { clientEvents ->
                val client =
                    newFactory(
                        "cancelled-async",
                        timeLimiter = TimeLimiterConfig.custom().timeoutDuration(Duration.ofMinutes(5)).build(),
                        bulkheads = bulkheads,
                        meterRegistry = meters,
                        meshProperties = meshFor("cancelled-async", individualRequestTimeoutMs = 20_000),
                    ).create("cancelled-async")
                val bulkhead = bulkheads.bulkhead("cancelled-async")
                val permits = bulkhead.bulkheadConfig.maxConcurrentCalls
                val future = client.getAsync(Greeting::class.java) { it.uri("/never-async") }
                assertThat(inFlight.await(5, TimeUnit.SECONDS)).`as`("the request must have reached the backend").isTrue()
                awaitLeasedConnection(meters)
                assertThat(bulkhead.metrics.availableConcurrentCalls).`as`("the exchange holds a permit").isEqualTo(permits - 1)

                // When: the caller walks away before the answer arrives
                assertThat(future.cancel(true)).isTrue()

                // Then: the future is cancelled ...
                assertThat(future.isCancelled).isTrue()
                assertThat(catchThrowable { future.get() }).isInstanceOf(CancellationException::class.java)
                // ... the exchange was abandoned: its permit and its connection are back ...
                assertThat(eventually(AWAIT) { bulkhead.metrics.availableConcurrentCalls == permits })
                    .`as`("the cancelled exchange must release its bulkhead permit within %s", AWAIT)
                    .isTrue()
                assertThat(eventually(AWAIT) { leased(meters) == 0.0 })
                    .`as`("the cancelled exchange must return its pool lease within %s", AWAIT)
                    .isTrue()
                // ... and this side reported it at DEBUG
                val events = clientEvents.awaitThrough()
                val cancelLine = events.single { it.formattedMessage.startsWith("Cancelled request") }
                assertThat(cancelLine.level).isEqualTo(Level.DEBUG)
                assertThat(events.last().formattedMessage).endsWith("outcome=cancelled")
            }
        }

        @Test
        fun `should complete the future when the call itself ends in a cancellation nobody asked for`() {
            // What is tested: the continuation's guarantee that the future handed out is completed on EVERY path -
            //   here the one where the decorated call fails with a CancellationException although the caller never
            //   cancelled (an interceptor on the builder throws one, standing in for any library inside the
            //   exchange that does).
            // Success criteria: the future completes within a bounded wait, with that CancellationException, and the
            //   client reports the call as an error (outcome=error), not as a caller's cancellation.
            // Why it matters: the caller's own cancel() is what completes the future in the cancellation branch; a
            //   CancellationException from anywhere else that were merely logged and dropped would leave the
            //   caller's future pending forever - a hang with no error line, the worst failure an asynchronous API
            //   can have.
            //
            // Given: a builder whose interceptor throws a CancellationException inside the exchange
            val thrown = CancellationException("cancelled inside the exchange")
            val cancelling = RestClient.builder().requestInterceptor { _, _, _ -> throw thrown }
            capturingLinesNaming(clientLogger, "backend 'self-cancelled'", Level.DEBUG) { clientEvents ->
                val client = newFactory("self-cancelled", restClientBuilder = cancelling).create("self-cancelled")

                // When
                val future = client.getAsync(Greeting::class.java) { it.uri("/self-cancelled") }

                // Then: completed, with the exchange's own exception, and reported as an error
                val outcome = future.handle { _, failure -> failure }.get(5, TimeUnit.SECONDS)
                assertThat(outcome).isInstanceOf(CancellationException::class.java).hasMessage(thrown.message)
                assertThat(future.isDone).isTrue()
                assertThat(clientEvents.awaitThrough().last().formattedMessage).endsWith("outcome=error")
            }
        }
    }

    @Nested
    inner class `An answer without a body` {
        @Test
        fun `should fail a 2xx without a body with the named empty-body exception, not charging the breaker`() {
            // What is tested: the response contract for a success without a body - a 204, and a 200 with an empty
            //   entity, for a GET and a POST - in the blocking form.
            // Success criteria: each call fails with EmptyResponseBodyException naming the backend and the type that
            //   was expected; the breaker counted all three exchanges as successes and none as a failure.
            // Why it matters: passed on, the empty string reaches Jackson, which fails with a parse error naming
            //   neither the backend nor the cause, while the reactive twin would complete its Mono empty without any
            //   error. One named exception on both stacks gives a caller one thing to catch; raised outside the
            //   decoration, it cannot open the circuit against a backend that answered promptly with a success
            //   status.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/no-content")).willReturn(aResponse().withStatus(204)))
            wireMock.stubFor(get(urlEqualTo("/empty")).willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json")))
            wireMock.stubFor(post(urlEqualTo("/no-content")).willReturn(aResponse().withStatus(204)))
            val circuitBreakers = CircuitBreakerRegistry.ofDefaults()
            val client = newFactory("empty-body", circuitBreakers = circuitBreakers).create("empty-body")

            // When
            val failures =
                listOf(
                    catchThrowable { client.get(Greeting::class.java) { it.uri("/no-content") } },
                    catchThrowable { client.get(Greeting::class.java) { it.uri("/empty") } },
                    catchThrowable { client.post(Greeting::class.java, LookupRequest("X-1")) { it.uri("/no-content") } },
                )

            // Then
            assertThat(failures).allSatisfy { failure ->
                assertThat(failure).isInstanceOfSatisfying(EmptyResponseBodyException::class.java) {
                    assertThat(it.backendName).isEqualTo("empty-body")
                    assertThat(it.responseType).isEqualTo(Greeting::class.java)
                    assertThat(it.message).contains("'empty-body'", "Greeting")
                }
            }
            val metrics = circuitBreakers.circuitBreaker("empty-body").metrics
            assertThat(metrics.numberOfSuccessfulCalls).isEqualTo(3)
            assertThat(metrics.numberOfFailedCalls).isZero()
        }

        @Test
        fun `should log an empty 2xx body at WARN with its stack trace, like a parse failure`() {
            // What is tested: the client's error line for a 204 where a body was expected.
            // Success criteria: the line is WARN, carries EmptyResponseBodyException as its throwable and no
            //   `expected client error` marker.
            // Why it matters: a backend whose contract promises a body and sends none is an anomaly worth a WARN -
            //   the error line is the one place this side reports it, since the exchange event shows a plain 204
            //   success.
            //
            // Given / When: the backend answers 204 No Content
            val line = errorLineFor(status = 204, backendName = "empty-line")

            // Then
            assertThat(line.level).isEqualTo(Level.WARN)
            assertThat(line.throwableProxy?.className).isEqualTo(EmptyResponseBodyException::class.java.name)
            assertThat(line.formattedMessage).doesNotContain("expected client error")
        }

        @Test
        fun `should deliver an empty 2xx body on the future as the named empty-body exception`() {
            // What is tested: the same contract in the asynchronous form - getAsync on a 204, postAsync on an empty
            //   200.
            // Success criteria: both futures fail with EmptyResponseBodyException (unwrapped, as every outcome of the
            //   future is) and the breaker counted both exchanges as successes.
            // Why it matters: the asynchronous form decodes on the waiter thread; a check that lived only on the
            //   blocking path would let the futures fail with Jackson's parse error.
            //
            // Given
            wireMock.stubFor(get(urlEqualTo("/no-content")).willReturn(aResponse().withStatus(204)))
            wireMock.stubFor(post(urlEqualTo("/empty")).willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json")))
            val circuitBreakers = CircuitBreakerRegistry.ofDefaults()
            val client = newFactory("async-empty-body", circuitBreakers = circuitBreakers).create("async-empty-body")

            // When
            val failures =
                listOf(
                    client.getAsync(Greeting::class.java) { it.uri("/no-content") },
                    client.postAsync(Greeting::class.java, LookupRequest("X-2")) { it.uri("/empty") },
                ).map { it.handle { _, failure -> failure }.get(5, TimeUnit.SECONDS) }

            // Then
            assertThat(failures).allSatisfy { assertThat(it).isInstanceOf(EmptyResponseBodyException::class.java) }
            val metrics = circuitBreakers.circuitBreaker("async-empty-body").metrics
            assertThat(metrics.numberOfSuccessfulCalls).isEqualTo(2)
            assertThat(metrics.numberOfFailedCalls).isZero()
        }
    }

    @Nested
    inner class `The per-endpoint transport settings, the pool lifecycle and the response charset` {
        @Test
        fun `should send an endpoint's static headers and call its own base URL instead of the sidecar`() {
            // What is tested: the two per-endpoint target fields of app.mesh.endpoints[] - `base-url`, which
            //   replaces the sidecar address for that endpoint, and `headers`, applied on top of the client's
            //   defaults (so an endpoint can override the User-Agent too).
            // Success criteria: with the sidecar address pointing at a closed port, the call succeeds against the
            //   endpoint's own base URL, and WireMock sees the static header and the endpoint's User-Agent instead
            //   of the library's token.
            // Why it matters: an endpoint outside the mesh (or a sidecar on another port) is reached only through
            //   these two fields, and no other test exercises them; a factory that ignored them would route such a
            //   backend into the sidecar - or to nowhere - without a test failing.
            //
            // Given: a sidecar that refuses connections, and an endpoint with its own base URL and headers
            wireMock.stubFor(get(urlEqualTo("/direct")).willReturn(okJson("""{"message":"direct"}""")))
            val meshProperties =
                MeshProperties(
                    defaults =
                        MeshProperties.Defaults(
                            individualRequestTimeoutMs = 1000,
                            transport = MeshProperties.Transport(sidecarBaseUrl = "http://127.0.0.1:1"),
                        ),
                    endpoints =
                        listOf(
                            MeshProperties.EndpointConfig(
                                triplet = "/endpoint/localhost/GET/direct",
                                alias = "direct",
                                baseUrl = wireMock.baseUrl(),
                                headers = mapOf("X-Static-Header" to "from-the-endpoint", "User-Agent" to "endpoint-agent/1.0"),
                            ),
                        ),
                )
            val client = newFactory("direct", meshProperties = meshProperties).create("direct")

            // When
            val result = client.get(Greeting::class.java) { it.uri("/direct") }

            // Then
            assertThat(result.message).isEqualTo("direct")
            wireMock.verify(
                getRequestedFor(urlEqualTo("/direct"))
                    .withHeader("X-Static-Header", equalTo("from-the-endpoint"))
                    .withHeader("User-Agent", equalTo("endpoint-agent/1.0")),
            )
        }

        @Test
        fun `should close a pooled connection past its lifetime from the background evictor and stop it on shutdown`() {
            // What is tested: the pool's lifecycle settings with non-zero values on the Apache engine - `max-life-time`
            //   as the connections' time-to-live and `evict-in-background` as that engine's own eviction schedule - and
            //   that shutdown stops that schedule.
            // Success criteria: a keep-alive connection left idle in the pool after a call (the `available` gauge
            //   reads 1 right after it) is closed by the evictor once its 1 s lifetime has passed (the gauge drops
            //   back to zero) although its idle timeout is an hour - so it is the lifetime, applied by the
            //   background schedule, that closed it; and after shutdown() the evictor is shut down.
            // Why it matters: both settings default to minutes, so no other test sees the scheduled task run.
            //   A lifetime that never reached the pool, or an evictor that never ran, would let connections outlive
            //   the sidecar's own limits - and an evictor left running after shutdown would keep a thread working on
            //   a closed pool.
            //
            // Given: a short lifetime, a long idle timeout, a fast eviction schedule, and a completed call
            wireMock.stubFor(get(urlEqualTo("/kept-alive")).willReturn(okJson("""{"message":"hi"}""")))
            val meters = SimpleMeterRegistry()
            val factory =
                newFactory(
                    "evicted",
                    meterRegistry = meters,
                    pool =
                        RestClientProperties.Pool(
                            maxIdleTime = Duration.ofHours(1),
                            maxLifeTime = Duration.ofSeconds(1),
                            evictInBackground = Duration.ofMillis(50),
                        ),
                )
            factory.create("evicted").get(Greeting::class.java) { it.uri("/kept-alive") }
            assertThat(factory.evictor).`as`("a non-zero interval must schedule the evictor").isNotNull()
            // The connection must have been IN the pool for its closing to say anything: an engine that closed it
            // after the call (no keep-alive, a pool that never kept it) would read "available 0" just the same.
            assertThat(available(meters)).`as`("the call's connection must be back in the pool, idle").isEqualTo(1.0)

            // When: the lifetime passes, and later the context shuts down
            assertThat(eventually(AWAIT) { available(meters) == 0.0 && leased(meters) == 0.0 })
                .`as`("the evictor must close the expired connection within %s", AWAIT)
                .isTrue()
            factory.shutdown()

            // Then
            assertThat(factory.evictor!!.isShutdown).`as`("shutdown() must stop the eviction schedule").isTrue()
        }

        @Test
        fun `should decode a body in the charset the response declares`() {
            // What is tested: a response declaring `charset=ISO-8859-1` with umlauts encoded in that charset - the
            //   path that decodes the body with its declared charset before Jackson reads it (a UTF-8 body is read
            //   from the bytes directly).
            // Success criteria: the umlauts arrive intact, and the size meter records the UTF-8 size of the decoded
            //   text (two bytes per umlaut), not the one byte per umlaut that came over the wire.
            // Why it matters: read as UTF-8, a Latin-1 umlaut is an invalid byte sequence and becomes a replacement
            //   character - the backend's data silently corrupted. And the meter must speak one unit for every body,
            //   or a limit sized from it would mean different things per backend.
            //
            // Given: a Latin-1 body with umlauts, and a registry
            val text = """{"message":"Grüße aus Köln"}"""
            wireMock.stubFor(
                get(urlEqualTo("/latin1")).willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json;charset=ISO-8859-1")
                        .withBody(text.toByteArray(Charsets.ISO_8859_1)),
                ),
            )
            val registry = SimpleMeterRegistry()
            val client = newClient("latin1", meterRegistry = registry)

            // When
            val result = client.get(Greeting::class.java) { it.uri("/latin1") }

            // Then
            assertThat(result.message).isEqualTo("Grüße aus Köln")
            val summary = registry.find(RestClientMetrics.RESPONSE_BODY_SIZE).tag(ResponseBodyMetrics.ADAPTER_TAG, "latin1").summary()
            assertThat(summary).isNotNull()
            assertThat(summary!!.max()).isEqualTo(text.toByteArray(Charsets.UTF_8).size.toDouble())
            assertThat(summary.max()).isGreaterThan(text.toByteArray(Charsets.ISO_8859_1).size.toDouble())
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /**
     * One validator factory for the validation cases; Hibernate Validator is on the test classpath. Built on
     * first use and closed in [tearDown] when it was - a factory holds caches and a bean-metadata manager that
     * only `close` releases.
     */
    private val validatorFactoryHolder = lazy { Validation.buildDefaultValidatorFactory() }
    private val validatorFactory: ValidatorFactory by validatorFactoryHolder

    /** A single-endpoint mesh for [backendName] against WireMock. */
    private fun meshFor(
        backendName: String,
        individualRequestTimeoutMs: Long = 1000,
    ): MeshProperties = meshOf(wireMock.baseUrl(), "localhost", backendName, individualRequestTimeoutMs = individualRequestTimeoutMs)

    /**
     * A factory over registries built for this one test. The TimeLimiter defaults to [GENEROUS_BUDGET]: a test
     * that is not ABOUT timing must not fail because a loaded build machine took a second for the first request
     * of the JVM (class loading, JIT, the pool's first connection). Only the timeout cases pass a short budget.
     */
    private fun newFactory(
        backendName: String,
        timeLimiter: TimeLimiterConfig = GENEROUS_BUDGET,
        circuitBreaker: CircuitBreakerConfig = CircuitBreakerConfig.ofDefaults(),
        bulkhead: BulkheadConfig = BulkheadConfig.ofDefaults(),
        circuitBreakers: CircuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreaker),
        bulkheads: BulkheadRegistry = BulkheadRegistry.of(bulkhead),
        maxInMemorySize: DataSize = RestClientProperties().maxInMemorySize,
        pool: RestClientProperties.Pool = RestClientProperties.Pool(),
        meterRegistry: MeterRegistry? = null,
        meshProperties: MeshProperties = meshFor(backendName),
        validation: ResponseValidationProperties = ResponseValidationProperties(),
        userAgent: String? = null,
        validatorFactory: ValidatorFactory? = null,
        // Deliberately no Spring context here: a bare builder stands in for Boot's bean. A test that needs to
        // observe what the factory puts on its clients passes a builder carrying an interceptor of its own.
        restClientBuilder: RestClient.Builder = RestClient.builder(),
        nanoTime: () -> Long = System::nanoTime,
    ): ResilientRestClientFactory {
        val resilience =
            BlockingResilienceDecorator(
                circuitBreakers,
                TimeLimiterRegistry.of(timeLimiter),
                bulkheads,
            ).also { decorators += it }
        return ResilientRestClientFactory(
            RestClientProperties(maxInMemorySize = maxInMemorySize, pool = pool, validation = validation, userAgent = userAgent),
            meshProperties,
            resilience,
            objectMapper,
            validatorFactory = validatorFactory,
            meterRegistry = meterRegistry,
            restClientBuilder = restClientBuilder,
            nanoTime = nanoTime,
        ).also { factories += it }
    }

    /**
     * Blocks until [meters] reports at least one leased connection on the factory's pool, or fails after a
     * bounded wait. The pool's `leased` gauge is the only observable the transport offers for "a worker is on
     * the wire"; a latch inside the call would fire before the connection is actually taken.
     */
    private fun awaitLeasedConnection(meters: MeterRegistry) {
        assertThat(eventually(AWAIT) { leased(meters) >= 1 }).`as`("a connection must be leased within %s", AWAIT).isTrue()
    }

    private fun leased(meters: MeterRegistry): Double = poolGauge(meters, "leased")

    private fun available(meters: MeterRegistry): Double = poolGauge(meters, "available")

    private fun poolGauge(
        meters: MeterRegistry,
        name: String,
    ): Double =
        meters
            .find("${RestClientMetrics.POOL_METER_PREFIX}.pool.$name")
            .gauge()
            ?.value() ?: 0.0

    /** Attaches a capture of the factory's drain line for the duration of [block]. */
    private fun capturingDrainLine(block: (CapturingAppender) -> Unit) {
        capturingLinesNaming(LoggerFactory.getLogger(ResilientRestClientFactory::class.java) as Logger, DRAIN_MARKER, block = block)
    }

    /**
     * Drives one exchange that the backend answers with [status], and returns the client's own error line as a
     * captured event.
     *
     * The logger is raised to DEBUG for the duration on purpose: the benign-4xx line is emitted at exactly that
     * level, so at the default level the event would never be created and every assertion on it would pass
     * vacuously. Restored afterwards, since the level is global to the logger.
     */
    private fun errorLineFor(
        status: Int,
        backendName: String,
    ): ILoggingEvent {
        val path = "/status-$status"
        wireMock.stubFor(get(urlEqualTo(path)).willReturn(aResponse().withStatus(status)))
        return capturingLinesNaming(clientLogger, "backend '$backendName'", Level.DEBUG) { appender ->
            val client = newClient(backendName)
            runCatching { client.get(Greeting::class.java) { it.uri(path) } }
            appender.awaitThrough().single { it.formattedMessage.startsWith(ERROR_PREFIX) }
        }
    }

    /** A client of a fresh [newFactory]; the same [GENEROUS_BUDGET] default, for the same reason. */
    private fun newClient(
        backendName: String,
        timeLimiter: TimeLimiterConfig = GENEROUS_BUDGET,
        circuitBreaker: CircuitBreakerConfig = CircuitBreakerConfig.ofDefaults(),
        bulkhead: BulkheadConfig = BulkheadConfig.ofDefaults(),
        maxInMemorySize: DataSize = RestClientProperties().maxInMemorySize,
        meterRegistry: MeterRegistry? = null,
        userAgent: String? = null,
    ): ResilientRestClient =
        newFactory(
            backendName,
            timeLimiter = timeLimiter,
            circuitBreaker = circuitBreaker,
            bulkhead = bulkhead,
            maxInMemorySize = maxInMemorySize,
            meterRegistry = meterRegistry,
            userAgent = userAgent,
        ).create(backendName)

    /**
     * A client whose factory received a builder carrying [registry] - the stand-in for what Boot's
     * `ObservationRestClientCustomizer` puts on the auto-configured builder - gated by [enabled]. The decorator
     * receives the same registry, as it would from the context, so the caller's observation can cross the
     * hand-off.
     */
    private fun newObservingClient(
        backendName: String,
        registry: ObservationRegistry,
        enabled: Boolean,
    ): ResilientRestClient {
        val resilience =
            BlockingResilienceDecorator(
                CircuitBreakerRegistry.of(CircuitBreakerConfig.ofDefaults()),
                // Deliberately generous, like every helper here: Resilience4j's DEFAULT 1 s budget makes a test
                // timing-dependent, because a slow call under load would fail with a TimeoutException instead.
                TimeLimiterRegistry.of(GENEROUS_BUDGET),
                BulkheadRegistry.of(BulkheadConfig.ofDefaults()),
                observationRegistry = registry,
            ).also { decorators += it }
        val properties =
            RestClientProperties(observability = RestClientProperties.Observability(httpObservationEnabled = enabled))
        return ResilientRestClientFactory(
            properties,
            meshFor(backendName),
            resilience,
            objectMapper,
            // No Spring context here: the registry is placed on the builder by hand, as Boot's customizer would.
            restClientBuilder = RestClient.builder().observationRegistry(registry),
        ).also { factories += it }
            .create(backendName)
    }

    /**
     * Runs [block] with a capture of [logger]'s lines naming [marker] attached, the logger opened to [level] when
     * one is given - see [capturing]. The capture is readable from a thread OTHER than the one that logged them,
     * which some assertions in this class need: the client's own lines are written on the calling thread, but a
     * call driven from a helper thread (the interruption case) logs there.
     *
     * [marker] scopes the capture to one call: the logger is a shared, process-wide one, so a lingering call
     * from an earlier test (an abandoned worker, a late timeout) would otherwise be captured as well. Every
     * message of interest names its backend (`... backend '<name>' ...`), and backend names are unique per test.
     */
    private fun <T> capturingLinesNaming(
        logger: Logger,
        marker: String,
        level: Level? = null,
        block: (CapturingAppender) -> T,
    ): T = capturing(logger, level, accept = { it.formattedMessage.contains(marker) }, block = block)

    /**
     * Everything captured, once the line starting with [terminalPrefix] - the last one the call can produce - has
     * been logged, so the capture is not merely safe to read but COMPLETE.
     */
    private fun CapturingAppender.awaitThrough(terminalPrefix: String = COMPLETED_PREFIX): List<ILoggingEvent> = awaitEvents { it.formattedMessage.startsWith(terminalPrefix) }

    private class RecordingObservationHandler : ObservationHandler<Observation.Context> {
        val started = CopyOnWriteArrayList<Observation.Context>()

        override fun supportsContext(context: Observation.Context): Boolean = true

        override fun onStart(context: Observation.Context) {
            started.add(context)
        }
    }
}

/** [Greeting] with a constraint the validation cases can violate; the mapper reads it the same way. */
class ConstrainedGreeting {
    @field:Size(max = 5)
    var message: String? = null
}
