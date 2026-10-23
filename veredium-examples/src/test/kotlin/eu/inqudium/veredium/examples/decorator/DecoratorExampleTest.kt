package eu.inqudium.veredium.examples.decorator

import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.okJson
import com.github.tomakehurst.wiremock.client.WireMock.serverError
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import eu.inqudium.veredium.core.resilience.BlockingResilienceDecorator
import eu.inqudium.veredium.examples.Sidecar
import eu.inqudium.veredium.examples.Sidecar.ADA
import eu.inqudium.veredium.testsupport.DrippingBackend
import eu.inqudium.veredium.testsupport.eventually
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import org.apache.http.impl.client.CloseableHttpClient
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import tools.jackson.databind.ObjectMapper
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * The decorator example as a host runs it: the whole Boot application, HttpClient 4 calling the stand-in sidecar
 * on a worker of the decorator.
 *
 * Like the other examples' contexts it stays off Micrometer's global registry, which it would join after another
 * context had registered meters there.
 */
@SpringBootTest(classes = [DecoratorExampleApplication::class], properties = ["management.metrics.use-global-registry=false"])
class DecoratorExampleTest {
    @Autowired
    private lateinit var directory: LegacyCustomerDirectory

    @Autowired
    private lateinit var circuitBreakers: CircuitBreakerRegistry

    @Autowired
    private lateinit var bulkheads: BulkheadRegistry

    @Autowired
    private lateinit var decorator: BlockingResilienceDecorator

    @Autowired
    private lateinit var httpClient: CloseableHttpClient

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @Autowired
    private lateinit var properties: LegacyDirectoryProperties

    @BeforeEach
    fun stubBackends() {
        Sidecar.reset()
        circuitBreakers.circuitBreaker(LegacyCustomerDirectory.NAME).reset()
    }

    @Test
    fun `should read a customer with the native client, addressed by the Host header it sets itself`() {
        // What is tested: LegacyCustomerDirectory.customer for a customer the directory knows.
        // Success criteria: the customer is decoded; the stub only answers the Host header the sidecar routes on.
        // Why it matters: it is the basic call of the example - a native HttpClient 4 exchange on a worker of the
        //   decorator, with the routing header the client modules would have derived from the triplet.
        //
        // When
        val customer = directory.customer("c-1")

        // Then
        assertThat(customer).isEqualTo(ADA)
    }

    @Test
    fun `should answer an unknown customer with null and leave the breaker untouched`() {
        // What is tested: LegacyCustomerDirectory.customer for an id the directory answers with 404.
        // Success criteria: the call returns null; the breaker counts no failure.
        // Why it matters: the 404 crosses the breaker as an exception; without the customizer that ignores it,
        //   a run of unknown ids would open the circuit against a healthy backend.
        //
        // When
        val customer = directory.customer("c-404")

        // Then
        assertThat(customer).isNull()
        assertThat(circuitBreakers.circuitBreaker(LegacyCustomerDirectory.NAME).metrics.numberOfFailedCalls).isZero()
    }

    @Test
    fun `should count a server error as a failure of the backend`() {
        // What is tested: LegacyCustomerDirectory.customer for an id the directory answers with 500.
        // Success criteria: the caller gets DirectoryFailureException; the breaker counts one failure.
        // Why it matters: the other side of the previous case - the breaker must still see the faults it exists for.
        //
        // Given
        Sidecar.server.stubFor(get(urlEqualTo("/v1/customers/c-500")).willReturn(serverError()))

        // When / Then
        assertThatThrownBy { directory.customer("c-500") }.isInstanceOf(DirectoryFailureException::class.java)
        assertThat(circuitBreakers.circuitBreaker(LegacyCustomerDirectory.NAME).metrics.numberOfFailedCalls).isEqualTo(1)
    }

    @Test
    fun `should let the client's socket timeout trip first against a silent directory`() {
        // What is tested: a lookup against a directory that answers only after 30 s, under the 1 s budget and the
        //   700 ms socket timeout.
        // Success criteria: the caller gets HttpClient 4's SocketTimeoutException after at least 700 ms and before
        //   the 1 s budget; the breaker counts it as a failure.
        // Why it matters: the bounds are layered - the transport's timeout sits under the budget, so a silent
        //   backend is reported by the transport, with its own exception, and the budget stays the outer backstop.
        //
        // Given
        Sidecar.server.stubFor(
            get(urlEqualTo("/v1/customers/c-silent")).willReturn(okJson("""{"id":"c-silent","name":"Silent"}""").withFixedDelay(30_000)),
        )

        // When
        val started = System.nanoTime()
        val failure = catchThrowable { directory.customer("c-silent") }
        val waited = Duration.ofNanos(System.nanoTime() - started)

        // Then
        assertThat(failure).isInstanceOf(SocketTimeoutException::class.java)
        assertThat(waited).isGreaterThanOrEqualTo(Duration.ofMillis(700)).isLessThan(Duration.ofSeconds(1))
        assertThat(circuitBreakers.circuitBreaker(LegacyCustomerDirectory.NAME).metrics.numberOfFailedCalls).isEqualTo(1)
    }

    @Test
    fun `should release the caller at the budget and end the worker's read at once`() {
        // What is tested: a lookup against a directory whose body drips in, one byte every 200 ms for 30 s - every
        //   read well inside the 700 ms socket timeout, the whole call far past the 1 s budget.
        // Success criteria: the caller gets a TimeoutException after at least 1 s and well before the 30 s; within
        //   2 s after that the client has dropped the connection and the bulkhead has all its permits back.
        // Why it matters: HttpClient 4 bounds each read, not a whole call; the decorator's TimeLimiter is that
        //   bound. The dropped connection proves that the interrupt ended the worker's blocked read - with
        //   `cancel-running-future: false` the worker would read on for 30 s, holding its permit and connection.
        //
        // Given
        DrippingBackend().use { backend ->
            val dripping = directoryAt(backend.baseUrl)
            val bulkhead = bulkheads.bulkhead(LegacyCustomerDirectory.NAME)
            val permits = bulkhead.bulkheadConfig.maxConcurrentCalls

            // When
            val started = System.nanoTime()
            val failure = catchThrowable { dripping.customer("c-drip") }
            val waited = Duration.ofNanos(System.nanoTime() - started)

            // Then
            assertThat(failure).isInstanceOf(TimeoutException::class.java)
            assertThat(waited).isGreaterThanOrEqualTo(Duration.ofSeconds(1)).isLessThan(Duration.ofSeconds(5))
            assertThat(backend.connectionDropped.await(2, TimeUnit.SECONDS)).`as`("the worker's read was ended").isTrue()
            assertThat(eventually(Duration.ofSeconds(2)) { bulkhead.metrics.availableConcurrentCalls == permits }).isTrue()
        }
    }

    @Test
    fun `should deliver the customer on a future, and end the call when the future is cancelled`() {
        // What is tested: LegacyCustomerDirectory.customerAsync for a known customer, and against a directory
        //   whose body drips in for 30 s, cancelled while it arrives.
        // Success criteria: the first future completes with the customer within 5 s; within 2 s after the cancel
        //   the client has dropped the dripping connection and the bulkhead has all its permits back.
        // Why it matters: the asynchronous form shares the pipeline; cancelling its future is how an asynchronous
        //   caller walks away, and it has to end the exchange as an interrupted blocking caller does.
        //
        // Given
        DrippingBackend().use { backend ->
            val dripping = directoryAt(backend.baseUrl)
            val bulkhead = bulkheads.bulkhead(LegacyCustomerDirectory.NAME)
            val permits = bulkhead.bulkheadConfig.maxConcurrentCalls

            // When
            val known = directory.customerAsync("c-1").get(5, TimeUnit.SECONDS)
            val slow = dripping.customerAsync("c-drip")
            assertThat(eventually { backend.connections.get() == 1 }).`as`("the slow call is on the wire").isTrue()
            slow.cancel(true)

            // Then
            assertThat(known).isEqualTo(ADA)
            assertThat(slow).isCancelled()
            assertThat(backend.connectionDropped.await(2, TimeUnit.SECONDS)).`as`("the worker's read was ended").isTrue()
            assertThat(eventually(Duration.ofSeconds(2)) { bulkhead.metrics.availableConcurrentCalls == permits }).isTrue()
        }
    }

    @Test
    fun `should fail the future of an unknown customer with the directory's answer`() {
        // What is tested: LegacyCustomerDirectory.customerAsync for an id answered with 404.
        // Success criteria: the future fails with UnknownCustomerException as its cause, unwrapped by the decorator.
        // Why it matters: every outcome of the asynchronous form arrives on the future as the exception the
        //   blocking form would have thrown.
        //
        // When
        val failure = catchThrowable { directory.customerAsync("c-404").get(5, TimeUnit.SECONDS) }

        // Then
        assertThat(failure).isInstanceOf(ExecutionException::class.java).hasCauseInstanceOf(UnknownCustomerException::class.java)
    }

    @Test
    fun `should publish the call's budget above the client's response timeout`() {
        // What is tested: the BaseAdapter values of LegacyCustomerDirectory.
        // Success criteria: the TimeLimiter budget is the configured 1 s, above the 700 ms socket timeout.
        // Why it matters: the same two numbers the client modules publish; here the host chose them itself, and
        //   the order - transport below the budget - is what makes the transport trip first.
        //
        // When
        val budget = directory.timeLimiterTimeout
        val response = directory.responseTimeout

        // Then
        assertThat(budget).isEqualTo(Duration.ofSeconds(1))
        assertThat(response).isEqualTo(Duration.ofMillis(700))
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /**
     * The example's directory with the same decorator, client and mapper, pointed at [baseUrl] - for a backend
     * that is not the stand-in sidecar.
     */
    private fun directoryAt(baseUrl: String) = LegacyCustomerDirectory(decorator, httpClient, objectMapper, properties.copy(baseUrl = baseUrl))

    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun sidecar(registry: DynamicPropertyRegistry) {
            Sidecar.register(registry)
            registry.add("legacy-directory.base-url") { Sidecar.server.baseUrl() }
        }
    }
}
