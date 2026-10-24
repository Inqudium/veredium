package eu.inqudium.veredium.examples.mixed

import eu.inqudium.veredium.examples.Backends
import eu.inqudium.veredium.examples.ExampleContext
import eu.inqudium.veredium.examples.Sidecar
import eu.inqudium.veredium.examples.Sidecar.ADA
import eu.inqudium.veredium.examples.Sidecar.ADAS_ORDERS
import eu.inqudium.veredium.restclient.ResilientRestClientFactory
import eu.inqudium.veredium.webclient.ResilientWebClientFactory
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.test.context.ActiveProfiles
import org.springframework.web.client.HttpClientErrorException
import org.springframework.web.reactive.function.client.WebClientResponseException
import java.time.Duration

/**
 * The mixed example as a host runs it: both client modules active in one Boot context, each mesh endpoint owned by
 * the stack its `stack` key names.
 */
@ExampleContext(MixedExampleApplication::class)
@ActiveProfiles(PROFILE)
class MixedExampleTest(
    private val adapter: MixedCustomerAdapter,
    private val restClients: ResilientRestClientFactory,
    private val webClients: ResilientWebClientFactory,
    private val circuitBreakers: CircuitBreakerRegistry,
) {
    @BeforeEach
    fun stubBackends() {
        Sidecar.reset()
    }

    @Test
    fun `should call each backend through the stack that owns it`() {
        // What is tested: MixedCustomerAdapter - a blocking GET on the restclient's endpoint, a reactive POST on the
        //   webclient's - in one context with both auto-configurations active.
        // Success criteria: the customer and the orders arrive; the stubs answer only the Host header of each
        //   triplet, so the answers also prove the routing.
        // Why it matters: it is what ADR-0003 is for - a host that runs both clients, each backend on the stack that
        //   fits it - and the proof that the context starts with both stacks' mesh wiring.
        //
        // When
        val customer = adapter.customer("c-1")
        val page = adapter.orders("c-1").block()

        // Then
        assertThat(customer).isEqualTo(ADA)
        assertThat(page?.orders).isEqualTo(ADAS_ORDERS)
    }

    @Test
    fun `should refuse the other stack's endpoint in each factory, naming its owner`() {
        // What is tested: create() of each factory for the endpoint the other stack owns.
        // Success criteria: both fail with an IllegalArgumentException naming the owning stack.
        // Why it matters: one backend served by both stacks would split its traffic over two pools, two bulkheads
        //   and two sets of verdicts on one breaker; the factory says whose endpoint it is instead.
        //
        // When / Then
        assertThatThrownBy { restClients.create(Backends.ORDER_SEARCH) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("owned by the webclient stack")
        assertThatThrownBy { webClients.create(Backends.CUSTOMERS) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("owned by the restclient stack")
    }

    @Test
    fun `should run each breaker on the verdicts of the stack that calls it`() {
        // What is tested: the record predicate of each endpoint's circuit breaker, given a 404 in the shape of each
        //   stack.
        // Success criteria: each breaker leaves its own stack's 404 unrecorded and records the other stack's - the
        //   verdicts it carries are its owner's, and only its owner's.
        // Why it matters: with both stacks in one context, a breaker customized by the wrong stack would count every
        //   benign 404 of its real caller as a backend failure and open against a healthy backend.
        //
        // Given: a 404 as each client throws it
        val blocking404 = HttpClientErrorException.create(HttpStatus.NOT_FOUND, "Not Found", HttpHeaders(), ByteArray(0), null)
        val reactive404 = WebClientResponseException(404, "Not Found", null, null, null)

        // When
        val customersBreaker = circuitBreakers.circuitBreaker(Backends.CUSTOMERS).circuitBreakerConfig.recordExceptionPredicate
        val ordersBreaker = circuitBreakers.circuitBreaker(Backends.ORDER_SEARCH).circuitBreakerConfig.recordExceptionPredicate

        // Then
        assertThat(customersBreaker.test(blocking404)).`as`("customers (restclient) records a RestClient 404").isFalse()
        assertThat(ordersBreaker.test(reactive404)).`as`("order-search (webclient) records a WebClient 404").isFalse()
        assertThat(customersBreaker.test(reactive404)).`as`("customers (restclient) records a WebClient 404").isTrue()
        assertThat(ordersBreaker.test(blocking404)).`as`("order-search (webclient) records a RestClient 404").isTrue()
    }

    @Test
    fun `should publish the ceiling of both stacks' budgets`() {
        // What is tested: the BaseAdapter values of MixedCustomerAdapter, whose two clients run on different stacks.
        // Success criteria: the values of the slower endpoint, order-search on the reactive client: the response
        //   timeout 900 ms and the TimeLimiter budget 1300 ms - the blocking customers endpoint's 700 ms and 1100 ms
        //   are lower.
        // Why it matters: both stacks derive the budgets from the same ladder, so the ceiling over a blocking and a
        //   reactive client is a number an orchestrating caller can compose like any other adapter's.
        //
        // When
        val response = adapter.responseTimeout
        val budget = adapter.timeLimiterTimeout

        // Then
        assertThat(response).isEqualTo(Duration.ofMillis(900))
        assertThat(budget).isEqualTo(Duration.ofMillis(1300))
    }
}
