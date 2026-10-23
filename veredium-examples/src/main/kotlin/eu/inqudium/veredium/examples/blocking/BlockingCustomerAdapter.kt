package eu.inqudium.veredium.examples.blocking

import eu.inqudium.veredium.core.BaseAdapter
import eu.inqudium.veredium.examples.Backends
import eu.inqudium.veredium.examples.Customer
import eu.inqudium.veredium.examples.OrderPage
import eu.inqudium.veredium.examples.OrderSearch
import eu.inqudium.veredium.restclient.ResilientRestClientFactory
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.client.HttpClientErrorException
import java.time.Duration

/**
 * An adapter over two backends with the blocking client: one [eu.inqudium.veredium.restclient.ResilientRestClient]
 * per mesh alias, created once from the factory. A call only describes the request - the URI, a body, a content
 * type; the client runs it through the bulkhead, the time limiter and the circuit breaker of its alias and
 * deserialises the answer.
 *
 * Every call blocks the calling thread until the answer is there. The same calls without the wait are the
 * asynchronous example, [eu.inqudium.veredium.examples.async.AsyncCustomerAdapter].
 */
@Component
class BlockingCustomerAdapter(
    factory: ResilientRestClientFactory,
) : BaseAdapter {
    private val customers = factory.create(Backends.CUSTOMERS)
    private val orderSearch = factory.create(Backends.ORDER_SEARCH)

    /** The pessimistic ceiling over both clients: what one call of this adapter may take at most. */
    override val timeLimiterTimeout: Duration
        get() = maxOf(customers.timeLimiterTimeout, orderSearch.timeLimiterTimeout)

    override val responseTimeout: Duration
        get() = maxOf(customers.responseTimeout, orderSearch.responseTimeout)

    /**
     * The customer with [id], or `null` when the directory does not know it. A 404 is Spring's
     * [HttpClientErrorException.NotFound]; every other failure - another status, a timeout, an open breaker - is
     * left to the caller.
     */
    fun customer(id: String): Customer? =
        try {
            customers.get(Customer::class.java) { it.uri("/v1/customers/{id}", id) }
        } catch (_: HttpClientErrorException.NotFound) {
            null
        }

    /** The orders of [customerId]. The body is serialised to JSON before the exchange starts. */
    fun orders(customerId: String): OrderPage =
        orderSearch.post(OrderPage::class.java, OrderSearch(customerId)) {
            it.uri("/v1/orders/search").contentType(MediaType.APPLICATION_JSON)
        }
}
