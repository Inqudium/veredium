package eu.inqudium.veredium.examples.mixed

import eu.inqudium.veredium.core.BaseAdapter
import eu.inqudium.veredium.examples.Backends
import eu.inqudium.veredium.examples.Customer
import eu.inqudium.veredium.examples.OrderPage
import eu.inqudium.veredium.examples.OrderSearch
import eu.inqudium.veredium.restclient.ResilientRestClientFactory
import eu.inqudium.veredium.webclient.ResilientWebClientFactory
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.client.HttpClientErrorException
import reactor.core.publisher.Mono
import java.time.Duration

/**
 * An adapter that uses both clients, each for the backend its stack owns: the customer directory blocking, through
 * [ResilientRestClientFactory], the order search reactive, through [ResilientWebClientFactory]. Each factory serves
 * only its own stack's endpoints - asked for the other's, it refuses and names the owner.
 *
 * Its [BaseAdapter] budgets are the ceiling over both clients, as in the single-stack adapters: both stacks derive
 * them from the same ladder, so the two values compare across stacks.
 */
@Component
class MixedCustomerAdapter(
    restClients: ResilientRestClientFactory,
    webClients: ResilientWebClientFactory,
) : BaseAdapter {
    private val customers = restClients.create(Backends.CUSTOMERS)
    private val orderSearch = webClients.create(Backends.ORDER_SEARCH)

    /** The pessimistic ceiling over both clients, as in the blocking adapter. */
    override val timeLimiterTimeout: Duration
        get() = maxOf(customers.timeLimiterTimeout, orderSearch.timeLimiterTimeout)

    override val responseTimeout: Duration
        get() = maxOf(customers.responseTimeout, orderSearch.responseTimeout)

    /** The customer with [id], or `null` for a 404 - a blocking call, as in the blocking example. */
    fun customer(id: String): Customer? =
        try {
            customers.get(Customer::class.java) { it.uri("/v1/customers/{id}", id) }
        } catch (_: HttpClientErrorException.NotFound) {
            null
        }

    /** The orders of [customerId] - a cold [Mono], as in the reactive example. */
    fun orders(customerId: String): Mono<OrderPage> =
        orderSearch.post(OrderPage::class.java, OrderSearch(customerId)) {
            it.uri("/v1/orders/search").contentType(MediaType.APPLICATION_JSON)
        }
}
