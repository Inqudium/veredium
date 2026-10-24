package eu.inqudium.veredium.examples.reactive

import eu.inqudium.veredium.core.BaseAdapter
import eu.inqudium.veredium.examples.Backends
import eu.inqudium.veredium.examples.Customer
import eu.inqudium.veredium.examples.CustomerOverview
import eu.inqudium.veredium.examples.OrderPage
import eu.inqudium.veredium.examples.OrderSearch
import eu.inqudium.veredium.webclient.ResilientWebClientFactory
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono
import java.time.Duration

/**
 * The adapter of the blocking example on the reactive client: the same two backends, the same aliases, the same
 * budgets - only the calling convention differs. Every call returns a cold [Mono]; nothing goes on the wire until
 * it is subscribed, and a failure arrives as its error signal, never as an exception out of the method.
 *
 * [customerOrNull] shows the coroutine form, for a host that writes suspending code.
 */
@Component
class ReactiveCustomerAdapter(
    factory: ResilientWebClientFactory,
) : BaseAdapter {
    private val customers = factory.create(Backends.CUSTOMERS)
    private val orderSearch = factory.create(Backends.ORDER_SEARCH)

    /** The pessimistic ceiling over both clients: what one call of this adapter may take at most. */
    override val timeLimiterTimeout: Duration
        get() = maxOf(customers.timeLimiterTimeout, orderSearch.timeLimiterTimeout)

    override val responseTimeout: Duration
        get() = maxOf(customers.responseTimeout, orderSearch.responseTimeout)

    /** The customer with [id]; empty when the directory answers 404. Every other failure stays an error signal. */
    fun customer(id: String): Mono<Customer> =
        customers
            .get(Customer::class.java) { it.uri("/v1/customers/{id}", id) }
            .onErrorResume(WebClientResponseException.NotFound::class.java) { Mono.empty() }

    /** The orders of [customerId]. */
    fun orders(customerId: String): Mono<OrderPage> =
        orderSearch.post(OrderPage::class.java, OrderSearch(customerId)) {
            it.uri("/v1/orders/search").contentType(MediaType.APPLICATION_JSON)
        }

    /**
     * The customer with [id] and their orders, both subscribed at once. `zip` cancels the other call when one
     * fails, and is empty when the customer is unknown.
     */
    fun overview(id: String): Mono<CustomerOverview> = Mono.zip(customer(id), orders(id)) { found, page -> CustomerOverview(found, page.orders) }

    /** [customer] from a coroutine: suspends until the answer is there, `null` for an unknown customer. */
    suspend fun customerOrNull(id: String): Customer? = customer(id).awaitSingleOrNull()
}
