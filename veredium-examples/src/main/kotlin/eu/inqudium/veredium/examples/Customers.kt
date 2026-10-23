package eu.inqudium.veredium.examples

/**
 * The payloads the examples exchange with their two backends: a customer directory, read by id, and an order
 * search, queried by a POSTed body. Plain data classes, (de)serialised by Jackson. That a required property the
 * backend leaves out fails the call instead of arriving as `null` needs Jackson's Kotlin module
 * (`jackson-module-kotlin`) on the classpath: the reactive client brings it, a host with only the blocking client
 * or the decorator declares it itself, as this module's pom does.
 */
data class Customer(
    val id: String,
    val name: String,
)

/** The body of an order search. */
data class OrderSearch(
    val customerId: String,
    val limit: Int = 20,
)

/** One order of an [OrderPage]. */
data class Order(
    val id: String,
    val total: String,
)

/** What an order search answers. */
data class OrderPage(
    val orders: List<Order>,
)

/** A customer together with their orders - what an adapter composes from its two backends. */
data class CustomerOverview(
    val customer: Customer,
    val orders: List<Order>,
)

/**
 * The mesh aliases of the two backends, as `application.yml` declares them: the key of a client
 * (`factory.create(alias)`), of its Resilience4j instances and of its metrics.
 */
object Backends {
    const val CUSTOMERS = "customers"
    const val ORDER_SEARCH = "order-search"
}
