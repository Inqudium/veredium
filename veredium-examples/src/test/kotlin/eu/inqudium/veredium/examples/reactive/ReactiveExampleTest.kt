package eu.inqudium.veredium.examples.reactive

import eu.inqudium.veredium.examples.CustomerOverview
import eu.inqudium.veredium.examples.Sidecar
import eu.inqudium.veredium.examples.Sidecar.ADA
import eu.inqudium.veredium.examples.Sidecar.ADAS_ORDERS
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.time.Duration

/**
 * The reactive example as a host runs it: the whole Boot application, its adapter calling the stand-in sidecar.
 *
 * Both examples' contexts run in one test JVM, so neither joins Micrometer's global registry: the second one would
 * join it after the first had registered meters there, and Micrometer warns about that.
 */
@SpringBootTest(classes = [ReactiveExampleApplication::class], properties = ["management.metrics.use-global-registry=false"])
class ReactiveExampleTest {
    @Autowired
    private lateinit var adapter: ReactiveCustomerAdapter

    @BeforeEach
    fun stubBackends() = Sidecar.reset()

    @Test
    fun `should read a customer through the sidecar, addressed by the host of its triplet`() {
        // What is tested: ReactiveCustomerAdapter.customer, subscribed.
        // Success criteria: the Mono emits the customer; the stub only answers the triplet's Host header.
        // Why it matters: it is the basic call of the example on the reactive stack.
        //
        // When
        val customer = adapter.customer("c-1").block(WAIT)

        // Then
        assertThat(customer).isEqualTo(ADA)
    }

    @Test
    fun `should complete an unknown customer empty, and as null from a coroutine`() {
        // What is tested: ReactiveCustomerAdapter.customer and customerOrNull for an id answered with 404.
        // Success criteria: the Mono completes without a value; the suspending form returns null.
        // Why it matters: it shows the one error signal the adapter turns into absence, in both conventions.
        //
        // When
        val empty = adapter.customer("c-404").blockOptional(WAIT)
        val suspended = runBlocking { adapter.customerOrNull("c-404") }

        // Then
        assertThat(empty).isEmpty()
        assertThat(suspended).isNull()
    }

    @Test
    fun `should zip both calls into one overview`() {
        // What is tested: ReactiveCustomerAdapter.overview.
        // Success criteria: the Mono emits the customer with the orders.
        // Why it matters: it shows two calls subscribed at once and composed with Reactor's own operators.
        //
        // When
        val overview = adapter.overview("c-1").block(WAIT)

        // Then
        assertThat(overview).isEqualTo(CustomerOverview(ADA, ADAS_ORDERS))
    }

    @Test
    fun `should publish the same budgets as the blocking adapter`() {
        // What is tested: the BaseAdapter values of ReactiveCustomerAdapter.
        // Success criteria: the response timeout is positive and below the TimeLimiter budget.
        // Why it matters: both stacks derive the budgets from the same ladder, so a host composes them from one
        //   type whichever stack an adapter uses.
        //
        // When
        val response = adapter.responseTimeout
        val budget = adapter.timeLimiterTimeout

        // Then
        assertThat(response).isPositive()
        assertThat(budget).isGreaterThan(response)
    }

    companion object {
        private val WAIT: Duration = Duration.ofSeconds(5)

        @JvmStatic
        @DynamicPropertySource
        fun sidecar(registry: DynamicPropertyRegistry) = Sidecar.register(registry)
    }
}
