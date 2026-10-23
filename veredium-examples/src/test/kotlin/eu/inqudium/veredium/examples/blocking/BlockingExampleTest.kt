package eu.inqudium.veredium.examples.blocking

import eu.inqudium.veredium.examples.Sidecar
import eu.inqudium.veredium.examples.Sidecar.ADA
import eu.inqudium.veredium.examples.Sidecar.ADAS_ORDERS
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource

/**
 * The blocking example as a host runs it: the whole Boot application, its adapter calling the stand-in sidecar.
 *
 * Both examples' contexts run in one test JVM, so neither joins Micrometer's global registry: the second one would
 * join it after the first had registered meters there, and Micrometer warns about that.
 */
@SpringBootTest(classes = [BlockingExampleApplication::class], properties = ["management.metrics.use-global-registry=false"])
class BlockingExampleTest {
    @Autowired
    private lateinit var adapter: BlockingCustomerAdapter

    @BeforeEach
    fun stubBackends() = Sidecar.reset()

    @Test
    fun `should read a customer through the sidecar, addressed by the host of its triplet`() {
        // What is tested: BlockingCustomerAdapter.customer for a customer the directory knows.
        // Success criteria: the customer is decoded from the answer; the stub only answers a request whose Host
        //   header is the triplet's host, so the answer also proves the routing header.
        // Why it matters: it is the basic call of the example - the scan, the configuration and one GET.
        //
        // When
        val customer = adapter.customer("c-1")

        // Then
        assertThat(customer).isEqualTo(ADA)
    }

    @Test
    fun `should answer an unknown customer with null instead of an exception`() {
        // What is tested: BlockingCustomerAdapter.customer for an id the directory answers with 404.
        // Success criteria: the call returns null.
        // Why it matters: it shows the one failure the adapter turns into a value; the response contract makes
        //   every non-2xx an exception, and an adapter decides which of them its callers see.
        //
        // When / Then
        assertThat(adapter.customer("c-404")).isNull()
    }

    @Test
    fun `should search the orders with a JSON body`() {
        // What is tested: BlockingCustomerAdapter.orders.
        // Success criteria: the orders of the stub arrive; the stub only answers the expected JSON body and Host.
        // Why it matters: it is the POST of the example - a body serialised by the client, a page decoded.
        //
        // When
        val page = adapter.orders("c-1")

        // Then
        assertThat(page.orders).isEqualTo(ADAS_ORDERS)
    }

    @Test
    fun `should publish the budgets the mesh ladder derived from the configuration`() {
        // What is tested: the BaseAdapter values of BlockingCustomerAdapter.
        // Success criteria: the response timeout is positive and below the TimeLimiter budget.
        // Why it matters: an orchestrating caller composes its end-to-end budget from these two numbers; the
        //   ladder puts the transport's timeout under the TimeLimiter's, so the transport is what trips first.
        //
        // When
        val response = adapter.responseTimeout
        val budget = adapter.timeLimiterTimeout

        // Then
        assertThat(response).isPositive()
        assertThat(budget).isGreaterThan(response)
    }

    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun sidecar(registry: DynamicPropertyRegistry) = Sidecar.register(registry)
    }
}
