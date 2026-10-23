package eu.inqudium.veredium.examples.async

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.okJson
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import eu.inqudium.veredium.examples.CustomerOverview
import eu.inqudium.veredium.examples.Sidecar
import eu.inqudium.veredium.examples.Sidecar.ADA
import eu.inqudium.veredium.examples.Sidecar.ADAS_ORDERS
import eu.inqudium.veredium.restclient.ResilientRestClient
import eu.inqudium.veredium.testsupport.CapturingAppender
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.util.concurrent.TimeUnit

/**
 * The asynchronous example as a host runs it: the whole Boot application, its adapter calling the stand-in sidecar.
 *
 * Like the other examples' contexts it stays off Micrometer's global registry, which it would join after another
 * context had registered meters there.
 */
@SpringBootTest(classes = [AsyncExampleApplication::class], properties = ["management.metrics.use-global-registry=false"])
class AsyncExampleTest {
    @Autowired
    private lateinit var adapter: AsyncCustomerAdapter

    @BeforeEach
    fun stubBackends() = Sidecar.reset()

    @Test
    fun `should deliver a customer on the future, and null for an unknown one`() {
        // What is tested: AsyncCustomerAdapter.customer for a known id and for one answered with 404.
        // Success criteria: the first future completes with the customer, the second with null, both within 5 s.
        // Why it matters: it shows the asynchronous form of the response contract - the 404 arrives on the future
        //   as the exception the blocking call throws, and the adapter maps it there.
        //
        // When
        val known = adapter.customer("c-1").get(5, TimeUnit.SECONDS)
        val unknown = adapter.customer("c-404").get(5, TimeUnit.SECONDS)

        // Then
        assertThat(known).isEqualTo(ADA)
        assertThat(unknown).isNull()
    }

    @Test
    fun `should combine both calls into one overview on one future`() {
        // What is tested: AsyncCustomerAdapter.overview.
        // Success criteria: the future completes within 5 s with the customer and the orders.
        // Why it matters: it shows two calls in flight at once, composed with the CompletableFuture API.
        //
        // When
        val overview = adapter.overview("c-1").get(5, TimeUnit.SECONDS)

        // Then
        assertThat(overview).isEqualTo(CustomerOverview(ADA, ADAS_ORDERS))
    }

    @Test
    fun `should abandon the running call when the caller cancels the combined future`() {
        // What is tested: cancelling the future of overview while the customer directory has not answered yet.
        // Success criteria: the client logs the cancellation of the customers call within 5 s.
        // Why it matters: a derived future does not pass a cancel on by itself; without the adapter handing it on,
        //   the exchange would run until its budget expired and hold its bulkhead permit all the while.
        //
        // Given: a customer the directory answers only after the test is long over
        Sidecar.server.stubFor(
            get(urlEqualTo("/v1/customers/c-slow")).willReturn(okJson("""{"id":"c-slow","name":"Slow"}""").withFixedDelay(30_000)),
        )
        val logger = LoggerFactory.getLogger(ResilientRestClient::class.java) as Logger
        val level = logger.level
        val appender = CapturingAppender { it.formattedMessage.startsWith("Cancelled request to backend 'customers'") }.apply { start() }
        logger.level = Level.DEBUG
        logger.addAppender(appender)
        try {
            // When
            adapter.overview("c-slow").cancel(true)

            // Then
            assertThat(appender.awaitEvents()).hasSize(1)
        } finally {
            logger.detachAppender(appender)
            logger.level = level
        }
    }

    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun sidecar(registry: DynamicPropertyRegistry) = Sidecar.register(registry)
    }
}
