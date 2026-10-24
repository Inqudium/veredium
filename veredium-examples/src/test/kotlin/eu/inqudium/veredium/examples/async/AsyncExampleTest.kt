package eu.inqudium.veredium.examples.async

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import com.github.tomakehurst.wiremock.client.WireMock.equalToJson
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.okJson
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.serverError
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import eu.inqudium.veredium.examples.CustomerOverview
import eu.inqudium.veredium.examples.ExampleContext
import eu.inqudium.veredium.examples.Sidecar
import eu.inqudium.veredium.examples.Sidecar.ADA
import eu.inqudium.veredium.examples.Sidecar.ADAS_ORDERS
import eu.inqudium.veredium.restclient.ResilientRestClient
import eu.inqudium.veredium.testsupport.capturing
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.web.client.HttpServerErrorException
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit

/** The asynchronous example as a host runs it: the whole Boot application, its adapter calling the stand-in sidecar. */
@ExampleContext(AsyncExampleApplication::class)
class AsyncExampleTest(
    private val adapter: AsyncCustomerAdapter,
) {
    @BeforeEach
    fun stubBackends() {
        Sidecar.reset()
    }

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
        // What is tested: cancelling the future of overview while neither the customer directory nor the order
        //   search has answered yet.
        // Success criteria: the client logs the cancellation of the customers call within 5 s.
        // Why it matters: a derived future does not pass a cancel on by itself; without the adapter handing it on,
        //   the exchange would run until its budget expired and hold its bulkhead permit all the while. Both
        //   backends answer only after the test is long over, so the caller's cancel is the only thing that can end
        //   the combined future before the budgets do. A failing call ends it by itself: the next two tests.
        //
        // Given: a customer the directory, and orders the search, answer only after the test is long over
        stubSlowCustomer()
        Sidecar.server.stubFor(
            post(urlEqualTo("/v1/orders/search"))
                .withRequestBody(equalToJson("""{"customerId":"c-slow","limit":20}"""))
                .willReturn(okJson("""{"orders":[]}""").withFixedDelay(30_000)),
        )
        val logger = LoggerFactory.getLogger(ResilientRestClient::class.java) as Logger
        capturing(logger, Level.DEBUG, accept = { it.formattedMessage.startsWith("Cancelled request to backend 'customers'") }) { appender ->
            // When
            adapter.overview("c-slow").cancel(true)

            // Then
            assertThat(appender.awaitEvents()).hasSize(1)
        }
    }

    @Test
    fun `should fail the overview on a failing order search at once, and abandon the customers call`() {
        // What is tested: overview while the order search answers 500 at once and the customer directory has not
        //   answered yet.
        // Success criteria: the future fails with the order search's 500, and the client logs the cancellation of
        //   the customers call - a cancel only the adapter can have sent, since the test cancels nothing.
        // Why it matters: thenCombine waits for both inputs even after one has failed. Without the adapter failing
        //   fast, the future stays open until the customers call ends on its TimeLimiter budget, fails with THAT
        //   timeout instead of the 500, and the cancel comes too late to be logged.
        //
        // Given: a search that fails at once, and a customer the directory answers only after the test is long over
        stubSlowCustomer()
        Sidecar.server.stubFor(
            post(urlEqualTo("/v1/orders/search"))
                .withRequestBody(equalToJson("""{"customerId":"c-slow","limit":20}"""))
                .willReturn(serverError()),
        )

        // When / Then
        assertFailsWithAndCancels(backend = "customers")
    }

    @Test
    fun `should fail the overview on a failing customer directory at once, and abandon the order search`() {
        // What is tested: overview while the customer directory answers 500 at once and the order search has not
        //   answered yet - the other direction of the previous test.
        // Success criteria: the future fails with the directory's 500, and the client logs the cancellation of the
        //   order-search call.
        // Why it matters: the customers future is a derived one (customer maps the 404), the orders future the
        //   client's own; failing fast has to work from either side.
        //
        // Given: a directory that fails at once, and orders the search answers only after the test is long over
        Sidecar.server.stubFor(get(urlEqualTo("/v1/customers/c-slow")).willReturn(serverError()))
        Sidecar.server.stubFor(
            post(urlEqualTo("/v1/orders/search"))
                .withRequestBody(equalToJson("""{"customerId":"c-slow","limit":20}"""))
                .willReturn(okJson("""{"orders":[]}""").withFixedDelay(30_000)),
        )

        // When / Then
        assertFailsWithAndCancels(backend = "order-search")
    }

    private fun stubSlowCustomer() {
        Sidecar.server.stubFor(
            get(urlEqualTo("/v1/customers/c-slow")).willReturn(okJson("""{"id":"c-slow","name":"Slow"}""").withFixedDelay(30_000)),
        )
    }

    /** Runs overview for `c-slow` and checks that it fails with a 500 and the client cancels the call to [backend]. */
    private fun assertFailsWithAndCancels(backend: String) {
        val logger = LoggerFactory.getLogger(ResilientRestClient::class.java) as Logger
        capturing(logger, Level.DEBUG, accept = { it.formattedMessage.startsWith("Cancelled request to backend '$backend'") }) { appender ->
            // When
            val failure = catchThrowable { adapter.overview("c-slow").get(5, TimeUnit.SECONDS) }

            // Then
            assertThat(failure).isInstanceOf(ExecutionException::class.java).cause().isInstanceOf(HttpServerErrorException.InternalServerError::class.java)
            assertThat(appender.awaitEvents()).hasSize(1)
        }
    }
}
