package eu.inqudium.veredium.examples.decorator

import eu.inqudium.veredium.core.BaseAdapter
import eu.inqudium.veredium.core.resilience.BlockingResilienceDecorator
import eu.inqudium.veredium.examples.Customer
import org.apache.http.HttpHeaders
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.util.EntityUtils
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper
import java.time.Duration
import java.util.concurrent.CompletableFuture

/** The directory answered that it does not know the customer: a valid answer, which the breaker ignores. */
class UnknownCustomerException(
    id: String,
) : RuntimeException("no customer '$id'")

/** The directory answered with a status other than 200 and 404: a fault of the backend, which the breaker counts. */
class DirectoryFailureException(
    status: Int,
) : RuntimeException("the customer directory answered $status")

/**
 * A customer lookup over a legacy client the library knows nothing about - a native Apache HttpClient 4 - bounded
 * by [BlockingResilienceDecorator]: the call runs on a virtual worker thread inside
 * `CircuitBreaker( TimeLimiter( Bulkhead( call ) ) )`, configured under `resilience4j.*.instances.legacy-directory`.
 *
 * The decorator adds what the client cannot do itself: a hard budget for the whole call, after which the caller is
 * released with a `TimeoutException` and the worker interrupted. On a virtual thread the interrupt closes the
 * socket HttpClient 4 is blocked on, so the worker ends at once and its permit and connection come back.
 *
 * [customer] blocks the caller; [customerAsync] returns at once, and cancelling its future abandons the call the
 * same way.
 */
@Component
class LegacyCustomerDirectory(
    private val decorator: BlockingResilienceDecorator,
    private val httpClient: CloseableHttpClient,
    private val objectMapper: ObjectMapper,
    private val properties: LegacyDirectoryProperties,
) : BaseAdapter {
    /** The TimeLimiter budget configured for the call. */
    override val timeLimiterTimeout: Duration get() = decorator.requestBudget(NAME)

    /** HttpClient 4's socket timeout, the bound on each read underneath the budget. */
    override val responseTimeout: Duration get() = properties.responseTimeout

    /**
     * The customer with [id], or `null` when the directory does not know it. Every other outcome is an exception:
     * `TimeoutException` past the budget, `CallNotPermittedException` with the breaker open,
     * `BulkheadFullException` with every permit taken, and the client's own failures.
     */
    fun customer(id: String): Customer? =
        try {
            decorator.decorate(NAME) { lookup(id) }
        } catch (_: UnknownCustomerException) {
            null
        }

    /**
     * [customer] without the wait. The future fails with [UnknownCustomerException] for an unknown customer; it is
     * the decorator's own future, so cancelling it reaches the call.
     */
    fun customerAsync(id: String): CompletableFuture<Customer> = decorator.decorateAsync(NAME) { lookup(id) }

    /** The exchange itself, on the worker thread. */
    private fun lookup(id: String): Customer {
        val uri =
            URIBuilder(properties.baseUrl)
                .setPathSegments("v1", "customers", id)
                .build()
        val request =
            HttpGet(uri).apply {
                setHeader(HttpHeaders.HOST, properties.host)
                setHeader(HttpHeaders.ACCEPT, "application/json")
            }
        // The response handler form releases the connection whatever the handler does.
        return httpClient.execute(request) { response ->
            when (val status = response.statusLine.statusCode) {
                200 -> objectMapper.readValue(EntityUtils.toString(response.entity, Charsets.UTF_8), Customer::class.java)
                404 -> throw UnknownCustomerException(id)
                else -> throw DirectoryFailureException(status)
            }
        }
    }

    companion object {
        /** The name of the call's Resilience4j instances. */
        const val NAME = "legacy-directory"
    }
}
