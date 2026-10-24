package eu.inqudium.veredium.examples.decorator

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import eu.inqudium.veredium.examples.ExampleContext
import eu.inqudium.veredium.examples.Sidecar
import eu.inqudium.veredium.examples.Sidecar.ADA
import eu.inqudium.veredium.examples.decorator.CorrelatedCustomerLookup.Companion.REQUEST_ID
import eu.inqudium.veredium.testsupport.capturing
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.util.concurrent.TimeUnit

/**
 * The MDC hand-off of the decorator example: the lines the lookup logs on the decorator's worker carry the caller's
 * correlation key, for the blocking and the asynchronous form, and across a second hop the host bridges itself.
 */
@ExampleContext(DecoratorExampleApplication::class)
class CorrelatedCustomerLookupTest(
    private val lookup: CorrelatedCustomerLookup,
) {
    private val adapterLogger = LoggerFactory.getLogger(DecoratorCustomerAdapter::class.java) as Logger

    @BeforeEach
    fun stubBackends() {
        Sidecar.reset()
    }

    @BeforeEach
    fun clearMdc() {
        MDC.clear()
    }

    @Test
    fun `should tag the line logged on the worker with the caller's request id`() {
        // What is tested: CorrelatedCustomerLookup.customer - the caller puts the key, the adapter logs on the
        //   decorator's worker.
        // Success criteria: the customer is returned; the adapter's line was written on a `legacy-call-` worker and
        //   carries the request id; the caller's MDC is empty again afterwards.
        // Why it matters: the worker is a thread the caller never ran on; without the decorator's copy the line
        //   would have no correlation key, and nothing in the log would tie it to its request.
        //
        // When
        val (customer, line) =
            capturing(adapterLogger) { lines ->
                lookup.customer("req-1", "c-1") to lines.awaitEvents { it.isLookupOf("c-1") }.single { it.isLookupOf("c-1") }
            }

        // Then
        assertThat(customer).isEqualTo(ADA)
        assertThat(line.threadName).startsWith("legacy-call-")
        assertThat(line.mdcPropertyMap).containsEntry(REQUEST_ID, "req-1")
        assertThat(MDC.get(REQUEST_ID)).isNull()
    }

    @Test
    fun `should keep the copy on the worker after the caller removed the key`() {
        // What is tested: CorrelatedCustomerLookup.customerAsync - the key leaves the caller's MDC as soon as the
        //   future is handed out.
        // Success criteria: the future completes with the customer; the worker's line carries the request id.
        // Why it matters: the decorator copies the MDC when the call is decorated, so the asynchronous form needs no
        //   key left in place for as long as the call runs.
        //
        // When
        val (customer, line) =
            capturing(adapterLogger) { lines ->
                val customer = lookup.customerAsync("req-2", "c-1").get(5, TimeUnit.SECONDS)
                customer to lines.awaitEvents { it.isLookupOf("c-1") }.single { it.isLookupOf("c-1") }
            }

        // Then
        assertThat(customer).isEqualTo(ADA)
        assertThat(line.mdcPropertyMap).containsEntry(REQUEST_ID, "req-2")
    }

    @Test
    fun `should carry the request id across the fan-out thread to every worker`() {
        // What is tested: CorrelatedCustomerLookup.customers - two lookups, each called from a fan-out thread of the
        //   host, which hands the caller's MDC on with CallerMdc.with.
        // Success criteria: both results arrive in order (null for the unknown id); both worker lines carry the
        //   request id.
        // Why it matters: the decorator copies the MDC of the thread that calls it; for a thread of the host's own,
        //   that is the second hop the host must bridge, or the lines go out without the key.
        //
        // When
        val (customers, lines) =
            capturing(adapterLogger) { lines ->
                val customers = lookup.customers("req-3", listOf("c-1", "c-404"))
                customers to lines.awaitEvents(count = 2) { it.isLookupOf("c-1") || it.isLookupOf("c-404") }
            }

        // Then
        assertThat(customers).containsExactly(ADA, null)
        assertThat(lines.filter { it.isLookupOf("c-1") || it.isLookupOf("c-404") })
            .hasSize(2)
            .allSatisfy { assertThat(it.mdcPropertyMap).containsEntry(REQUEST_ID, "req-3") }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /** Whether this is the adapter's "Looking up customer" line for [id]. */
    private fun ILoggingEvent.isLookupOf(id: String) = formattedMessage == "Looking up customer '$id'"
}
