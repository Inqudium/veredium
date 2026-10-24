package eu.inqudium.veredium.examples.decorator

import eu.inqudium.veredium.testsupport.ConfigurationFiles
import io.github.resilience4j.bulkhead.BulkheadConfig
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.boot.convert.DurationStyle

/**
 * Contract test for the two invariants between the decorator example's `legacy-directory` block and its Resilience4j
 * instances, bound from the `application.yml` this module ships - the file a host copies and tunes, not the budget
 * override [DecoratorExampleTest] runs with:
 *
 * 1. HttpClient 4's pool holds at least as many connections as the bulkhead admits calls, so a permit always finds a
 *    connection. The queueing stays in the bulkhead, which refuses a call at once with an exception the breaker
 *    ignores, instead of moving into the pool, where the wait ends in a failure the breaker counts.
 * 2. HttpClient 4's socket timeout sits below the TimeLimiter budget, so a silent directory is reported by the
 *    transport, with its own exception, and the budget stays the outer backstop.
 *
 * The Resilience4j values are resolved the way Resilience4j's Boot properties resolve them: the instance's own value,
 * else its `base-config`'s, else the `default` config's, else the library default. No context, no I/O beyond the
 * classpath: `YamlPropertySourceLoader` + `Binder` via [ConfigurationFiles].
 */
class DecoratorConfigContractTest {
    @Test
    fun `should give the legacy directory's pool at least as many connections as its bulkhead has permits`() {
        // What is tested: legacy-directory.max-connections against the bulkhead permits of the legacy-directory
        //   instance, both from the shipped application.yml.
        // Success criteria: max-connections is at least the resolved max-concurrent-calls.
        // Why it matters: a permit that finds no connection waits in the pool and fails after the connection
        //   request timeout - a failure the breaker counts against a healthy directory - where the bulkhead would
        //   have refused the call at once with an exception the breaker ignores.
        //
        // Given
        val directory = legacyDirectory()
        val bulkhead = ConfigurationFiles.bindMap(SHIPPED, "resilience4j.bulkhead")

        // When
        val permits = resolved(bulkhead, "max-concurrent-calls")?.toInt() ?: BulkheadConfig.ofDefaults().maxConcurrentCalls

        // Then
        assertThat(directory.maxConnections)
            .`as`("legacy-directory.max-connections against the bulkhead's %d permits", permits)
            .isGreaterThanOrEqualTo(permits)
    }

    @Test
    fun `should set the legacy directory's socket timeout below its TimeLimiter budget`() {
        // What is tested: legacy-directory.response-timeout against the timeout-duration of the legacy-directory
        //   TimeLimiter instance, both from the shipped application.yml.
        // Success criteria: the response timeout is strictly shorter than the resolved budget.
        // Why it matters: with the socket timeout at or above the budget, a silent directory would always end on
        //   the TimeLimiter's TimeoutException, and the transport's own report of what went wrong would never be
        //   seen - the layering the example teaches would hold in its KDoc only.
        //
        // Given
        val directory = legacyDirectory()
        val timeLimiter = ConfigurationFiles.bindMap(SHIPPED, "resilience4j.timelimiter")

        // When
        val budget =
            resolved(timeLimiter, "timeout-duration")?.let { DurationStyle.detectAndParse(it) }
                ?: TimeLimiterConfig.ofDefaults().timeoutDuration

        // Then
        assertThat(directory.responseTimeout)
            .`as`("legacy-directory.response-timeout against the TimeLimiter budget %s", budget)
            .isLessThan(budget)
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /** The `legacy-directory` block of the shipped file, bound as the application binds it. */
    private fun legacyDirectory(): LegacyDirectoryProperties = ConfigurationFiles.bind(SHIPPED, "legacy-directory", LegacyDirectoryProperties::class.java)

    /** [key] of the call's instance in a flattened `resilience4j.<layer>` block, inherited through its base config. */
    private fun resolved(
        layer: Map<String, String>,
        key: String,
    ): String? {
        val instance = "instances.${DecoratorCustomerAdapter.NAME}"
        val baseConfig = layer["$instance.base-config"] ?: "default"
        return layer["$instance.$key"] ?: layer["configs.$baseConfig.$key"]
    }

    private companion object {
        /** The configuration this module ships in `src/main/resources`. */
        const val SHIPPED = "application.yml"
    }
}
