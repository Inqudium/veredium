package eu.inqudium.veredium.core.resilience

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import eu.inqudium.veredium.core.mesh.MeshProperties
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.common.bulkhead.configuration.CommonBulkheadConfigurationProperties
import io.github.resilience4j.common.circuitbreaker.configuration.CommonCircuitBreakerConfigurationProperties
import io.github.resilience4j.common.timelimiter.configuration.CommonTimeLimiterConfigurationProperties
import io.github.resilience4j.spring6.bulkhead.configure.BulkheadConfigurationProperties
import io.github.resilience4j.spring6.circuitbreaker.configure.CircuitBreakerConfigurationProperties
import io.github.resilience4j.spring6.timelimiter.configure.TimeLimiterConfigurationProperties
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.time.Duration

/**
 * Tests for [MeshResilienceStartupGuard] against `Registry.of(...)` registries and hand-filled Resilience4j
 * property objects - no Spring needed, the guard's check is a plain method.
 *
 * The guard exists for the two failures the customizer mechanism has by design: an alias nobody declared under
 * `resilience4j.<layer>.instances` still gets an instance (Resilience4j creates one for the customizer, on the
 * registry default rather than on the operator's base config), and a registry built by hand has every alias
 * but never saw a customizer. Both are silent. Every test here is therefore about VISIBILITY - that the
 * omission ends the start and the message names the key - rather than about the knobs themselves.
 */
class MeshResilienceStartupGuardTest {
    private val alias = "api-example-com-GET-accounts"
    private val triplet = "/endpoint/api.example.com/GET/accounts/{accountId}"
    private val secondAlias = "api-example-com-POST-payments"
    private val secondTriplet = "/endpoint/api.example.com/POST/payments"

    private val guardLogger = LoggerFactory.getLogger(MeshResilienceStartupGuard::class.java) as Logger
    private val appender =
        ListAppender<ILoggingEvent>().apply {
            start()
            guardLogger.addAppender(this)
        }

    @AfterEach
    fun detachAppender() {
        guardLogger.detachAppender(appender)
        appender.stop()
    }

    @Nested
    inner class Passing {
        @Test
        fun `should pass when every alias is declared and the customizers reached their instances`() {
            // What is tested: the healthy case - all three layers declare the alias, the instances exist, and the
            //   breaker and time limiter carry the customizer's effect.
            // How success is judged: the check throws nothing and writes the three startup lines an operator
            //   reads: the two policy lines naming the alias, and the per-endpoint decision line.
            // Why it matters: a guard that failed the healthy start would block every deployment; the log lines
            //   are the only evidence at runtime that the verdicts and the budget are in force, and for WHICH
            //   aliases.

            // Given: properties and registries the way the Boot configuration leaves them
            val props = propsFor()
            val guard = guardOver(props, wired(props))

            // When / Then
            assertThatCode { guard.verify() }.doesNotThrowAnyException()
            val lines = appender.list.filter { it.level == Level.INFO }.map { it.formattedMessage }
            assertThat(lines).hasSize(3)
            assertThat(lines).allSatisfy { line -> assertThat(line).contains(alias) }
            assertThat(lines.single { it.contains("mesh backends") }).contains("timeLimiter=on (1800ms)", "circuitBreaker=off (untouched)")
        }

        @Test
        fun `should name the benign-4xx policy without the client-side failure types in its line`() {
            // What is tested: that the INFO line of the benign-4xx policy names the aliases it was applied to and
            //   NOT the client-side failure types - those belong to the sibling policy's line.
            // How success is judged: the line contains both aliases and none of the failure type names.
            // Why it matters: this line is the startup evidence an operator reads to confirm WHICH backends are
            //   covered by WHICH rule. The reactive stack once filled the single placeholder with the sibling's
            //   type list and silently dropped the alias; a placeholder/argument mismatch neither fails nor warns.

            // Given: two endpoints
            val props = propsFor(secondTriplet)
            val guard = guardOver(props, wired(props))

            // When
            guard.verify()

            // Then
            val line = appender.list.map { it.formattedMessage }.single { it.contains("benign 4xx") }
            assertThat(line).contains(alias, secondAlias)
            assertThat(line).doesNotContain("BulkheadFullException", "TestBufferLimitException")
        }

        @Test
        fun `should not check the budget of an endpoint whose time-limiter derivation is off`() {
            // What is tested: a disabled derivation leaves the YAML budget alone, so the guard must accept
            //   whatever the instance carries - here 7s instead of the ladder's 1800ms.
            // Why it matters: a guard that demanded the derived budget regardless of the toggle would make the
            //   opt-out unusable.

            // Given: time-limiter derivation off, a 7s instance
            val props =
                MeshProperties(
                    defaults = MeshProperties.Defaults(individualRequestTimeoutMs = 1000),
                    endpoints =
                        listOf(
                            MeshProperties.EndpointConfig(triplet, timeLimiter = MeshProperties.BackendToggleOverride(enabled = false)),
                        ),
                )
            val wiring = wired(props)
            wiring.timeLimiters.remove(alias)
            wiring.timeLimiters.timeLimiter(alias, TimeLimiterConfig.custom().timeoutDuration(Duration.ofSeconds(7)).build())

            // When / Then
            assertThatCode { guardOver(props, wiring).verify() }.doesNotThrowAnyException()
        }
    }

    @Nested
    inner class Undeclared {
        @Test
        fun `should fail naming the key when the time limiter of an alias is not declared`() {
            // What is tested: the silent case the guard exists for - the alias has no
            //   `resilience4j.timelimiter.instances.<alias>` entry, although the registry DOES hold an instance
            //   (as it does in the context, where Resilience4j creates one for the customizer on the default).
            // How success is judged: the check throws, and the message names the exact property key to add -
            //   as the only problem, so the registry's instance is not mistaken for a second one.
            // Why it matters: nothing else notices. The instance works, the derived budget is on it, and only
            //   the operator's base config (cancelRunningFuture, the 3s fallback) is silently gone.

            // Given: everything wired, then the declaration removed - the registry instance stays
            val props = propsFor()
            val wiring = wired(props)
            wiring.timeLimiterProperties.instances.remove(alias)

            // When / Then
            assertThatThrownBy { guardOver(props, wiring).verify() }
                .isInstanceOf(IllegalStateException::class.java)
                .hasMessageContaining("resilience4j.timelimiter.instances.$alias is not declared")
                .hasMessageContaining("1 problem(s)")
        }

        @Test
        fun `should fail when the bulkhead of an alias is not declared`() {
            // The bulkhead has no customizer, so an undeclared one is created on the first call from the default
            // (25 permits, no wait) - just as silently. Declared or not is read off the properties like the others.
            val props = propsFor()
            val wiring = wired(props)
            wiring.bulkheadProperties.instances.remove(alias)
            wiring.bulkheads.remove(alias)

            assertThatThrownBy { guardOver(props, wiring).verify() }
                .hasMessageContaining("resilience4j.bulkhead.instances.$alias is not declared")
                .hasMessageContaining("1 problem(s)")
        }

        @Test
        fun `should report every problem of every endpoint in one failure`() {
            // What is tested: problems are COLLECTED, not thrown at the first one.
            // How success is judged: with two endpoints, nothing declared and nothing in the registries, one
            //   exception names all six missing declarations and the four missing instances.
            // Why it matters: an operator fixing a deployment should see the whole list once, not one key per
            //   restart.

            // Given: two endpoints, empty properties and registries
            val props = propsFor(secondTriplet)
            val guard = guardOver(props, Wiring())

            // When / Then
            assertThatThrownBy { guard.verify() }
                .hasMessageContaining("10 problem(s)")
                .hasMessageContaining("resilience4j.circuitbreaker.instances.$alias")
                .hasMessageContaining("resilience4j.timelimiter.instances.$alias")
                .hasMessageContaining("resilience4j.bulkhead.instances.$alias")
                .hasMessageContaining("resilience4j.circuitbreaker.instances.$secondAlias")
                .hasMessageContaining("resilience4j.timelimiter.instances.$secondAlias")
                .hasMessageContaining("resilience4j.bulkhead.instances.$secondAlias")
                .hasMessageContaining("the circuitbreaker '$alias' has no instance in the registry")
                .hasMessageContaining("the timelimiter '$secondAlias' has no instance in the registry")
        }
    }

    @Nested
    inner class Unreached {
        @Test
        fun `should fail when the breaker exists but did not receive the verdicts`() {
            // What is tested: the second silent case - a registry on which no customizer ever ran, as a host or a
            //   test gets when it builds the registry bean by hand instead of through the auto-configuration.
            //   Every alias is declared and present, so the declaration check alone would pass.
            // How success is judged: the check throws and names the breaker as not reached.
            // Why it matters: this is the case where the metrics look like nobody chose the values - because
            //   nobody did.

            // Given: a breaker under the alias, on the registry default
            val props = propsFor()
            val wiring = wired(props)
            wiring.circuitBreakers.remove(alias)
            wiring.circuitBreakers.circuitBreaker(alias)

            // When / Then
            assertThatThrownBy { guardOver(props, wiring).verify() }
                .hasMessageContaining("the circuitbreaker '$alias' does not ignore a client-side failure")
                .hasMessageContaining("the circuitbreaker '$alias' records a benign 4xx")
                .hasMessageContaining("did not reach it")
        }

        @Test
        fun `should fail when the time limiter exists but carries another budget`() {
            // Given: a 9s instance where the ladder says 1800ms, derivation on
            val props = propsFor()
            val wiring = wired(props)
            wiring.timeLimiters.remove(alias)
            wiring.timeLimiters.timeLimiter(alias, TimeLimiterConfig.custom().timeoutDuration(Duration.ofSeconds(9)).build())

            // When / Then
            assertThatThrownBy { guardOver(props, wiring).verify() }
                .hasMessageContaining("the timelimiter '$alias' has timeoutDuration=9000ms instead of the derived 1800ms")
        }

        @Test
        fun `should fail when a declared bulkhead has no instance in the registry`() {
            // A declared bulkhead the registry does not hold means the registry was not built from the properties
            // - the same hand-made-registry case as above, seen on the one layer without a customizer.
            val props = propsFor()
            val wiring = wired(props)
            wiring.bulkheads.remove(alias)

            assertThatThrownBy { guardOver(props, wiring).verify() }
                .hasMessageContaining("the bulkhead '$alias' has no instance in the registry")
                .hasMessageContaining("1 problem(s)")
        }

        @Test
        fun `should check the derived slow-call only when the circuit-breaker derivation is enabled`() {
            // Given: derivation enabled, but the breaker carries the verdicts and NOT the derived threshold
            val props = propsFor(circuitBreakerEnabled = true)
            val wiring = wired(propsFor()) // customized with the derivation OFF: verdicts present, knobs untouched

            // When / Then: the one thing missing is named
            assertThatThrownBy { guardOver(props, wiring).verify() }
                .hasMessageContaining("1 problem(s)")
                .hasMessageContaining("has slowCallDurationThreshold=60000ms instead of the derived 800ms")
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /** What the guard reads: the three bound property objects and the three registries. All empty by default. */
    private class Wiring {
        val circuitBreakerProperties = CircuitBreakerConfigurationProperties()
        val timeLimiterProperties = TimeLimiterConfigurationProperties()
        val bulkheadProperties = BulkheadConfigurationProperties()
        val circuitBreakers: CircuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
        val timeLimiters: TimeLimiterRegistry = TimeLimiterRegistry.ofDefaults()
        val bulkheads: BulkheadRegistry = BulkheadRegistry.ofDefaults()

        /** Declares [alias] under all three layers, the way the YAML's `instances.<alias>` entries bind. */
        fun declare(alias: String) {
            circuitBreakerProperties.instances[alias] = CommonCircuitBreakerConfigurationProperties.InstanceProperties()
            timeLimiterProperties.instances[alias] = CommonTimeLimiterConfigurationProperties.InstanceProperties()
            bulkheadProperties.instances[alias] = CommonBulkheadConfigurationProperties.InstanceProperties()
        }
    }

    /** Every alias of [props] declared, present and customized, as the Boot configuration leaves them. */
    private fun wired(props: MeshProperties): Wiring {
        val wiring = Wiring()
        props.resolveAll().values.forEach { endpoint ->
            wiring.declare(endpoint.alias)
            wiring.bulkheads.bulkhead(endpoint.alias)
        }
        CustomizedRegistries.circuitBreakers(wiring.circuitBreakers, props)
        CustomizedRegistries.timeLimiters(wiring.timeLimiters, props)
        return wiring
    }

    private fun guardOver(
        props: MeshProperties,
        wiring: Wiring,
    ): MeshResilienceStartupGuard =
        MeshResilienceStartupGuard(
            props,
            TestVerdicts.breakerVerdicts,
            wiring.circuitBreakerProperties,
            wiring.timeLimiterProperties,
            wiring.bulkheadProperties,
            wiring.circuitBreakers,
            wiring.timeLimiters,
            wiring.bulkheads,
        )

    /**
     * Props for the accounts endpoint and any [more] triplets; individual=1000 at maxRetries=0 derives
     * timeLimiter=1800ms and slowCall=800ms.
     */
    private fun propsFor(
        vararg more: String,
        circuitBreakerEnabled: Boolean = false,
    ): MeshProperties =
        MeshProperties(
            defaults =
                MeshProperties.Defaults(
                    individualRequestTimeoutMs = 1000,
                    circuitBreaker = MeshProperties.BackendToggle(enabled = circuitBreakerEnabled),
                ),
            endpoints = (listOf(triplet) + more).map { MeshProperties.EndpointConfig(it) },
        )
}
