package eu.inqudium.veredium.core.resilience

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import eu.inqudium.veredium.core.mesh.MeshConnectionPool
import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.resilience.TestMesh.ACCOUNTS_ALIAS
import eu.inqudium.veredium.core.resilience.TestMesh.ACCOUNTS_TRIPLET
import eu.inqudium.veredium.core.resilience.TestMesh.PAYMENTS_ALIAS
import eu.inqudium.veredium.core.resilience.TestMesh.PAYMENTS_TRIPLET
import eu.inqudium.veredium.core.resilience.TestMesh.propsFor
import io.github.resilience4j.bulkhead.BulkheadConfig
import io.github.resilience4j.bulkhead.BulkheadFullException
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
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
    inner class `A healthy wiring` {
        @Test
        fun `should pass when every alias is declared and the customizers reached their instances`() {
            // What is tested: the healthy case - all three layers declare the alias, the instances exist, and the
            //   breaker and time limiter carry the customizer's effect.
            // Success criteria: the check throws nothing and writes the three startup lines an operator
            //   reads: the two policy lines naming the alias, and the per-endpoint decision line.
            // Why it matters: a guard that failed the healthy start would block every deployment; the log lines
            //   are the only evidence at runtime that the verdicts and the budget are in force, and for WHICH
            //   aliases.
            //
            // Given: properties and registries the way the Boot configuration leaves them
            val props = propsFor()
            val guard = guardOver(props, wired(props))

            // When / Then
            assertThatCode { guard.verify() }.doesNotThrowAnyException()
            val lines = appender.list.filter { it.level == Level.INFO }.map { it.formattedMessage }
            assertThat(lines).hasSize(3)
            assertThat(lines).allSatisfy { line -> assertThat(line).contains(ACCOUNTS_ALIAS) }
            assertThat(lines.single { it.contains("mesh backends") }).contains("timeLimiter=on (1800ms)", "circuitBreaker=off (untouched)")
        }

        @Test
        fun `should name the benign-4xx policy without the client-side failure types in its line`() {
            // What is tested: that the INFO line of the benign-4xx policy names the aliases it was applied to and
            //   NOT the client-side failure types - those belong to the sibling policy's line.
            // Success criteria: the line contains both aliases and none of the failure type names.
            // Why it matters: this line is the startup evidence an operator reads to confirm WHICH backends are
            //   covered by WHICH rule. A placeholder filled with the sibling's type list silently drops the alias,
            //   and a placeholder/argument mismatch neither fails nor warns.
            //
            // Given: two endpoints
            val props = propsFor(PAYMENTS_TRIPLET)
            val guard = guardOver(props, wired(props))

            // When
            guard.verify()

            // Then
            val line = appender.list.map { it.formattedMessage }.single { it.contains("benign 4xx") }
            assertThat(line).contains(ACCOUNTS_ALIAS, PAYMENTS_ALIAS)
            assertThat(line).doesNotContain("BulkheadFullException", "TestBufferLimitException")
        }

        @Test
        fun `should not check the budget of an endpoint whose time-limiter derivation is off`() {
            // What is tested: a disabled derivation leaves the YAML budget alone, so the guard must accept
            //   whatever the instance carries - here 7s instead of the ladder's 1800ms.
            // Success criteria: with the endpoint's time-limiter override `enabled = false` and a 7s instance in
            //   the registry, `verify()` throws nothing.
            // Why it matters: a guard that demanded the derived budget regardless of the toggle would make the
            //   opt-out unusable.
            //
            // Given: time-limiter derivation off, a 7s instance
            val props =
                MeshProperties(
                    defaults = MeshProperties.Defaults(individualRequestTimeoutMs = 1000),
                    endpoints =
                        listOf(
                            MeshProperties.EndpointConfig(ACCOUNTS_TRIPLET, timeLimiter = MeshProperties.BackendToggleOverride(enabled = false)),
                        ),
                )
            val wiring = wired(props)
            wiring.timeLimiters.remove(ACCOUNTS_ALIAS)
            wiring.timeLimiters.timeLimiter(ACCOUNTS_ALIAS, TimeLimiterConfig.custom().timeoutDuration(Duration.ofSeconds(7)).build())

            // When / Then
            assertThatCode { guardOver(props, wiring).verify() }.doesNotThrowAnyException()
        }
    }

    @Nested
    inner class `An undeclared instance` {
        @Test
        fun `should fail naming the key when the time limiter of an alias is not declared`() {
            // What is tested: the silent case the guard exists for - the alias has no
            //   `resilience4j.timelimiter.instances.<alias>` entry, although the registry DOES hold an instance
            //   (as it does in the context, where Resilience4j creates one for the customizer on the default).
            // Success criteria: the check throws, and the message names the exact property key to add -
            //   as the only problem, so the registry's instance is not mistaken for a second one.
            // Why it matters: nothing else notices. The instance works, the derived budget is on it, and only
            //   the operator's base config (cancelRunningFuture, the 3s fallback) is silently gone.
            //
            // Given: everything wired, then the declaration removed - the registry instance stays
            val props = propsFor()
            val wiring = wired(props)
            wiring.timeLimiterProperties.instances.remove(ACCOUNTS_ALIAS)

            // When / Then
            assertThatThrownBy { guardOver(props, wiring).verify() }
                .isInstanceOf(IllegalStateException::class.java)
                .hasMessageContaining("resilience4j.timelimiter.instances.$ACCOUNTS_ALIAS is not declared")
                .hasMessageContaining("1 problem(s)")
        }

        @Test
        fun `should fail when the bulkhead of an alias is not declared`() {
            // What is tested: the declaration check on the bulkhead layer - the alias has no
            //   `resilience4j.bulkhead.instances.<alias>` entry (and no registry instance).
            // Success criteria: `verify()` throws naming that key as not declared, as the only problem.
            // Why it matters: the bulkhead has no customizer, so an undeclared one is created on the first call
            //   from the default (25 permits, no wait) - just as silently. Declared or not is read off the
            //   properties like the others.
            //
            // Given: everything wired, then the bulkhead declaration and instance removed
            val props = propsFor()
            val wiring = wired(props)
            wiring.bulkheadProperties.instances.remove(ACCOUNTS_ALIAS)
            wiring.bulkheads.remove(ACCOUNTS_ALIAS)

            // When / Then
            assertThatThrownBy { guardOver(props, wiring).verify() }
                .hasMessageContaining("resilience4j.bulkhead.instances.$ACCOUNTS_ALIAS is not declared")
                .hasMessageContaining("1 problem(s)")
        }

        @Test
        fun `should report every problem of every endpoint in one failure`() {
            // What is tested: problems are COLLECTED, not thrown at the first one.
            // Success criteria: with two endpoints, nothing declared and nothing in the registries, one
            //   exception names all six missing declarations and the four missing instances.
            // Why it matters: an operator fixing a deployment should see the whole list once, not one key per
            //   restart.
            //
            // Given: two endpoints, empty properties and registries
            val props = propsFor(PAYMENTS_TRIPLET)
            val guard = guardOver(props, Wiring())

            // When / Then
            assertThatThrownBy { guard.verify() }
                .hasMessageContaining("10 problem(s)")
                .hasMessageContaining("resilience4j.circuitbreaker.instances.$ACCOUNTS_ALIAS")
                .hasMessageContaining("resilience4j.timelimiter.instances.$ACCOUNTS_ALIAS")
                .hasMessageContaining("resilience4j.bulkhead.instances.$ACCOUNTS_ALIAS")
                .hasMessageContaining("resilience4j.circuitbreaker.instances.$PAYMENTS_ALIAS")
                .hasMessageContaining("resilience4j.timelimiter.instances.$PAYMENTS_ALIAS")
                .hasMessageContaining("resilience4j.bulkhead.instances.$PAYMENTS_ALIAS")
                .hasMessageContaining("the circuitbreaker '$ACCOUNTS_ALIAS' has no instance in the registry")
                .hasMessageContaining("the timelimiter '$PAYMENTS_ALIAS' has no instance in the registry")
        }
    }

    @Nested
    inner class `An instance the customizer did not reach` {
        @Test
        fun `should fail when the breaker exists but did not receive the verdicts`() {
            // What is tested: the second silent case - a registry on which no customizer ever ran, as a host or a
            //   test gets when it builds the registry bean by hand instead of through the auto-configuration.
            //   Every alias is declared and present, so the declaration check alone would pass.
            // Success criteria: the check throws and names the breaker as not reached.
            // Why it matters: this is the case where the metrics look like nobody chose the values - because
            //   nobody did.
            //
            // Given: a breaker under the alias, on the registry default
            val props = propsFor()
            val wiring = wired(props)
            wiring.circuitBreakers.remove(ACCOUNTS_ALIAS)
            wiring.circuitBreakers.circuitBreaker(ACCOUNTS_ALIAS)

            // When / Then
            assertThatThrownBy { guardOver(props, wiring).verify() }
                .hasMessageContaining("the circuitbreaker '$ACCOUNTS_ALIAS' does not ignore a client-side failure")
                .hasMessageContaining("the circuitbreaker '$ACCOUNTS_ALIAS' records a benign 4xx")
                .hasMessageContaining("did not reach it")
        }

        @Test
        fun `should fail when the time limiter exists but carries another budget`() {
            // What is tested: the reach check on the time-limiter layer - the instance is declared and present
            //   but carries 9s while the ladder derives 1800ms with the derivation on.
            // Success criteria: `verify()` throws naming the instance's timeoutDuration=9000ms and the derived
            //   1800ms.
            // Why it matters: a budget other than the derived one breaks the ladder's ordering silently - the
            //   caller is released too late (or too early) and nothing but this check compares the two.
            //
            // Given: a 9s instance where the ladder says 1800ms, derivation on
            val props = propsFor()
            val wiring = wired(props)
            wiring.timeLimiters.remove(ACCOUNTS_ALIAS)
            wiring.timeLimiters.timeLimiter(ACCOUNTS_ALIAS, TimeLimiterConfig.custom().timeoutDuration(Duration.ofSeconds(9)).build())

            // When / Then
            assertThatThrownBy { guardOver(props, wiring).verify() }
                .hasMessageContaining("the timelimiter '$ACCOUNTS_ALIAS' has timeoutDuration=9000ms instead of the derived 1800ms")
        }

        @Test
        fun `should fail when a declared bulkhead has no instance in the registry`() {
            // What is tested: a bulkhead that is declared in the properties but absent from the registry.
            // Success criteria: `verify()` throws naming "the bulkhead '<alias>' has no instance in the registry",
            //   as the only problem.
            // Why it matters: a declared bulkhead the registry does not hold means the registry was not built
            //   from the properties - the same hand-made-registry case as above, seen on the one layer without a
            //   customizer.
            //
            // Given: everything wired, then the bulkhead's registry instance removed
            val props = propsFor()
            val wiring = wired(props)
            wiring.bulkheads.remove(ACCOUNTS_ALIAS)

            // When / Then
            assertThatThrownBy { guardOver(props, wiring).verify() }
                .hasMessageContaining("the bulkhead '$ACCOUNTS_ALIAS' has no instance in the registry")
                .hasMessageContaining("1 problem(s)")
        }

        @Test
        fun `should check the derived slow-call only when the circuit-breaker derivation is enabled`() {
            // What is tested: the slow-call reach check - with the circuit-breaker derivation enabled, the guard
            //   compares the breaker's slowCallDurationThreshold with the derived value.
            // Success criteria: a breaker customized with the derivation OFF (verdicts present, Resilience4j's
            //   60000ms default) fails with exactly one problem naming 60000ms instead of the derived 800ms.
            // Why it matters: verdicts in place prove the customizer ran, not that it applied the derived knobs;
            //   without this check an enabled derivation could be silently missing from the breaker.
            //
            // Given: derivation enabled, but the breaker carries the verdicts and NOT the derived threshold
            val props = propsFor(circuitBreakerEnabled = true)
            val wiring = wired(propsFor()) // customized with the derivation OFF: verdicts present, knobs untouched

            // When / Then: the one thing missing is named
            assertThatThrownBy { guardOver(props, wiring).verify() }
                .hasMessageContaining("1 problem(s)")
                .hasMessageContaining("has slowCallDurationThreshold=60000ms instead of the derived 800ms")
        }
    }

    @Nested
    inner class `An overridden verdict` {
        @Test
        fun `should name the record rule, not an unreached registry, when the breaker ignores a client-side failure but records the 404`() {
            // What is tested: the second diagnosis of a breaker that records the benign 4xx - its client-side
            //   verdict IS in place (it ignores the bulkhead rejection), only the benign-4xx verdict is not.
            // Success criteria: exactly one problem, worded as an override that points at the instance's
            //   record-exceptions configuration; the "did not reach it" diagnosis of an unreached registry does
            //   NOT appear.
            // Why it matters: the two cases have different fixes. Resilience4j's build() OR-s a record-exceptions
            //   class list back in past the customizer's predicate, so a list naming a superclass of the 4xx
            //   re-records it; an "unreached registry" message for that case would send the operator looking for
            //   a hand-built registry bean that does not exist.
            //
            // Given: a breaker that ignores the client-side failure but records everything else, the 404 included
            val props = propsFor()
            val wiring = wired(props)
            wiring.circuitBreakers.remove(ACCOUNTS_ALIAS)
            wiring.circuitBreakers.circuitBreaker(ACCOUNTS_ALIAS, CircuitBreakerConfig.custom().ignoreExceptions(BulkheadFullException::class.java).build())

            // When / Then
            assertThatThrownBy { guardOver(props, wiring).verify() }
                .isInstanceOf(IllegalStateException::class.java)
                .hasMessageContaining("1 problem(s)")
                .hasMessageContaining("the circuitbreaker '$ACCOUNTS_ALIAS' records a benign 4xx (404) as a backend failure although it ignores a client-side failure")
                .hasMessageContaining("resilience4j.circuitbreaker.instances.$ACCOUNTS_ALIAS")
                .hasMessageContaining("record-exceptions")
                .hasMessageNotContaining("did not reach it")
        }
    }

    @Nested
    inner class `The pool sizing` {
        @Test
        fun `should warn naming both numbers and every alias when the bulkheads admit more calls than the pool holds`() {
            // What is tested: the invariant sum(max-concurrent-calls) <= max-connections over the mesh aliases,
            //   read off the registry's bulkhead instances (one at 10 permits, one at the default 25) against a
            //   pool of 30.
            // Success criteria: the start is NOT failed; exactly one WARN carries the sum 35, the pool size 30 and
            //   each alias with its own permit count - the merged value from the registry, not a default.
            // Why it matters: with more permits than connections the excess queues in the shared pool, and the
            //   pending-acquire timeout that follows is charged to the breaker of whichever backend is waiting -
            //   a healthy backend's breaker can open because another one holds the connections. Nothing else in
            //   the library checks the invariant the reference configuration asks a host to keep.
            //
            // Given: two endpoints, a pool of 30, bulkheads of 10 and 25
            val props = propsFor(PAYMENTS_TRIPLET, maxConnections = 30)
            val wiring = wired(props)
            wiring.bulkheads.remove(ACCOUNTS_ALIAS)
            wiring.bulkheads.bulkhead(ACCOUNTS_ALIAS, BulkheadConfig.custom().maxConcurrentCalls(10).build())

            // When
            assertThatCode { guardOver(props, wiring).verify() }.doesNotThrowAnyException()

            // Then
            val warning = appender.list.filter { it.level == Level.WARN }.map { it.formattedMessage }
            assertThat(warning).hasSize(1)
            assertThat(warning.single())
                .contains("max-concurrent-calls)=35", "app.mesh.defaults.transport.max-connections=30")
                .contains("$ACCOUNTS_ALIAS=10", "$PAYMENTS_ALIAS=25")
        }

        @Test
        fun `should not warn when the bulkheads exactly fill the pool`() {
            // What is tested: the boundary of the pool check - two default bulkheads (2 x 25) on a pool of 50.
            // Success criteria: no WARN at all.
            // Why it matters: the reference configuration sizes 4 x 25 = 100 against the default 100 on purpose
            //   (the bulkheads shed exactly as the pool runs out); a check off by one would warn on every
            //   correctly sized deployment and teach operators to ignore it.
            //
            // Given
            val props = propsFor(PAYMENTS_TRIPLET, maxConnections = 50)

            // When
            guardOver(props, wired(props)).verify()

            // Then
            assertThat(appender.list.filter { it.level == Level.WARN }).isEmpty()
        }

        @Test
        fun `should check each pool the client factories report against its own backends only`() {
            // What is tested: the pool check over the pools a client factory reports (MeshConnectionPools) - two
            //   engines, each with its own pool of 30: one pool carries both aliases (10 + 25), the other one alias
            //   (25), and a third, unbounded pool (the JDK client) carries an alias as well.
            // Success criteria: exactly one WARN, naming the over-committed pool, the sum 35 and both of its
            //   aliases; the pool with 25 of 30 permits and the unbounded pool raise nothing.
            // Why it matters: with selectable engines every engine has a pool of its own; a sum over ALL aliases
            //   against one pool warns about a split that is correctly sized, and checking an unbounded pool
            //   warns about a limit that does not exist - both teach operators to ignore the line.
            //
            // Given
            val props = propsFor(PAYMENTS_TRIPLET, maxConnections = 30)
            val wiring = wired(props)
            wiring.bulkheads.remove(ACCOUNTS_ALIAS)
            wiring.bulkheads.bulkhead(ACCOUNTS_ALIAS, BulkheadConfig.custom().maxConcurrentCalls(10).build())
            val pools =
                listOf(
                    MeshConnectionPool("apache", 30, setOf(ACCOUNTS_ALIAS, PAYMENTS_ALIAS)),
                    MeshConnectionPool("jetty", 30, setOf(PAYMENTS_ALIAS)),
                    MeshConnectionPool("jdk", null, setOf(ACCOUNTS_ALIAS, PAYMENTS_ALIAS)),
                )

            // When
            guardOver(props, wiring) { pools }.verify()

            // Then
            val warning = appender.list.filter { it.level == Level.WARN }.map { it.formattedMessage }
            assertThat(warning).hasSize(1)
            assertThat(warning.single())
                .contains("connection pool 'apache'", "max-concurrent-calls)=35", "max-connections=30", "$ACCOUNTS_ALIAS=10", "$PAYMENTS_ALIAS=25")
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
        connectionPools: () -> List<MeshConnectionPool> = { emptyList() },
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
            connectionPools,
        )
}
