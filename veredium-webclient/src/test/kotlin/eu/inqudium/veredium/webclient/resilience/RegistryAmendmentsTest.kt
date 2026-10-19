package eu.inqudium.veredium.webclient.resilience

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.Duration

/**
 * Tests for the shared amendment mechanics both startup registrars use ([amend]).
 *
 * These pin the three properties the callers depend on and previously only asserted in prose: which config an
 * amendment builds on, that registering actually overrides, and that two amendments to the same instance
 * converge no matter which runs first.
 */
class RegistryAmendmentsTest {
    @Nested
    inner class BaseConfigLookup {
        @Test
        fun `should build on a materialized instance rather than on the registry default`() {
            // What is tested: the most specific source wins - an instance that already exists.
            // How success is judged: an unrelated knob of the materialized config (the sliding window) survives
            //   an amendment that only touches the failure rate.
            // Why it matters: this is the case that silently loses an operator's per-backend policy. Basing the
            //   amendment on `defaultConfig` would compile, run, and quietly reset every setting the endpoint
            //   had - visible only to whoever later wonders why their YAML has no effect.
            //
            // Given a registry whose instance carries a non-default window
            val registry = CircuitBreakerRegistry.ofDefaults()
            registry.circuitBreaker("backend", CircuitBreakerConfig.custom().slidingWindowSize(7).build())

            // When only the failure rate is amended
            val amended = registry.amend("backend") { failureRateThreshold(42f) }

            // Then the window survives and the rate is new
            assertThat(amended.slidingWindowSize).isEqualTo(7)
            assertThat(amended.failureRateThreshold).isEqualTo(42f)
        }

        @Test
        fun `should build on a named configuration when no instance has been materialized yet`() {
            // Why this case is important: `getConfiguration(name)` sees ONLY named `configs:` entries, and a
            // registrar can easily run before the instance behind such a name exists. Falling through to the
            // registry default at that moment would discard the named policy - and whether it happens depends
            // on bean ordering, so it would be intermittent.
            //
            // Given a NAMED configuration, with no instance created from it
            val registry = CircuitBreakerRegistry.ofDefaults()
            registry.addConfiguration("named", CircuitBreakerConfig.custom().slidingWindowSize(9).build())

            // When an instance under that name is amended
            val amended = registry.amend("named") { failureRateThreshold(42f) }

            // Then the named policy is the base
            assertThat(amended.slidingWindowSize).isEqualTo(9)
        }

        @Test
        fun `should fall back to the registry default when neither exists`() {
            // Given / When: a name the registry has never seen
            val registry =
                CircuitBreakerRegistry.of(CircuitBreakerConfig.custom().slidingWindowSize(11).build())
            val amended = registry.amend("unknown") { failureRateThreshold(42f) }

            // Then
            assertThat(amended.slidingWindowSize).isEqualTo(11)
        }
    }

    @Nested
    inner class Overriding {
        @Test
        fun `should replace the registered instance, not return the existing one`() {
            // What is tested: that the amendment is actually in force for callers who resolve the instance
            //   afterwards - which is the whole point, and the step that a plain `circuitBreaker(name, config)`
            //   silently skips because it returns the EXISTING instance when the name is taken.
            // How success is judged: the instance resolved from the registry after the call carries the amended
            //   config, not merely the returned config object.
            //
            // Given an existing instance
            val registry = CircuitBreakerRegistry.ofDefaults()
            registry.circuitBreaker("backend", CircuitBreakerConfig.custom().failureRateThreshold(10f).build())

            // When
            registry.amend("backend") { failureRateThreshold(90f) }

            // Then the registry hands out the amended one
            assertThat(registry.circuitBreaker("backend").circuitBreakerConfig.failureRateThreshold)
                .isEqualTo(90f)
        }

        @Test
        fun `should amend a time limiter the same way`() {
            // Given / When / Then: the TimeLimiter counterpart, same lookup and same override semantics.
            val registry = TimeLimiterRegistry.ofDefaults()
            registry.timeLimiter("backend", TimeLimiterConfig.custom().cancelRunningFuture(false).build())

            val amended = registry.amend("backend") { timeoutDuration(Duration.ofMillis(1234)) }

            assertThat(amended.timeoutDuration).isEqualTo(Duration.ofMillis(1234))
            assertThat(amended.shouldCancelRunningFuture()).isFalse()
            assertThat(registry.timeLimiter("backend").timeLimiterConfig.timeoutDuration)
                .isEqualTo(Duration.ofMillis(1234))
        }
    }

    @Nested
    inner class OrderIndependence {
        @Test
        fun `should converge on the same config whichever amendment runs first`() {
            // What is tested: THE property both registrars rely on. Two components amend the same circuit
            //   breaker at startup - the mesh ladder sets a slow-call threshold, the client-side-failure rule
            //   adds an ignore predicate - and Spring does not promise which SmartInitializingSingleton runs
            //   first.
            // How success is judged: both orders produce a config that carries BOTH changes. Running the same
            //   two amendments in both orders and comparing the outcomes is the direct expression of the claim.
            // Why it is important: if the property did not hold, one of the two changes would be lost in
            //   whichever order Spring happened to pick - green tests, correct-looking code, and a circuit
            //   breaker that either ignores nothing or has no slow-call rung, depending on bean ordering.
            //
            // Given two amendments in the shape the real registrars use
            val slowCall: CircuitBreakerConfig.Builder.(CircuitBreakerConfig) -> Unit = {
                slowCallDurationThreshold(Duration.ofMillis(700))
            }
            val ignoreArithmetic: CircuitBreakerConfig.Builder.(CircuitBreakerConfig) -> Unit = { base ->
                ignoreException { e -> e is ArithmeticException || base.ignoreExceptionPredicate.test(e) }
            }

            // When they are applied in both orders, on two separate registries
            val ladderFirst = CircuitBreakerRegistry.ofDefaults()
            ladderFirst.amend("backend", slowCall)
            val a = ladderFirst.amend("backend", ignoreArithmetic)

            val ruleFirst = CircuitBreakerRegistry.ofDefaults()
            ruleFirst.amend("backend", ignoreArithmetic)
            val b = ruleFirst.amend("backend", slowCall)

            // Then both carry both changes
            assertThat(a.slowCallDurationThreshold).isEqualTo(Duration.ofMillis(700))
            assertThat(b.slowCallDurationThreshold).isEqualTo(Duration.ofMillis(700))
            assertThat(a.ignoreExceptionPredicate.test(ArithmeticException())).isTrue()
            assertThat(b.ignoreExceptionPredicate.test(ArithmeticException())).isTrue()
        }

        @Test
        fun `should keep an earlier ignore predicate when a later amendment adds to it`() {
            // Why this case is important: `ignoreException(...)` REPLACES the predicate that from(base) copied.
            // An amendment that means "also ignore X" must OR the base predicate back in - which is the reason
            // amend hands the base config to the amendment at all. Without that, the second registrar to run
            // would quietly drop the first one's ignore rule.
            //
            // Given a first amendment that ignores one type
            val registry = CircuitBreakerRegistry.ofDefaults()
            registry.amend("backend") { base ->
                ignoreException { e -> e is IllegalStateException || base.ignoreExceptionPredicate.test(e) }
            }

            // When a second one adds another, OR-ing the base back in
            val amended =
                registry.amend("backend") { base ->
                    ignoreException { e -> e is ArithmeticException || base.ignoreExceptionPredicate.test(e) }
                }

            // Then both are ignored, and an unrelated failure still counts
            assertThat(amended.ignoreExceptionPredicate.test(IllegalStateException())).isTrue()
            assertThat(amended.ignoreExceptionPredicate.test(ArithmeticException())).isTrue()
            assertThat(amended.ignoreExceptionPredicate.test(RuntimeException())).isFalse()
        }
    }
}
