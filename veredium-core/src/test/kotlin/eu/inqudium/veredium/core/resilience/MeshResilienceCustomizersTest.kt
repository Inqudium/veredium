package eu.inqudium.veredium.core.resilience

import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.mesh.ResolvedTimeouts
import io.github.resilience4j.bulkhead.BulkheadFullException
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.io.IOException
import java.time.Duration

/**
 * Unit tests for the mesh-derived knobs the customizers hand to Resilience4j, applied through
 * [CustomizedRegistries] the way the Boot configuration applies them - no Spring needed. The derived alias for
 * triplet `/endpoint/api.example.com/GET/accounts/{accountId}` is `api-example-com-GET-accounts`.
 *
 * Reference numbers (defaults individual=1000, margins/transport at their Kotlin defaults; the default
 * maxRetries is 0). At maxRetries=3 (pinned in the tests that need a NON-inverted slow-call): endpoint
 * individual=500 -> global 2200, responseTimeout 2400, timeLimiter 2800, slowCall 800 (>= 500, NOT inverted).
 * At the default maxRetries=0: endpoint individual=500 -> global 700, responseTimeout 900, timeLimiter 1300,
 * slowCall 300 (< 500, INVERTED); endpoint individual=1000 -> global 1200, slowCall 800 (< 1000, INVERTED).
 *
 * The breaker verdicts the same customizer installs have their own tests (`ClientSideFailurePolicyTest`,
 * `HttpExchangeErrorPolicyTest`); here they appear only where the toggle semantics need them.
 */
class MeshResilienceCustomizersTest {
    private val alias = "api-example-com-GET-accounts"
    private val triplet = "/endpoint/api.example.com/GET/accounts/{accountId}"

    @Nested
    inner class CircuitBreaker {
        @Test
        fun `inherits a configured slow-call rate when the endpoint is not inverted`() {
            // What is tested: an ENABLED CircuitBreaker inherits the YAML slowCallRateThreshold when the derived
            //   slow-call is NOT inverted - only the derived duration knob is overridden.
            // Success criteria: with a configured 80% rate and a non-inverted endpoint (slowCall 800 >=
            //   individual 500), the instance ends up with the derived duration 800ms AND the inherited 80%.
            // Why it matters: mesh must not clobber an operator's rate where the derivation is already safe.

            // Given: what YAML produced for the instance pins a non-default 80% rate; CB derivation enabled.
            // maxRetries=3 is pinned so the slow-call stays NON-inverted (the default maxRetries=0 would invert it).
            val registry = CircuitBreakerRegistry.ofDefaults()
            val configured = CircuitBreakerConfig.custom().slowCallRateThreshold(80f).build()

            // When: the customizer runs over that config
            val config =
                CustomizedRegistries
                    .circuitBreaker(registry, endpoint(individual = 500, maxRetries = 3, circuitBreakerEnabled = true), configured)
                    .circuitBreakerConfig

            // Then: the derived duration is applied and the inherited 80% rate survives (not pinned)
            assertThat(config.slowCallDurationThreshold).isEqualTo(Duration.ofMillis(800))
            assertThat(config.slowCallRateThreshold).isEqualTo(80f)
        }

        @Test
        fun `pins the slow-call rate to 100 percent when the derived slow-call sits below individual`() {
            // What is tested: an ENABLED CircuitBreaker pins the rate to 100% exactly in the inversion case
            //   (derived slowCall < individual), overriding whatever rate YAML configured.
            // Success criteria: for individual=1000 maxRetries=0 (slowCall 800 < 1000), a configured 80%
            //   becomes 100% while the derived duration is the ladder value.
            // Why it matters: an inverted duration is only safe when the breaker requires TOTAL degradation to
            //   open; pinning the rate in the SAME customizer makes the coupling unresolvable by construction.

            // Given: an inverted endpoint and a config pinning a (would-be-unsafe) 80% rate, CB enabled
            val registry = CircuitBreakerRegistry.ofDefaults()
            val configured = CircuitBreakerConfig.custom().slowCallRateThreshold(80f).build()

            // When
            val config =
                CustomizedRegistries
                    .circuitBreaker(registry, endpoint(individual = 1000, maxRetries = 0, circuitBreakerEnabled = true), configured)
                    .circuitBreakerConfig

            // Then: the rate is pinned to 100% and the duration is the ladder value
            assertThat(config.slowCallRateThreshold).isEqualTo(100f)
            assertThat(config.slowCallDurationThreshold).isEqualTo(Duration.ofMillis(800))
        }

        @Test
        fun `leaves the slow-call knobs untouched but still installs the verdicts when the derivation is off`() {
            // What is tested: the common operational case - CircuitBreaker derivation off (the default). The
            //   slow-call knobs stay whatever YAML made them, while the two verdicts are installed regardless,
            //   because the breaker is in the call path regardless of the toggle.
            // Success criteria: a 33% marker rate and the YAML slow-call duration survive; the same config
            //   ignores a bulkhead rejection and does not record a 404.
            // Why it matters: gating the verdicts on the toggle would leave the invariant unmet in exactly the
            //   configuration that ships (toggle off everywhere).

            // Given: a marker config and the default toggles (TL on, CB off)
            val registry = CircuitBreakerRegistry.ofDefaults()
            val configured =
                CircuitBreakerConfig
                    .custom()
                    .slowCallRateThreshold(33f)
                    .slowCallDurationThreshold(Duration.ofSeconds(42))
                    .build()
            val endpoint = endpoint(individual = 500)
            assertThat(endpoint.circuitBreakerEnabled).isFalse()

            // When
            val config = CustomizedRegistries.circuitBreaker(registry, endpoint, configured).circuitBreakerConfig

            // Then: knobs untouched ...
            assertThat(config.slowCallRateThreshold).isEqualTo(33f)
            assertThat(config.slowCallDurationThreshold).isEqualTo(Duration.ofSeconds(42))
            // ... verdicts installed
            assertThat(config.ignoreExceptionPredicate.test(bulkheadRejection())).isTrue()
            assertThat(config.recordExceptionPredicate.test(notFound())).isFalse()
        }

        @Test
        fun `keeps a benign 4xx unrecorded when the operator's record-exceptions list names a superclass of it`() {
            // What is tested: the record verdict against an operator's record-exceptions CLASS LIST - here
            //   RuntimeException, a superclass of the stack's 4xx exception, as an operator writes it to count 5xx
            //   explicitly - together with an ignore-exceptions list.
            // Success criteria: the benign 404 is NOT recorded; a listed exception (a 503, an
            //   IllegalStateException) IS recorded; an exception outside the list (IOException) is not; the
            //   operator's ignore list still ignores its type.
            // Why it matters: Resilience4j's build() OR-s the class lists back in on top of the record predicate
            //   the customizer sets, so without clearing them the list re-recorded the 404 behind the verdict.
            //   With the startup guard that was a failed start blaming the registry; without it, a breaker that
            //   opens on business-case 404s.
            //
            // Given: what YAML produced - a record list with a superclass of the 404, and an ignore list
            val configured =
                CircuitBreakerConfig
                    .custom()
                    .recordExceptions(RuntimeException::class.java)
                    .ignoreExceptions(IllegalArgumentException::class.java)
                    .build()

            // When
            val config = CustomizedRegistries.circuitBreaker(CircuitBreakerRegistry.ofDefaults(), endpoint(individual = 500), configured).circuitBreakerConfig

            // Then
            assertThat(config.recordExceptionPredicate.test(notFound())).`as`("benign 404").isFalse()
            assertThat(config.recordExceptionPredicate.test(TestResponseException(503))).`as`("listed: a 503").isTrue()
            assertThat(config.recordExceptionPredicate.test(IllegalStateException("listed"))).`as`("listed: a RuntimeException").isTrue()
            assertThat(config.recordExceptionPredicate.test(IOException("not listed"))).`as`("outside the list").isFalse()
            assertThat(config.ignoreExceptionPredicate.test(IllegalArgumentException("operator's ignore"))).isTrue()
            assertThat(config.ignoreExceptionPredicate.test(bulkheadRejection())).isTrue()
        }

        @Test
        fun `keeps a benign 4xx unrecorded when the operator's record predicate matches it`() {
            // What is tested: the same composition through the other configuration route - a record-exception
            //   PREDICATE (`record-exception-predicate` in YAML) that matches every RuntimeException.
            // Success criteria: the 404 is not recorded, a 503 is, an IOException (outside the predicate) is not.
            // Why it matters: the predicate is the half the customizer AND-s directly; pinned so that dropping the
            //   `&& configured...` term, or the benign-4xx term, turns a test red.
            //
            // Given
            val configured = CircuitBreakerConfig.custom().recordException { it is RuntimeException }.build()

            // When
            val config = CustomizedRegistries.circuitBreaker(CircuitBreakerRegistry.ofDefaults(), endpoint(individual = 500), configured).circuitBreakerConfig

            // Then
            assertThat(config.recordExceptionPredicate.test(notFound())).isFalse()
            assertThat(config.recordExceptionPredicate.test(TestResponseException(503))).isTrue()
            assertThat(config.recordExceptionPredicate.test(IOException("outside the predicate"))).isFalse()
        }

        @Test
        fun `lets a per-endpoint circuit-breaker enabled flag override the default off`() {
            // What is tested: a per-endpoint circuit-breaker.enabled=true overrides the default-off, so the CB is
            //   derived for exactly that endpoint.
            // Success criteria: with defaults CB off but the endpoint's flag true, the CB gets the ladder
            //   slowCallDurationThreshold 300ms (individual 500 at the default maxRetries=0).
            // Why it matters: per-endpoint opt-in must work even when the global default is off.

            // Given: default CB off but a per-endpoint circuit-breaker.enabled=true
            val props =
                MeshProperties(
                    defaults = MeshProperties.Defaults(individualRequestTimeoutMs = 1000),
                    endpoints =
                        listOf(
                            MeshProperties.EndpointConfig(
                                triplet,
                                individualRequestTimeoutMs = 500,
                                circuitBreaker = MeshProperties.BackendToggleOverride(enabled = true),
                            ),
                        ),
                )

            // When
            val config =
                CustomizedRegistries
                    .circuitBreaker(CircuitBreakerRegistry.ofDefaults(), props.resolveAll().getValue(triplet))
                    .circuitBreakerConfig

            // Then: the CB is derived for this endpoint
            assertThat(config.slowCallDurationThreshold).isEqualTo(Duration.ofMillis(300))
        }
    }

    @Nested
    inner class TimeLimiter {
        @Test
        fun `sets the derived budget and inherits everything else`() {
            // What is tested: the time-limiter customizer sets exactly ONE knob - the ladder budget - and leaves
            //   the rest of the YAML policy as it was.
            // Success criteria: over a config with a 9s budget and cancelRunningFuture=false, the result
            //   carries the derived 1300ms (individual 500: global 700, responseTimeout 900, + 400) AND still
            //   cancelRunningFuture=false.
            // Why it matters: cancelRunningFuture is what the decorator relies on to release the worker; a
            //   customizer that rebuilt the whole config would reset it to the library default behind the
            //   operator's back.

            // Given: a YAML-shaped policy with a marker knob
            val registry = TimeLimiterRegistry.ofDefaults()
            val configured =
                TimeLimiterConfig
                    .custom()
                    .timeoutDuration(Duration.ofSeconds(9))
                    .cancelRunningFuture(false)
                    .build()

            // When
            val config = CustomizedRegistries.timeLimiter(registry, endpoint(individual = 500), configured).timeLimiterConfig

            // Then
            assertThat(config.timeoutDuration).isEqualTo(Duration.ofMillis(1300))
            assertThat(config.shouldCancelRunningFuture()).isFalse()
        }

        @Test
        fun `leaves the time limiter untouched when its derivation is off for the endpoint`() {
            // What is tested: a per-endpoint time-limiter.enabled=false overrides the default-on, so no customizer
            //   exists for that alias and the instance is exactly what YAML made it.
            // Success criteria: a registered 7s instance is returned as the very same object with its 7s.
            // Why it matters: per-endpoint opt-out must work even when the global default is on.

            // Given: default TL on but a per-endpoint time-limiter.enabled=false, and a registered 7s instance
            val registry = TimeLimiterRegistry.ofDefaults()
            val before = registry.timeLimiter(alias, TimeLimiterConfig.custom().timeoutDuration(Duration.ofSeconds(7)).build())
            val props =
                MeshProperties(
                    defaults = MeshProperties.Defaults(individualRequestTimeoutMs = 1000),
                    endpoints =
                        listOf(
                            MeshProperties.EndpointConfig(
                                triplet,
                                individualRequestTimeoutMs = 500,
                                timeLimiter = MeshProperties.BackendToggleOverride(enabled = false),
                            ),
                        ),
                )

            // When
            val after = CustomizedRegistries.timeLimiter(registry, props.resolveAll().getValue(triplet))

            // Then: the very same instance, its 7s intact
            assertThat(after).isSameAs(before)
            assertThat(after.timeLimiterConfig.timeoutDuration).isEqualTo(Duration.ofSeconds(7))
        }
    }

    @Nested
    inner class Naming {
        @Test
        fun `names each customizer after the alias so Resilience4j consults it for exactly that instance`() {
            // What is tested: the one property Resilience4j keys on - a customizer is looked up by the instance
            //   name while the instance is built, and a mismatch is a silent no-op.
            // Success criteria: both customizers report the endpoint's alias as their name.
            // Why it matters: the alias is derived (host-METHOD-path); a customizer named after the triplet or
            //   the adapter would never be consulted, and nothing but the startup guard would say so.
            //
            // Given
            val endpoint = endpoint(individual = 500)

            // When / Then
            assertThat(circuitBreakerCustomizer(endpoint, TestVerdicts.breakerVerdicts).name()).isEqualTo(alias)
            assertThat(timeLimiterCustomizer(endpoint).name()).isEqualTo(alias)
        }

        @Test
        fun `inherits the default toggle when the per-endpoint flag is unset`() {
            // What is tested: the tri-state resolution - a null per-endpoint flag inherits the default, it does
            //   not force false.
            // Success criteria: with default toggles (TL on, CB off) and unset endpoint flags, the resolved
            //   values are TL true / CB false; flipping the default CB to on flips the resolved value too.
            // Why it matters: modelling the override as Boolean? (not a non-null false) is what keeps "unset"
            //   distinct from "deliberately off".

            // Given: an endpoint with unset (null) toggles and the shared defaults
            val defaultToggles = endpoint(individual = 500)

            // When / Then: the resolved toggles equal the defaults (TL on, CB off)
            assertThat(defaultToggles.timeLimiterEnabled).isTrue()
            assertThat(defaultToggles.circuitBreakerEnabled).isFalse()

            // And when the default CB is flipped on, an unset endpoint inherits that
            assertThat(endpoint(individual = 500, circuitBreakerEnabled = true).circuitBreakerEnabled).isTrue()
        }
    }

    /** The single accounts endpoint resolved, with optional CB opt-in and a maxRetries override. */
    private fun endpoint(
        individual: Long,
        maxRetries: Int? = null,
        circuitBreakerEnabled: Boolean = false,
    ): ResolvedTimeouts =
        MeshProperties(
            defaults =
                MeshProperties.Defaults(
                    individualRequestTimeoutMs = 1000,
                    circuitBreaker = MeshProperties.BackendToggle(enabled = circuitBreakerEnabled),
                ),
            endpoints =
                listOf(
                    MeshProperties.EndpointConfig(
                        triplet,
                        individualRequestTimeoutMs = individual,
                        maxRetries = maxRetries,
                    ),
                ),
        ).resolveAll().getValue(triplet)

    private fun bulkheadRejection(): BulkheadFullException = TestVerdicts.bulkheadRejection()

    private fun notFound(): TestResponseException = TestResponseException(404)
}
