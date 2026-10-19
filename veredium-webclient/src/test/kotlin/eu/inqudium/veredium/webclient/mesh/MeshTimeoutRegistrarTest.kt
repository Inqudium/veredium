package eu.inqudium.veredium.webclient.mesh

import eu.inqudium.veredium.webclient.resilience.ResilienceAmendmentApplier
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration

/**
 * Unit tests for what [MeshTimeoutRegistrar] contributes, applied against `Registry.of(...)` through the
 * real [ResilienceAmendmentApplier] - no Spring needed. The derived
 * alias for triplet `/endpoint/api.example.com/GET/accounts/{accountId}` is `api-example-com-GET-accounts`.
 *
 * Reference numbers (defaults individual=1000, margins/transport at their Kotlin defaults; the default
 * maxRetries is now 0). At maxRetries=3 (pinned in the tests that need a NON-inverted slow-call): endpoint
 * individual=500 -> global 2200, responseTimeout 2400, timeLimiter 2800, slowCall 800 (>= 500, NOT inverted).
 * At the default maxRetries=0: endpoint individual=500 -> global 700, responseTimeout 900, timeLimiter 1300,
 * slowCall 300 (< 500, INVERTED); endpoint individual=1000 -> global 1200, slowCall 800 (< 1000, INVERTED).
 */
class MeshTimeoutRegistrarTest {
    private val alias = "api-example-com-GET-accounts"
    private val triplet = "/endpoint/api.example.com/GET/accounts/{accountId}"

    @Test
    fun `inherits a pre-registered per-backend slow-call rate when the endpoint is not inverted`() {
        // What is tested: an ENABLED CircuitBreaker inherits the endpoint's existing slowCallRateThreshold when
        // the derived slow-call is NOT inverted - only the derived duration knob is overridden.
        // How it is deemed successful: with a per-backend 80% rate and a non-inverted endpoint (slowCall 800 >=
        // individual 500), the instance ends up with the derived duration 800ms AND the inherited 80%.
        // Why it matters: mesh must not clobber an operator's rate where the derivation is already safe.

        // Given a registry whose instance under the alias pins a non-default 80% rate, and CB enabled.
        // maxRetries=3 is pinned so the slow-call stays NON-inverted (the default maxRetries=0 would invert it).
        val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
        circuitBreakerRegistry.circuitBreaker(alias, CircuitBreakerConfig.custom().slowCallRateThreshold(80f).build())
        val props = propsFor(individual = 500, maxRetries = 3, circuitBreakerEnabled = true)

        // When the registrar registers the derived configs
        ResilienceAmendmentApplier.applyOnce(circuitBreakerRegistry, TimeLimiterRegistry.ofDefaults(), MeshTimeoutRegistrar(props))

        // Then the derived duration is applied and the inherited 80% rate survives (not pinned)
        val cb = circuitBreakerRegistry.circuitBreaker(alias).circuitBreakerConfig
        assertThat(cb.slowCallDurationThreshold).isEqualTo(Duration.ofMillis(800))
        assertThat(cb.slowCallRateThreshold).isEqualTo(80f)
    }

    @Test
    fun `pins the slow-call rate to 100 percent when the derived slow-call sits below individual`() {
        // What is tested: an ENABLED CircuitBreaker pins the rate to 100% exactly in the inversion case
        // (derived slowCall < individual), overriding whatever rate was inherited.
        // How it is deemed successful: for individual=1000 maxRetries=0 (slowCall 800 < 1000), an inherited 80%
        // becomes 100% while the derived duration is the ladder value.
        // Why it matters: an inverted duration is only safe when the breaker requires TOTAL degradation to open;
        // pinning the rate makes the coupling unresolvable by construction.

        // Given an inverted endpoint and a registry pinning a (would-be-unsafe) 80% rate, CB enabled
        val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
        circuitBreakerRegistry.circuitBreaker(alias, CircuitBreakerConfig.custom().slowCallRateThreshold(80f).build())
        val props = propsFor(individual = 1000, maxRetries = 0, circuitBreakerEnabled = true)

        // When the registrar runs
        ResilienceAmendmentApplier.applyOnce(circuitBreakerRegistry, TimeLimiterRegistry.ofDefaults(), MeshTimeoutRegistrar(props))

        // Then the rate is pinned to 100% and the duration is the ladder value
        val cb = circuitBreakerRegistry.circuitBreaker(alias).circuitBreakerConfig
        assertThat(cb.slowCallRateThreshold).isEqualTo(100f)
        assertThat(cb.slowCallDurationThreshold).isEqualTo(Duration.ofMillis(800))
    }

    @Test
    fun `falls back to the named configuration when no instance is materialized yet`() {
        // What is tested: the base-config lookup is order-independent - a NAMED configs: configuration is used
        // when no instance has been materialized, instead of silently falling through to the registry default.
        // How it is deemed successful: with only addConfiguration(alias, 42% rate) and NO instance, the derived
        // config inherits 42% (a marker) rather than the R4j default 100%.
        // Why it matters: this is the exact init-order trap - an instance-only lookup would lose the operator's
        // named policy if the amendment is applied before the instance is created.

        // Given a NAMED config under the alias but no materialized instance, CB enabled, non-inverted endpoint.
        // maxRetries=3 is pinned so the slow-call stays NON-inverted (the default maxRetries=0 would invert it).
        val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
        circuitBreakerRegistry.addConfiguration(alias, CircuitBreakerConfig.custom().slowCallRateThreshold(42f).build())
        val props = propsFor(individual = 500, maxRetries = 3, circuitBreakerEnabled = true)

        // When the registrar runs
        ResilienceAmendmentApplier.applyOnce(circuitBreakerRegistry, TimeLimiterRegistry.ofDefaults(), MeshTimeoutRegistrar(props))

        // Then the base policy comes from the named config (42%), not defaultConfig (100%)
        val cb = circuitBreakerRegistry.circuitBreaker(alias).circuitBreakerConfig
        assertThat(cb.slowCallRateThreshold).isEqualTo(42f)
        assertThat(cb.slowCallDurationThreshold).isEqualTo(Duration.ofMillis(800))
    }

    @Test
    fun `derives the time-limiter but leaves the circuit-breaker untouched when the CB default is off`() {
        // What is tested: the common operational case - TimeLimiter on (default), CircuitBreaker off (default).
        // How it is deemed successful: the TimeLimiter carries the ladder timeoutDuration 1300ms while the CB
        // instance is the SAME object as before (no remove+recreate), keeping its pre-existing config.
        // Why it matters: the two backends are switched separately; a disabled backend must be skipped entirely.

        // Given a pre-registered CB instance carrying a marker, and default toggles (TL on, CB off)
        val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
        val cbBefore =
            circuitBreakerRegistry.circuitBreaker(alias, CircuitBreakerConfig.custom().slowCallRateThreshold(33f).build())
        val timeLimiterRegistry = TimeLimiterRegistry.ofDefaults()
        val props = propsFor(individual = 500) // default toggles: TL on, CB off

        // When the registrar runs
        ResilienceAmendmentApplier.applyOnce(circuitBreakerRegistry, timeLimiterRegistry, MeshTimeoutRegistrar(props))

        // Then the TimeLimiter is derived...
        assertThat(timeLimiterRegistry.timeLimiter(alias).timeLimiterConfig.timeoutDuration)
            .isEqualTo(Duration.ofMillis(1300))
        // ...and the CircuitBreaker is untouched: the very same instance, its marker rate intact
        val cbAfter = circuitBreakerRegistry.circuitBreaker(alias)
        assertThat(cbAfter).isSameAs(cbBefore)
        assertThat(cbAfter.circuitBreakerConfig.slowCallRateThreshold).isEqualTo(33f)
    }

    @Test
    fun `lets a per-endpoint circuit-breaker enabled flag override the default off`() {
        // What is tested: a per-endpoint circuit-breaker.enabled=true overrides the default-off, so the CB is
        // derived for exactly that endpoint.
        // How it is deemed successful: with defaults CB off but the endpoint's flag true, the CB gets the ladder
        // slowCallDurationThreshold 300ms (individual 500 at the default maxRetries=0).
        // Why it matters: per-endpoint opt-in must work even when the global default is off.

        // Given default CB off but a per-endpoint circuit-breaker.enabled=true
        val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
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

        // When the registrar runs
        ResilienceAmendmentApplier.applyOnce(circuitBreakerRegistry, TimeLimiterRegistry.ofDefaults(), MeshTimeoutRegistrar(props))

        // Then the CB is derived for this endpoint
        assertThat(circuitBreakerRegistry.circuitBreaker(alias).circuitBreakerConfig.slowCallDurationThreshold)
            .isEqualTo(Duration.ofMillis(300))
    }

    @Test
    fun `lets a per-endpoint time-limiter enabled false override the default on`() {
        // What is tested: a per-endpoint time-limiter.enabled=false overrides the default-on, so the TimeLimiter
        // is left untouched for that endpoint.
        // How it is deemed successful: a pre-registered TL instance with a 7s marker survives unchanged (same
        // instance), NOT replaced by the ladder 2800ms.
        // Why it matters: per-endpoint opt-out must work even when the global default is on.

        // Given default TL on but a per-endpoint time-limiter.enabled=false, and a pre-registered TL marker
        val timeLimiterRegistry = TimeLimiterRegistry.ofDefaults()
        val tlBefore =
            timeLimiterRegistry.timeLimiter(alias, TimeLimiterConfig.custom().timeoutDuration(Duration.ofSeconds(7)).build())
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

        // When the registrar runs
        ResilienceAmendmentApplier.applyOnce(CircuitBreakerRegistry.ofDefaults(), timeLimiterRegistry, MeshTimeoutRegistrar(props))

        // Then the TimeLimiter is left untouched: the very same instance, its 7s marker intact
        val tlAfter = timeLimiterRegistry.timeLimiter(alias)
        assertThat(tlAfter).isSameAs(tlBefore)
        assertThat(tlAfter.timeLimiterConfig.timeoutDuration).isEqualTo(Duration.ofSeconds(7))
    }

    @Test
    fun `inherits the default toggle when the per-endpoint flag is unset`() {
        // What is tested: the tri-state resolution - a null per-endpoint flag inherits the default, it does not
        // force false.
        // How it is deemed successful: with default toggles (TL on, CB off) and unset endpoint flags, the
        // resolved values are TL true / CB false; flipping the default CB to on flips the resolved value too.
        // Why it matters: modelling the override as Boolean? (not a non-null false) is what keeps "unset"
        // distinct from "deliberately off".

        // Given an endpoint with unset (null) toggles and the shared defaults
        val defaultToggles =
            MeshProperties(
                defaults = MeshProperties.Defaults(individualRequestTimeoutMs = 1000),
                endpoints = listOf(MeshProperties.EndpointConfig(triplet, individualRequestTimeoutMs = 500)),
            ).resolveAll().getValue(triplet)

        // Then the resolved toggles equal the defaults (TL on, CB off)
        assertThat(defaultToggles.timeLimiterEnabled).isTrue()
        assertThat(defaultToggles.circuitBreakerEnabled).isFalse()

        // And when the default CB is flipped on, an unset endpoint inherits that
        val cbDefaultOn =
            MeshProperties(
                defaults =
                    MeshProperties.Defaults(
                        individualRequestTimeoutMs = 1000,
                        circuitBreaker = MeshProperties.BackendToggle(enabled = true),
                    ),
                endpoints = listOf(MeshProperties.EndpointConfig(triplet, individualRequestTimeoutMs = 500)),
            ).resolveAll().getValue(triplet)
        assertThat(cbDefaultOn.circuitBreakerEnabled).isTrue()
    }

    /** Build props for the single accounts endpoint, with optional CB opt-in and a maxRetries override. */
    private fun propsFor(
        individual: Long,
        maxRetries: Int? = null,
        circuitBreakerEnabled: Boolean = false,
    ): MeshProperties =
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
        )
}
