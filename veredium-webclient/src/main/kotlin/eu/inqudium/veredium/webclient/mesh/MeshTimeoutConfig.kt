package eu.inqudium.veredium.webclient.mesh

import eu.inqudium.veredium.webclient.resilience.AmendmentPlan
import eu.inqudium.veredium.webclient.resilience.ResilienceAmendmentApplier
import eu.inqudium.veredium.webclient.resilience.ResilienceAmendmentContributor
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration

/**
 * Integrates the mesh timeout derivation into the module's EXISTING Resilience4j registries. Enables the
 * `app.mesh` binding and wires a [MeshTimeoutRegistrar], which CONTRIBUTES the derived timeout knob per
 * endpoint and per backend; [ResilienceAmendmentApplier] applies it, together with every other contribution,
 * to the registries auto-configured by resilience4j-spring-boot3 - no new registry beans are introduced.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(MeshProperties::class)
class MeshTimeoutConfig {
    @Bean
    fun meshTimeoutRegistrar(meshProperties: MeshProperties): MeshTimeoutRegistrar = MeshTimeoutRegistrar(meshProperties)
}

/**
 * Contributes the mesh-derived timeout rungs to the [AmendmentPlan]: per endpoint, and only for a backend the
 * endpoint has enabled, the TimeLimiter `timeoutDuration` (= [ResolvedTimeouts.timeLimiterTimeout]) and the
 * CircuitBreaker `slowCallDurationThreshold` (= [ResolvedTimeouts.slowCallDuration]).
 *
 * ONLY those knobs change. The applier bases each amendment on the endpoint's most specific existing config,
 * so the rest of that backend's R4j policy - a `configs:` block, a per-instance override - is inherited rather
 * than replaced; where mesh and YAML both speak, mesh wins. A DISABLED backend contributes NOTHING, so its
 * instance is not amended and not recreated: the endpoint keeps whatever it had, untouched.
 *
 * Coherence: an ENABLED CircuitBreaker pins `slowCallRateThreshold` to 100% exactly when the derived slow-call
 * sits below `individual` (the "inversion"); otherwise the inherited rate stands. That resolves the
 * duration/rate coupling by construction - the derived duration is only safe against a healthy-but-slow
 * endpoint when the rate demands TOTAL degradation before opening. The two are contributed as ONE amendment so
 * the pair can never be split apart by a later change.
 *
 * This class touches no registry and decides no ordering; [ResilienceAmendmentApplier] owns both, which is
 * also where the init-order reasoning lives. [contribute] is a plain method over a plan, so what it decides
 * can be asserted without a registry at all.
 */
class MeshTimeoutRegistrar(
    private val meshProperties: MeshProperties,
) : ResilienceAmendmentContributor {
    private val log = LoggerFactory.getLogger(MeshTimeoutRegistrar::class.java)

    override fun contribute(plan: AmendmentPlan) {
        meshProperties.resolveAll().values.sortedBy { it.alias }.forEach { t ->
            if (t.timeLimiterEnabled) {
                plan.timeLimiter(t.alias) { timeoutDuration(t.timeLimiterTimeout) }
            }
            if (t.circuitBreakerEnabled) {
                // Unit-align the comparison; the rate pin is part of the SAME amendment as the duration.
                val individual = Duration.ofMillis(t.individualRequestTimeoutMs)
                plan.circuitBreaker(t.alias) {
                    slowCallDurationThreshold(t.slowCallDuration)
                    if (t.slowCallDuration < individual) slowCallRateThreshold(100f)
                }
            }
            // What mesh DECIDES for this endpoint. The derived numbers themselves are logged once per endpoint
            // by MeshLadderLogger on ApplicationReadyEvent - repeating them here only invited the two to drift.
            log.info(
                "mesh backends [{}] {}: timeLimiter={}, circuitBreaker={}",
                t.alias,
                t.triplet,
                if (t.timeLimiterEnabled) "on (${t.timeLimiterTimeout.toMillis()}ms)" else "off (untouched)",
                if (t.circuitBreakerEnabled) {
                    "on (slowCall=${t.slowCallDuration.toMillis()}ms" +
                        if (t.slowCallDuration < Duration.ofMillis(t.individualRequestTimeoutMs)) {
                            ", rate pinned to 100%)"
                        } else {
                            ", rate inherited)"
                        }
                } else {
                    "off (untouched)"
                },
            )
        }
    }
}
