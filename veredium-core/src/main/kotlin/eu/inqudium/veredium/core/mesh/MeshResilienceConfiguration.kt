package eu.inqudium.veredium.core.mesh

import eu.inqudium.veredium.core.resilience.MeshOwnershipCheck
import eu.inqudium.veredium.core.resilience.MeshResilienceRegistrar
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment

/**
 * The stack-neutral half of the mesh wiring: the `app.mesh` binding and the startup line that prints the derived
 * ladder ([MeshLadderLogger]).
 *
 * The other half needs the client stack's breaker verdicts and is therefore registered by the stack, with its
 * verdicts object handed in directly ([MeshResilienceRegistrar]): the per-alias `*ConfigCustomizer` beans that
 * hand the derived rungs and the verdicts to Resilience4j while it builds each `instances.<alias>` entry, and the
 * start-time check that every mesh alias is declared and actually received them. Each stack's configuration
 * `@Import`s this class next to its registrar, so a host never imports either directly; both come with the stack
 * it depends on. With both stacks in one context, [MeshOwnershipCheck] makes sure each endpoint is served by one.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(MeshProperties::class)
class MeshResilienceConfiguration {
    /** The startup line of the derived ladder, over the bound [MeshProperties]. */
    @Bean
    fun meshLadderLogger(meshProperties: MeshProperties): MeshLadderLogger = MeshLadderLogger(meshProperties)

    companion object {
        /**
         * The check that every mesh endpoint is served by exactly one client stack ([MeshOwnershipCheck]). Static, as a
         * `BeanFactoryPostProcessor` has to be: it runs before this configuration is instantiated. With both stacks in
         * the context, both import this class and the check exists once.
         */
        @Bean
        @JvmStatic
        fun meshOwnershipCheck(environment: Environment): MeshOwnershipCheck = MeshOwnershipCheck(environment)
    }
}
