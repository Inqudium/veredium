package eu.inqudium.veredium.core.resilience

import eu.inqudium.veredium.core.mesh.MeshProperties
import org.springframework.beans.factory.config.BeanFactoryPostProcessor
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.core.env.Environment

/**
 * Fails the start when a mesh endpoint is not served by exactly one client stack - ADR-0003, "each mesh endpoint is
 * owned by one client stack".
 *
 * Each stack's [MeshResilienceRegistrar] declares the beans of the endpoints its stack owns
 * ([MeshProperties.endpointsOwnedBy]): those that name it in `stack`, and those that name no stack. With one client
 * module that is every endpoint. With both, an endpoint without `stack` is claimed twice - its breaker would get two
 * customizers with two stacks' verdicts - and an endpoint whose `stack` names a module the host does not run is
 * claimed by none, so nothing would serve it.
 *
 * A [BeanFactoryPostProcessor] runs after every bean definition is registered and before any singleton is created:
 * the start fails before Resilience4j builds a registry or a factory opens a pool. It reads bean definition NAMES
 * only - the stacks from the `meshResilienceStartupGuard.<stack>` definitions, the claims from the
 * `meshCircuitBreakerCustomizer.<stack>.<alias>` definitions ([MeshResilienceRegistrar.Companion]) - and binds
 * `app.mesh` from the [environment] as the registrars do. Nothing is instantiated, so no bean of a host can stand in
 * for a stack's verdicts here either.
 */
class MeshOwnershipCheck(
    private val environment: Environment,
) : BeanFactoryPostProcessor {
    override fun postProcessBeanFactory(beanFactory: ConfigurableListableBeanFactory) {
        val mesh =
            Binder
                .get(environment)
                .bind(MeshProperties.PREFIX, MeshProperties::class.java)
                .orElseGet { MeshProperties() }
        val problems = problems(mesh, beanFactory.beanDefinitionNames.toList())
        check(problems.isEmpty()) {
            "The mesh endpoints are not served by exactly one client stack each - ${problems.size} problem(s):\n  - " +
                problems.joinToString("\n  - ")
        }
    }

    companion object {
        /**
         * What is wrong with the ownership of [mesh]'s endpoints, given the bean definitions [beanNames]: one line per
         * endpoint that more than one stack, or no stack, claims. Empty when every endpoint has exactly one owner, and
         * when no stack is in the context at all (the mesh binding without a client module serves nothing to check).
         */
        fun problems(
            mesh: MeshProperties,
            beanNames: Collection<String>,
        ): List<String> {
            val stacks =
                beanNames
                    .filter { it.startsWith(MeshResilienceRegistrar.STARTUP_GUARD_BEAN_PREFIX) }
                    .map { it.removePrefix(MeshResilienceRegistrar.STARTUP_GUARD_BEAN_PREFIX) }
                    .sorted()
            if (stacks.isEmpty()) return emptyList()
            val claims: Map<String, List<String>> =
                beanNames
                    .filter { it.startsWith(MeshResilienceRegistrar.CIRCUIT_BREAKER_BEAN_PREFIX) }
                    .map { it.removePrefix(MeshResilienceRegistrar.CIRCUIT_BREAKER_BEAN_PREFIX) }
                    .mapNotNull { claim -> stacks.firstOrNull { claim.startsWith("$it.") }?.let { stack -> claim.removePrefix("$stack.") to stack } }
                    .groupBy({ it.first }, { it.second })
            return mesh.endpoints.mapIndexedNotNull { index, endpoint ->
                val alias = endpoint.effectiveAlias()
                val key = "${MeshProperties.PREFIX}.endpoints[$index].stack"
                val owners = claims[alias].orEmpty().sorted()
                when {
                    owners.size > 1 -> {
                        "mesh endpoint '$alias' (${endpoint.triplet}) is claimed by the stacks $owners: with more than " +
                            "one client stack in the context, $key must name the one that serves it"
                    }

                    owners.isEmpty() -> {
                        "mesh endpoint '$alias' (${endpoint.triplet}) is served by no stack: $key is " +
                            "'${endpoint.stack}', but the stacks in the context are $stacks"
                    }

                    else -> {
                        null
                    }
                }
            }
        }
    }
}
