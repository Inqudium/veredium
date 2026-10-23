package eu.inqudium.veredium.core.resilience

import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.mesh.MeshResilienceConfiguration
import io.github.resilience4j.bulkhead.Bulkhead
import io.github.resilience4j.bulkhead.BulkheadConfig
import io.github.resilience4j.bulkhead.BulkheadFullException
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import

/**
 * A response exception in the shape a client stack would throw it - status only. Stands in for Spring's
 * `RestClientResponseException` / `WebClientResponseException`, which this module does not know.
 */
class TestResponseException(
    val status: Int,
) : RuntimeException("status $status")

/** The stand-in for a stack's buffer-limit exception (`ResponseBodyLimitExceededException` / `DataBufferLimitException`). */
class TestBufferLimitException(
    message: String = "body too large",
) : RuntimeException(message)

/**
 * The verdicts of a stand-in client stack, so the shared rules can be exercised without either real stack on
 * the classpath. Everything a real stack's `*Verdicts` object provides, in the simplest shape that provides it.
 */
object TestVerdicts {
    val httpExchangeError: HttpExchangeErrorPolicy = HttpExchangeErrorPolicy { error -> (error as? TestResponseException)?.status }

    val clientSideFailure: ClientSideFailurePolicy =
        ClientSideFailurePolicy(
            clientSideFailures = listOf(BulkheadFullException::class.java, TestBufferLimitException::class.java),
            bufferLimitFailure = TestBufferLimitException::class.java,
        )

    val breakerVerdicts: BreakerVerdicts =
        BreakerVerdicts(
            httpExchangeError = httpExchangeError,
            clientSideFailure = clientSideFailure,
            benignClientErrorProbe = TestResponseException(404),
        )

    /** A real rejection instance - the type is only constructible through a bulkhead's factory. */
    fun bulkheadRejection(): BulkheadFullException = BulkheadFullException.createBulkheadFullException(Bulkhead.of("probe", BulkheadConfig.ofDefaults()))
}

/** What a stack's `*MeshRegistrar` is: the shared registrar with the stack's name and verdicts handed in. */
class TestMeshRegistrar : MeshResilienceRegistrar(STACK, TestVerdicts.breakerVerdicts) {
    companion object {
        /** The stand-in stack's name, as `restclient` / `webclient` are the real ones'. */
        const val STACK: String = "test"
    }
}

/** What a stack's configuration does for the mesh: import the shared configuration next to its registrar. */
@Configuration(proxyBeanMethods = false)
@Import(MeshResilienceConfiguration::class, TestMeshRegistrar::class)
class TestStackConfiguration

/** A second stand-in stack, for a context with two stacks - the case of a host that depends on both client modules. */
class OtherMeshRegistrar : MeshResilienceRegistrar(STACK, TestVerdicts.breakerVerdicts) {
    companion object {
        /** The second stand-in stack's name. */
        const val STACK: String = "other"
    }
}

/** The second stand-in stack's configuration, importing the shared mesh configuration as [TestStackConfiguration] does. */
@Configuration(proxyBeanMethods = false)
@Import(MeshResilienceConfiguration::class, OtherMeshRegistrar::class)
class OtherStackConfiguration

/**
 * The mesh most tests of this package are built on: the accounts endpoint, optionally next to further
 * endpoints, with the anchor at 1000 ms - which at the default maxRetries=0 derives timeLimiter=1800ms and
 * slowCall=800ms.
 */
object TestMesh {
    const val ACCOUNTS_TRIPLET = "/endpoint/api.example.com/GET/accounts/{accountId}"
    const val ACCOUNTS_ALIAS = "api-example-com-GET-accounts"
    const val PAYMENTS_TRIPLET = "/endpoint/api.example.com/POST/payments"
    const val PAYMENTS_ALIAS = "api-example-com-POST-payments"

    /**
     * Props for the accounts endpoint and any [more] triplets, with the circuit-breaker derivation set to
     * [circuitBreakerEnabled] and a pool of [maxConnections] (default: the Kotlin default of 100).
     */
    fun propsFor(
        vararg more: String,
        circuitBreakerEnabled: Boolean = false,
        maxConnections: Int = MeshProperties.Transport().maxConnections,
    ): MeshProperties =
        MeshProperties(
            defaults =
                MeshProperties.Defaults(
                    individualRequestTimeoutMs = 1000,
                    circuitBreaker = MeshProperties.BackendToggle(enabled = circuitBreakerEnabled),
                    transport = MeshProperties.Transport(maxConnections = maxConnections),
                ),
            endpoints = (listOf(ACCOUNTS_TRIPLET) + more).map { MeshProperties.EndpointConfig(it) },
        )

    /** Whether the instance registered under [alias] classifies [error] as ignorable (not a backend failure). */
    fun ignores(
        registry: CircuitBreakerRegistry,
        error: Throwable,
        alias: String = ACCOUNTS_ALIAS,
    ): Boolean =
        registry
            .circuitBreaker(alias)
            .circuitBreakerConfig.ignoreExceptionPredicate
            .test(error)
}
