package eu.inqudium.veredium.core.resilience

import eu.inqudium.veredium.core.mesh.MeshResilienceConfiguration
import io.github.resilience4j.bulkhead.Bulkhead
import io.github.resilience4j.bulkhead.BulkheadConfig
import io.github.resilience4j.bulkhead.BulkheadFullException
import org.springframework.context.annotation.Bean
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

/** What a stack's `*VerdictsConfig` is: the verdicts bean next to the import of the shared wiring. */
@Configuration(proxyBeanMethods = false)
@Import(MeshResilienceConfiguration::class)
class TestVerdictsConfig {
    @Bean
    fun breakerVerdicts(): BreakerVerdicts = TestVerdicts.breakerVerdicts
}
