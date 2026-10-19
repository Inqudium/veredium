package eu.inqudium.veredium.restclient

import eu.inqudium.veredium.core.resilience.BlockingResilienceDecorator
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import io.micrometer.observation.ObservationRegistry
import org.springframework.beans.factory.ObjectProvider
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * This module's instance of the core's [BlockingResilienceDecorator]: the pipeline every call of a
 * [ResilientRestClient] runs through, over the three registries `resilience4j-spring-boot4` auto-configures.
 *
 * The decorator carries no Spring annotation of its own - it is a plain component of `veredium-core`, usable
 * without any client - so the bean is declared here, and here is where this module names its workers:
 * `restclient-call-<n>` is the thread name on every log line written during an exchange, which is how a reader
 * tells this module's calls from a host's own decorated calls in the same JVM. The observation registry is
 * optional (consequence 2 in the decorator's KDoc): absent, the exchange's observation starts as its own root.
 */
@Configuration(proxyBeanMethods = false)
class RestClientResilienceConfig {
    @Bean
    fun blockingResilienceDecorator(
        circuitBreakerRegistry: CircuitBreakerRegistry,
        timeLimiterRegistry: TimeLimiterRegistry,
        bulkheadRegistry: BulkheadRegistry,
        observationRegistry: ObjectProvider<ObservationRegistry>,
    ): BlockingResilienceDecorator =
        BlockingResilienceDecorator(
            circuitBreakerRegistry,
            timeLimiterRegistry,
            bulkheadRegistry,
            observationRegistry.ifAvailable,
            WORKER_NAME_PREFIX,
        )

    companion object {
        /** The name prefix of this module's worker threads, as thread dumps and the exchange event's `thread` show it. */
        const val WORKER_NAME_PREFIX: String = "restclient-call-"
    }
}
