package eu.inqudium.veredium.examples.decorator

import eu.inqudium.veredium.core.resilience.BlockingResilienceDecorator
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.common.circuitbreaker.configuration.CircuitBreakerConfigCustomizer
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import io.micrometer.observation.ObservationRegistry
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration

/**
 * The legacy customer directory: where it is, and how long one exchange may take. Bound from
 * `legacy-directory.*`.
 *
 * @property baseUrl Where the client connects - here the local sidecar, as for the client examples.
 * @property host The Host header the sidecar routes on; the native client has to set it itself.
 * @property connectTimeout HttpClient 4's connect timeout.
 * @property responseTimeout HttpClient 4's socket timeout: the bound on every read, the head's and the body's.
 *   It sits below the TimeLimiter budget, so normally the transport trips first.
 * @property maxConnections The pool size; matches the bulkhead, so a permit always finds a connection.
 */
@ConfigurationProperties("legacy-directory")
data class LegacyDirectoryProperties(
    val baseUrl: String = "http://localhost:8042",
    val host: String = "customers.example.com",
    val connectTimeout: Duration = Duration.ofMillis(100),
    val responseTimeout: Duration = Duration.ofMillis(700),
    val maxConnections: Int = 10,
)

/**
 * The wiring a host declares itself, because the decorator carries no Spring annotation (ADR-0002):
 *
 * - the decorator, over the auto-configured registries, with worker threads named after the call
 *   (`legacy-call-<n>`) and the host's observation registry when there is one. It is shut down with the
 *   context (`@PreDestroy` on [BlockingResilienceDecorator.shutdown]);
 * - the breaker customizer the decorator needs: its pipeline raises three failures itself - a full bulkhead, an
 *   interrupted caller, a call after shutdown - and a breaker that counted them would open against a backend
 *   that was never called. [BlockingResilienceDecorator.SELF_INFLICTED_FAILURES] lists them, and so does the
 *   "customer unknown" answer, which is a valid answer of the backend, not a fault;
 * - the native HttpClient 4, closed with the context.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(LegacyDirectoryProperties::class)
class DecoratorConfiguration {
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
            workerNamePrefix = "legacy-call-",
        )

    @Bean
    fun legacyDirectoryBreaker(): CircuitBreakerConfigCustomizer =
        CircuitBreakerConfigCustomizer.of(LegacyCustomerDirectory.NAME) { builder ->
            builder.ignoreExceptions(*(BlockingResilienceDecorator.SELF_INFLICTED_FAILURES + UnknownCustomerException::class.java).toTypedArray())
        }

    /**
     * HttpClient 4 with the directory's bounds and nothing it does on its own: no retries (a retried request would
     * run past the budget unseen, and the default handler retries an interrupted read's `SocketException`), no
     * redirects, no cookies.
     */
    @Bean
    fun legacyHttpClient(properties: LegacyDirectoryProperties): CloseableHttpClient =
        HttpClients
            .custom()
            .setConnectionManager(
                PoolingHttpClientConnectionManager().apply {
                    maxTotal = properties.maxConnections
                    defaultMaxPerRoute = properties.maxConnections
                },
            ).setDefaultRequestConfig(
                RequestConfig
                    .custom()
                    .setConnectTimeout(properties.connectTimeout.toMillis().toInt())
                    .setSocketTimeout(properties.responseTimeout.toMillis().toInt())
                    .setConnectionRequestTimeout(properties.connectTimeout.toMillis().toInt())
                    .setRedirectsEnabled(false)
                    .build(),
            ).disableAutomaticRetries()
            .disableCookieManagement()
            .build()
}
