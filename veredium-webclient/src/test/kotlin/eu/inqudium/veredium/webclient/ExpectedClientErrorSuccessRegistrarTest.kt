package eu.inqudium.veredium.webclient

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import eu.inqudium.veredium.webclient.mesh.MeshProperties
import eu.inqudium.veredium.webclient.resilience.ResilienceAmendmentApplier
import eu.inqudium.veredium.webclient.resilience.ResilienceAmendmentContributor
import io.github.resilience4j.bulkhead.Bulkhead
import io.github.resilience4j.bulkhead.BulkheadConfig
import io.github.resilience4j.bulkhead.BulkheadFullException
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.core.io.buffer.DataBufferLimitException
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.concurrent.TimeoutException

/**
 * Tests for [ExpectedClientErrorIgnoreRegistrar] - the third and, until now, untested consumer of
 * [HttpExchangeErrorPolicy].
 *
 * The policy object exists so that two derivations cannot disagree: the synchronous error log's severity
 * ([ResilientWebClient], pinned by its own log-level assertions in `ResilientWebClientIntegrationTest`) and
 * this breaker record predicate. A policy whose consumers drift apart is worse than no policy: the log would
 * call an exchange benign while the breaker charges it against the backend. (The exchange event legatium
 * writes is not a consumer - it classifies a 4xx answer as `rejected` with its status on its own terms.)
 *
 * The distinction that carries the most risk is 408/429. They are 4xx, so a naive "ignore all client errors"
 * would discount them - and a run of request timeouts or rate-limit rejections is exactly what a breaker must
 * react to. Nothing else in the suite pinned that boundary.
 *
 * Behaviour is asserted through the registered instance's config and through the real decorated pipeline, never
 * against the contents of a status set, so tightening the policy stays a one-line change.
 */
class ExpectedClientErrorSuccessRegistrarTest {
    private val alias = "api-example-com-GET-accounts"
    private val triplet = "/endpoint/api.example.com/GET/accounts/{accountId}"

    @Nested
    inner class Registration {
        @Test
        fun `should record a benign 4xx as a success so a business case cannot open the breaker`() {
            // What is tested: that the statuses the adapter contract documents as normal outcomes - a 404 for
            //   "does not exist", a 400 for a malformed body, 403, 409 - are not charged to the backend.
            // How success is judged: the registered instance's RECORD predicate rejects each of them, so they
//   enter the sliding window as successes rather than being kept out of it altogether.
            // Why it matters: a healthy backend returns 4xx for bad requests all day. Counting those toward the
            //   failure rate lets a 404-heavy lookup open the circuit against a backend that is perfectly up -
            //   a self-inflicted outage caused by ordinary business traffic.

            // Given
            val registry = CircuitBreakerRegistry.ofDefaults()

            // When
            applyOnce(registry, ExpectedClientErrorSuccessRegistrar(propsFor()))

            // Then
            assertThat(countsAsFailure(registry, response(404))).isFalse()
            assertThat(countsAsFailure(registry, response(400))).isFalse()
            assertThat(countsAsFailure(registry, response(403))).isFalse()
            assertThat(countsAsFailure(registry, response(409))).isFalse()
        }

        @Test
        fun `should still record 408 and 429 although both are 4xx`() {
            // What is tested: the boundary the policy draws INSIDE the 4xx range - 408 Request Timeout and 429
            //   Too Many Requests are operational signals, not business cases, and stay recorded.
            // How success is judged: the predicate rejects both, while a neighbouring status (404) is accepted -
            //   so the assertion cannot pass by the predicate simply ignoring nothing.
            // Why it matters: this is the one distinction a reimplementation would get wrong, and getting it
            //   wrong is invisible: the breaker would sit blind through exactly the two conditions it exists to
            //   react to - a backend timing out under load and a backend shedding load. A 408/429 burst would
            //   never open the circuit, so retries keep hammering a backend that is already asking for relief.

            // Given
            val registry = CircuitBreakerRegistry.ofDefaults()

            // When
            applyOnce(registry, ExpectedClientErrorSuccessRegistrar(propsFor()))

            // Then
            assertThat(countsAsFailure(registry, response(408))).isTrue()
            assertThat(countsAsFailure(registry, response(429))).isTrue()
            assertThat(countsAsFailure(registry, response(404)))
                .`as`("the control: the predicate does discount a benign 4xx, so the two above are a real boundary")
                .isFalse()
        }

        @Test
        fun `should still record a 5xx, a resilience timeout and a client-side failure`() {
            // The counterpart that keeps the tests above honest: the amendment must discount ONLY benign 4xx,
            // not disable failure recording. A bulkhead rejection is included deliberately - discounting it is
            // ClientSideFailureIgnoreRegistrar's job for its own reason ("the backend was never called"), and
            // THIS registrar must not quietly take it over: two contributors sharing one predicate would make
            // it impossible to reason about which policy is in force.
            val registry = CircuitBreakerRegistry.ofDefaults()

            applyOnce(registry, ExpectedClientErrorSuccessRegistrar(propsFor()))

            assertThat(countsAsFailure(registry, response(500))).isTrue()
            assertThat(countsAsFailure(registry, response(503))).isTrue()
            assertThat(countsAsFailure(registry, TimeoutException("budget blown"))).isTrue()
            assertThat(countsAsFailure(registry, bulkheadRejection())).isTrue()
        }

        @Test
        fun `should preserve an operator's own ignore list instead of replacing it`() {
            // What is tested: that the amendment is ADDITIVE. `CircuitBreakerConfig.Builder.ignoreExceptions(...)`
            //   overwrites, so a naive amendment drops whatever the operator configured; the predicate has to OR
            //   the base predicate back in.
            // How success is judged: after the amendment both the operator's exception and a benign 4xx are
            //   ignored, and an unrelated failure is still recorded.
            // Why it matters: this registrar rewrites a config it does not own. Clobbering the operator's list
            //   would silently re-arm the breaker against errors they deliberately excluded - visible to them
            //   only as unexplained open circuits.

            // Given: an instance already ignoring an exception of the operator's choosing
            val registry = CircuitBreakerRegistry.ofDefaults()
            registry.circuitBreaker(
                alias,
                CircuitBreakerConfig.custom().ignoreExceptions(IllegalStateException::class.java).build(),
            )

            // When
            applyOnce(registry, ExpectedClientErrorSuccessRegistrar(propsFor()))

            // Then
            assertThat(ignores(registry, IllegalStateException("operator's choice"))).isTrue()
            assertThat(countsAsFailure(registry, response(404))).isFalse()
            assertThat(countsAsFailure(registry, response(500))).isTrue()
        }

        @Test
        fun `should amend every configured mesh backend`() {
            // Given: two endpoints, so the registrar has to iterate rather than handle a single alias
            val registry = CircuitBreakerRegistry.ofDefaults()
            val props =
                MeshProperties(
                    defaults = MeshProperties.Defaults(individualRequestTimeoutMs = 1000),
                    endpoints =
                        listOf(
                            MeshProperties.EndpointConfig(triplet),
                            MeshProperties.EndpointConfig("/endpoint/api.example.com/POST/payments"),
                        ),
                )

            // When
            applyOnce(registry, ExpectedClientErrorSuccessRegistrar(props))

            // Then: every derived alias carries the ignore
            assertThat(countsAsFailure(registry, response(404))).isFalse()
            assertThat(countsAsFailure(registry, response(404), alias = "api-example-com-POST-payments")).isFalse()
        }

        @Test
        fun `should apply the ignore even when the mesh circuit-breaker toggle is off`() {
            // The toggle governs only whether mesh DERIVES the slow-call knobs; the breaker is in the call path
            // unconditionally. Gating this amendment on it would leave the invariant unmet in exactly the
            // default configuration - the one that ships.

            // Given: mesh defaults, i.e. the toggle off
            val registry = CircuitBreakerRegistry.ofDefaults()
            val props = propsFor()
            assertThat(props.resolveAll().getValue(triplet).circuitBreakerEnabled).isFalse()

            // When
            applyOnce(registry, ExpectedClientErrorSuccessRegistrar(props))

            // Then
            assertThat(countsAsFailure(registry, response(404))).isFalse()
        }
    }

    @Nested
    inner class CoexistenceWithClientSideFailureIgnore {
        // Both contributors amend the SAME breaker instances for different reasons, and Spring does not order
        // the beans it hands to the applier. The KDoc claims the two stack additively regardless of order; these
        // two tests are what makes that a fact rather than an intention.

        @Test
        fun `should stack with the client-side-failure ignore when it contributes first`() {
            // Given / When
            val registry = CircuitBreakerRegistry.ofDefaults()
            val props = propsFor()
            ResilienceAmendmentApplier.applyOnce(
                registry,
                TimeLimiterRegistry.ofDefaults(),
                ClientSideFailureIgnoreRegistrar(props),
                ExpectedClientErrorSuccessRegistrar(props),
            )

            // Then: both policies are in force, and neither widened into a blanket ignore
            assertThat(countsAsFailure(registry, response(404))).isFalse()
            assertThat(ignores(registry, bulkheadRejection())).isTrue()
            assertThat(ignores(registry, DataBufferLimitException("too big"))).isTrue()
            assertThat(ignores(registry, response(500))).isFalse()
            assertThat(ignores(registry, response(429))).isFalse()
        }

        @Test
        fun `should stack with the client-side-failure ignore when it contributes second`() {
            // Given / When: the opposite contribution order
            val registry = CircuitBreakerRegistry.ofDefaults()
            val props = propsFor()
            ResilienceAmendmentApplier.applyOnce(
                registry,
                TimeLimiterRegistry.ofDefaults(),
                ExpectedClientErrorSuccessRegistrar(props),
                ClientSideFailureIgnoreRegistrar(props),
            )

            // Then: identical outcome - neither contribution undoes the other
            assertThat(countsAsFailure(registry, response(404))).isFalse()
            assertThat(ignores(registry, bulkheadRejection())).isTrue()
            assertThat(ignores(registry, DataBufferLimitException("too big"))).isTrue()
            assertThat(countsAsFailure(registry, response(500))).isTrue()
            assertThat(countsAsFailure(registry, response(429))).isTrue()
        }
    }

    @Nested
    inner class DecoratedPipeline {
        @Test
        fun `should not count a 404 answer as a circuit-breaker failure`() {
            // End to end through the real decorator rather than against the predicate: the breaker's error hook
            // records every error and defers the record/ignore decision to the config, so this is what proves
            // the amendment reaches the instance the decorator actually uses.

            // Given
            val registry = CircuitBreakerRegistry.ofDefaults()
            val decorator = decoratorWith(registry)
            applyOnce(registry, ExpectedClientErrorSuccessRegistrar(propsFor()))

            // When: the backend answers "not found"
            val failure = catchThrowable { decorator.decorate(alias, Mono.error<String>(response(404))).block() }

            // Then: the caller still sees the error - the exchange did not succeed ...
            assertThat(failure).isInstanceOf(WebClientResponseException::class.java)
            // ... but the breaker did not hold it against the backend
            assertThat(registry.circuitBreaker(alias).metrics.numberOfFailedCalls).isZero()
        }

        @Test
        fun `should still count a 500 answer as a circuit-breaker failure`() {
            // Without this, a breaker that recorded nothing at all - the far worse defect, since it disables the
            // protection silently - would satisfy the zero-failure assertion above.

            // Given
            val registry = CircuitBreakerRegistry.ofDefaults()
            val decorator = decoratorWith(registry)
            applyOnce(registry, ExpectedClientErrorSuccessRegistrar(propsFor()))

            // When
            val failure = catchThrowable { decorator.decorate(alias, Mono.error<String>(response(500))).block() }

            // Then
            assertThat(failure).isInstanceOf(WebClientResponseException::class.java)
            assertThat(registry.circuitBreaker(alias).metrics.numberOfFailedCalls).isEqualTo(1)
        }
    }

    @Nested
    inner class PolicyLogLine {
        @Test
        fun `should name the amended backends in its policy log line`() {
            // What is tested: that the INFO line the registrar emits on startup names the backends it amended.
            // How success is judged: the formatted message contains both aliases, and does NOT name the
            //   client-side failure types - those belong to the sibling registrar's policy, not this one.
            // Why it matters: this line is the only startup evidence that the amendment ran at all, and an
            //   operator reads it to confirm WHICH backends are covered. A line that names the wrong thing is
            //   worse than none: it reports a policy that is not the one in force, and the placeholder/argument
            //   mismatch behind it is invisible in review because SLF4J neither fails nor warns on it.

            // Given: two endpoints and a captured logger
            val props =
                MeshProperties(
                    defaults = MeshProperties.Defaults(individualRequestTimeoutMs = 1000),
                    endpoints =
                        listOf(
                            MeshProperties.EndpointConfig(triplet),
                            MeshProperties.EndpointConfig("/endpoint/api.example.com/POST/payments"),
                        ),
                )

            // When
            applyOnce(CircuitBreakerRegistry.ofDefaults(), ExpectedClientErrorSuccessRegistrar(props))

            // Then
            val line = appender.list.single { it.level == Level.INFO }.formattedMessage
            assertThat(line).contains(alias, "api-example-com-POST-payments")
            assertThat(line)
                .`as`("the line must state THIS registrar's policy, not the sibling's client-side failure types")
                .doesNotContain("BulkheadFullException", "DataBufferLimitException")
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private val registrarLogger = LoggerFactory.getLogger(ExpectedClientErrorSuccessRegistrar::class.java) as Logger

    private val appender =
        ListAppender<ILoggingEvent>().apply {
            start()
            registrarLogger.addAppender(this)
        }

    @AfterEach
    fun detachAppender() {
        registrarLogger.detachAppender(appender)
        appender.stop()
    }

    /** Applies [contributor] the way the context does - through the real applier. */
    private fun applyOnce(
        registry: CircuitBreakerRegistry,
        contributor: ResilienceAmendmentContributor,
    ) = ResilienceAmendmentApplier.applyOnce(registry, TimeLimiterRegistry.ofDefaults(), contributor)

    /** Whether the registered instance's config classifies [error] as ignorable (not a backend failure). */
    private fun ignores(
        registry: CircuitBreakerRegistry,
        error: Throwable,
        alias: String = this.alias,
    ): Boolean =
        registry
            .circuitBreaker(alias)
            .circuitBreakerConfig.ignoreExceptionPredicate
            .test(error)

    /**
     * Whether the breaker would charge [error] to the backend.
     *
     * Reads the RECORD predicate, which is the whole point of this registrar: a benign 4xx must come back
     * `false` here - not recorded, therefore counted as a SUCCESS and entering the sliding window as one.
     * Asserting through [ignores] instead would pass for a benign 4xx that was merely kept out of the
     * window, which is the behaviour this registrar was changed away from.
     */
    private fun countsAsFailure(
        registry: CircuitBreakerRegistry,
        error: Throwable,
        alias: String = this.alias,
    ): Boolean =
        registry
            .circuitBreaker(alias)
            .circuitBreakerConfig.recordExceptionPredicate
            .test(error)

    /** A response exception in the shape the pipeline sees it - status only; no body is needed here. */
    private fun response(status: Int): WebClientResponseException = WebClientResponseException(status, "status $status", null, null, null)

    /** A real rejection instance - the type is only constructible through the bulkhead's factory. */
    private fun bulkheadRejection(): BulkheadFullException = BulkheadFullException.createBulkheadFullException(Bulkhead.of("probe", BulkheadConfig.ofDefaults()))

    /** A decorator over the supplied breaker registry; the other two operators stay out of the way. */
    private fun decoratorWith(registry: CircuitBreakerRegistry): ReactiveResilienceDecorator =
        ReactiveResilienceDecorator(
            registry,
            TimeLimiterRegistry.of(TimeLimiterConfig.custom().timeoutDuration(Duration.ofMinutes(5)).build()),
            BulkheadRegistry.ofDefaults(),
        )

    /** Props for the single accounts endpoint, mesh defaults otherwise. */
    private fun propsFor(): MeshProperties =
        MeshProperties(
            defaults = MeshProperties.Defaults(individualRequestTimeoutMs = 1000),
            endpoints = listOf(MeshProperties.EndpointConfig(triplet)),
        )
}
