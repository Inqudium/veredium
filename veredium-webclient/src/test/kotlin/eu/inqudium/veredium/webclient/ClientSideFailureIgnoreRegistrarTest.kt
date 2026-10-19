package eu.inqudium.veredium.webclient

import eu.inqudium.veredium.webclient.mesh.MeshProperties
import eu.inqudium.veredium.webclient.mesh.MeshTimeoutRegistrar
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
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.springframework.core.io.buffer.DataBufferLimitException
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import java.time.Duration
import java.util.concurrent.TimeoutException

/**
 * Tests for [ClientSideFailureIgnoreRegistrar] and the invariant it protects in [ReactiveResilienceDecorator]:
 * a failure this pipeline raises ITSELF must never be charged to the backend.
 *
 * `BulkheadFullException` is the currently registered case and the one exercised here. The decorator composes
 * `CircuitBreaker( TimeLimiter( Bulkhead( call ) ) )`, so a rejection travels outwards through the breaker's
 * error hook, which records every error and defers the record/ignore decision to the config. Resilience4j's
 * default ignore list is empty, so without the registrar a concurrency peak - the backend perfectly healthy,
 * indeed never even called - drives the failure rate up and eventually opens the breaker, turning load
 * shedding into an outage. That is a heisenbug: it needs real contention and never shows up in a
 * single-threaded happy-path test, which is why the contention here is produced deterministically (a permit
 * held by an uncompleted source) rather than by racing threads.
 *
 * The tests deliberately assert the registrar's BEHAVIOUR (which exceptions the breaker ends up ignoring),
 * not the contents of `CLIENT_SIDE_FAILURES`, so extending that list stays a one-line change and does not
 * drag a test rewrite behind it.
 */
class ClientSideFailureIgnoreRegistrarTest {
    private val alias = "api-example-com-GET-accounts"
    private val triplet = "/endpoint/api.example.com/GET/accounts/{accountId}"

    @Nested
    inner class DecoratedPipeline {
        @Test
        fun `should not count a bulkhead rejection as a circuit-breaker failure`() {
            // What is tested: end to end through the real decorator - when the bulkhead rejects a call because
            //   its only permit is taken, the circuit breaker must not record that rejection as a failure.
            // How success is judged: the second call fails with a BulkheadFullException (proving the rejection
            //   really happened, so the assertion below is not vacuous) while the breaker's failed-call counter
            //   stays at zero.
            // Why it matters: this is the escalation path. Counting the rejection means a transient concurrency
            //   peak opens the breaker and takes the backend down for waitDurationInOpenState - the load
            //   shedding meant to absorb the peak amplifies it into an outage instead.

            // Given: a single-permit bulkhead that rejects immediately, and the registrar has run
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            val decorator = decoratorWith(circuitBreakerRegistry, singlePermitBulkheads())
            applyOnce(circuitBreakerRegistry, ClientSideFailureIgnoreRegistrar(propsFor()))
            val circuitBreaker = circuitBreakerRegistry.circuitBreaker(alias)

            // And: the only permit is held by a call that has not completed
            val held = Sinks.one<String>()
            val holding = decorator.decorate(alias, held.asMono()).subscribe({ }, { })

            try {
                // When: a second call is decorated and subscribed
                val rejection = catchThrowable { decorator.decorate(alias, Mono.just("second")).block() }

                // Then: it was rejected by the bulkhead ...
                assertThat(rejection).isInstanceOf(BulkheadFullException::class.java)
                // ... and the breaker did not hold that against the backend
                assertThat(circuitBreaker.metrics.numberOfFailedCalls).isZero()
            } finally {
                holding.dispose()
                held.tryEmitEmpty()
            }
        }

        @Test
        fun `should still count a genuine backend error as a circuit-breaker failure`() {
            // What is tested: the counterpart that keeps the test above honest - the amended config must ignore
            //   ONLY the bulkhead rejection, not disable failure recording as such.
            // How success is judged: a plain backend error through the same decorated pipeline advances the
            //   failed-call counter to exactly one.
            // Why it matters: without this, a breaker that recorded nothing at all - the far worse defect, since
            //   it disables the protection silently - would satisfy the zero-failure assertion above.

            // Given: the same wiring, with the registrar applied
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            val decorator = decoratorWith(circuitBreakerRegistry, singlePermitBulkheads())
            applyOnce(circuitBreakerRegistry, ClientSideFailureIgnoreRegistrar(propsFor()))
            val circuitBreaker = circuitBreakerRegistry.circuitBreaker(alias)

            // When: a call fails for a real reason
            val failure =
                catchThrowable {
                    decorator.decorate(alias, Mono.error<String>(IllegalStateException("backend exploded"))).block()
                }

            // Then: the error surfaces and the breaker counts it
            assertThat(failure).isInstanceOf(IllegalStateException::class.java)
            assertThat(circuitBreaker.metrics.numberOfFailedCalls).isEqualTo(1)
        }
    }

    @Nested
    inner class Registration {
        @Test
        fun `should apply the ignore even when the mesh circuit-breaker toggle is off`() {
            // What is tested: that the amendment is NOT gated on ResolvedTimeouts.circuitBreakerEnabled.
            // How success is judged: with the toggle at its default (off), the resulting config still ignores a
            //   BulkheadFullException.
            // Why it matters: the toggle governs only whether mesh DERIVES the slow-call knobs, while the breaker
            //   is in the call path unconditionally. Gating this on the toggle would leave the invariant unmet in
            //   exactly the default configuration - the one that actually ships.

            // Given: mesh defaults, i.e. the circuit-breaker toggle off
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            val props = propsFor()
            assertThat(props.resolveAll().getValue(triplet).circuitBreakerEnabled).isFalse()

            // When
            applyOnce(circuitBreakerRegistry, ClientSideFailureIgnoreRegistrar(props))

            // Then
            assertThat(ignores(circuitBreakerRegistry, alias, bulkheadRejection())).isTrue()
        }

        @Test
        fun `should preserve an operator's own ignore list instead of replacing it`() {
            // What is tested: that the amendment is ADDITIVE. CircuitBreakerConfig.Builder.ignoreExceptions(...)
            //   overwrites the class array, so a naive amendment would silently drop whatever the operator had
            //   configured; from(base) copies base's already-BUILT predicate and build() ORs the two, which is
            //   what makes it additive.
            // How success is judged: after the amendment the config ignores BOTH the operator's exception and the
            //   bulkhead rejection, and still records an unrelated one.
            // Why it matters: this registrar rewrites a config it does not own. Clobbering an operator's ignore
            //   list would silently re-arm the breaker against errors they had deliberately excluded - a
            //   regression they would only notice as unexplained open circuits.

            // Given: an instance whose config already ignores an exception of the operator's choosing
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            circuitBreakerRegistry.circuitBreaker(
                alias,
                CircuitBreakerConfig.custom().ignoreExceptions(IllegalStateException::class.java).build(),
            )

            // When
            applyOnce(circuitBreakerRegistry, ClientSideFailureIgnoreRegistrar(propsFor()))

            // Then: both are ignored, and an unrelated exception is still recorded
            assertThat(ignores(circuitBreakerRegistry, alias, IllegalStateException("operator's choice"))).isTrue()
            assertThat(ignores(circuitBreakerRegistry, alias, bulkheadRejection())).isTrue()
            assertThat(ignores(circuitBreakerRegistry, alias, TimeoutException("budget blown"))).isFalse()
        }

        @Test
        fun `should ignore a codec buffer-limit failure even though it arrives wrapped as a 200 response`() {
            // What is tested: that the buffer-limit failure is discounted in the shape it ACTUALLY arrives in.
            // How success is judged: a WebClientResponseException carrying a DataBufferLimitException as its
            //   CAUSE is ignored - not the bare exception, which is not what the pipeline ever sees.
            // Why it matters: Spring wraps a codec failure on an otherwise successful response into a
            //   WebClientResponseException reporting 200 OK, with the real cause underneath. Resilience4j's
            //   ignoreExceptions(Class...) matches the THROWN type only, so listing DataBufferLimitException
            //   there would have been a silent no-op - present in the code, visible in review, doing nothing.
            //   Only walking the cause chain reaches it.
            //
            // Given
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            applyOnce(circuitBreakerRegistry, ClientSideFailureIgnoreRegistrar(propsFor()))

            // When: the failure in the exact shape the client produces it
            val wrapped =
                WebClientResponseException(
                    200,
                    "OK",
                    null,
                    null,
                    null,
                ).initCause(DataBufferLimitException("Exceeded limit on max bytes to buffer : 8192")) as Throwable

            // Then
            assertThat(ignores(circuitBreakerRegistry, alias, wrapped)).isTrue()
            // ... and the bare form too, so the rule does not depend on the wrapping staying as it is
            assertThat(ignores(circuitBreakerRegistry, alias, DataBufferLimitException("too big"))).isTrue()
        }

        @Test
        fun `should still record a genuine backend failure that carries a large error body`() {
            // What is tested: the boundary of the cause walk - that discounting buffer failures does not leak
            //   into discounting real ones.
            // How success is judged: a 5xx is recorded, including one whose error body is large. Measured
            //   beforehand: such a response yields a plain WebClientResponseException with NO
            //   DataBufferLimitException underneath, because Spring does not fail on an oversized error body.
            // Why it matters: this is the failure mode that would make the whole breaker useless - if a 5xx
            //   ever started carrying a buffer failure as its cause, the cause walk would silently discount
            //   genuine outages. Nothing in the code prevents that; this test is what would catch it.
            //
            // Given
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            applyOnce(circuitBreakerRegistry, ClientSideFailureIgnoreRegistrar(propsFor()))

            // When / Then: a server error stays a server error
            val serverError = WebClientResponseException(500, "Internal Server Error", null, ByteArray(64 * 1024), null)
            assertThat(ignores(circuitBreakerRegistry, alias, serverError)).isFalse()
            assertThat(ignores(circuitBreakerRegistry, alias, TimeoutException("budget blown"))).isFalse()
        }

        @Test
        fun `should amend every configured mesh backend`() {
            // Given: two endpoints, so the registrar has to iterate rather than handle a single alias
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            val secondTriplet = "/endpoint/api.example.com/POST/payments"
            val props =
                MeshProperties(
                    defaults = MeshProperties.Defaults(individualRequestTimeoutMs = 1000),
                    endpoints =
                        listOf(
                            MeshProperties.EndpointConfig(triplet),
                            MeshProperties.EndpointConfig(secondTriplet),
                        ),
                )

            // When
            applyOnce(circuitBreakerRegistry, ClientSideFailureIgnoreRegistrar(props))

            // Then: every derived alias carries the ignore
            assertThat(ignores(circuitBreakerRegistry, alias, bulkheadRejection())).isTrue()
            assertThat(ignores(circuitBreakerRegistry, "api-example-com-POST-payments", bulkheadRejection())).isTrue()
        }
    }

    @Nested
    inner class CauseChainBoundary {
        // The cause walk in ClientSideFailurePolicy is bounded by MAX_CAUSE_DEPTH "so a pathological cause
        // chain cannot spin". Both ends of that bound are asserted here, through the registrar's predicate
        // rather than the policy function, so these stay statements about the shipped behaviour.

        @Test
        fun `should still discount a client-side failure at the deepest position the walk reaches`() {
            // The walk inspects MAX_CAUSE_DEPTH candidates, the thrown exception being the first. A root cause
            // at exactly that last position must still be found - otherwise the effective depth is one less
            // than the constant says, and nobody would notice.
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            applyOnce(circuitBreakerRegistry, ClientSideFailureIgnoreRegistrar(propsFor()))

            val atLimit = nested(ClientSideFailurePolicy.MAX_CAUSE_DEPTH - 1, DataBufferLimitException("too big"))

            assertThat(ignores(circuitBreakerRegistry, alias, atLimit)).isTrue()
        }

        @Test
        fun `should stop looking one layer beyond the bound`() {
            // What is tested: that the bound is real - a client-side failure buried DEEPER than the walk goes is
            //   not found, and the failure is therefore recorded.
            // How success is judged: the same chain as above with one more wrapper is NOT ignored.
            // Why it matters: this documents the accepted limitation rather than leaving it to be discovered.
            //   Discounting is the exception to normal breaker behaviour, so falling back to "record it" past
            //   the bound is the safe direction - a breaker that counts a client-side failure trips slightly
            //   early, whereas an unbounded walk on a cyclic chain would hang the predicate inside the
            //   breaker's error hook, on a request-serving thread.
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            applyOnce(circuitBreakerRegistry, ClientSideFailureIgnoreRegistrar(propsFor()))

            val beyondLimit = nested(ClientSideFailurePolicy.MAX_CAUSE_DEPTH, DataBufferLimitException("too big"))

            assertThat(ignores(circuitBreakerRegistry, alias, beyondLimit)).isFalse()
        }

        @Test
        fun `should terminate on a cyclic cause chain`() {
            // 1. What is tested: that a cause chain which loops back on itself does not spin the walk.
            // 2. How success is judged: the predicate returns a verdict at all (false - no client-side failure
            //    is in the cycle). A regression here does not fail the assertion, it hangs the test, which is
            //    the honest signal for an infinite loop.
            // 3. Why it matters: the walk's own guard (`takeIf { it !== candidate }`) only catches a throwable
            //    that is its OWN cause; a two-element A -> B -> A cycle passes straight through it and is
            //    stopped by the depth bound alone. Since this predicate runs inside the circuit breaker's error
            //    hook on a request-serving thread, a spin there would not be a slow test - it would wedge the
            //    caller.
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            applyOnce(circuitBreakerRegistry, ClientSideFailureIgnoreRegistrar(propsFor()))

            // Given: A -> B -> A, built the only way the JDK allows (initCause on a cause-less throwable)
            val first = IllegalStateException("first")
            val second = IllegalStateException("second")
            first.initCause(second)
            second.initCause(first)

            assertThat(ignores(circuitBreakerRegistry, alias, first)).isFalse()
        }

        /** A chain of [layers] wrappers around [root]; the returned throwable is the outermost one. */
        private fun nested(
            layers: Int,
            root: Throwable,
        ): Throwable {
            var current = root
            repeat(layers) { current = IllegalStateException("layer", current) }
            return current
        }
    }

    @Nested
    inner class CoexistenceWithMeshTimeoutRegistrar {
        // Both contributors target the same CircuitBreaker instances, and Spring does not order the beans it
        // hands to the applier. The two tests below pin that the result does not depend on which contributes
        // first: the applier folds the amendments, each seeing the previous one's result, and registers ONCE.
        // Without this property the shipped behaviour would hinge on bean registration order - the kind of
        // dependency that holds in the test JVM and breaks after an unrelated refactor.

        @Test
        fun `should keep the derived slow-call config when mesh contributes first`() {
            // Given: circuit-breaker derivation enabled
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            val props = propsFor(circuitBreakerEnabled = true)

            // When: the mesh contribution is collected BEFORE the ignore rule's
            ResilienceAmendmentApplier.applyOnce(
                circuitBreakerRegistry,
                TimeLimiterRegistry.ofDefaults(),
                MeshTimeoutRegistrar(props),
                ClientSideFailureIgnoreRegistrar(props),
            )

            // Then: the mesh-derived knobs survive AND the ignore is in place
            val config = circuitBreakerRegistry.circuitBreaker(alias).circuitBreakerConfig
            assertThat(config.slowCallDurationThreshold).isEqualTo(Duration.ofMillis(800))
            assertThat(config.slowCallRateThreshold).isEqualTo(100f)
            assertThat(config.ignoreExceptionPredicate.test(bulkheadRejection())).isTrue()
        }

        @Test
        fun `should keep the ignore when mesh contributes second`() {
            // Given: the same setup with the opposite contribution order
            val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
            val props = propsFor(circuitBreakerEnabled = true)

            // When
            ResilienceAmendmentApplier.applyOnce(
                circuitBreakerRegistry,
                TimeLimiterRegistry.ofDefaults(),
                ClientSideFailureIgnoreRegistrar(props),
                MeshTimeoutRegistrar(props),
            )

            // Then: identical outcome - neither contribution undoes the other
            val config = circuitBreakerRegistry.circuitBreaker(alias).circuitBreakerConfig
            assertThat(config.slowCallDurationThreshold).isEqualTo(Duration.ofMillis(800))
            assertThat(config.slowCallRateThreshold).isEqualTo(100f)
            assertThat(config.ignoreExceptionPredicate.test(bulkheadRejection())).isTrue()
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /**
     * Applies [contributor] the way the context does - through the real applier - with a throwaway time-limiter
     * registry, since none of these tests is about time limiters.
     */
    private fun applyOnce(
        circuitBreakerRegistry: CircuitBreakerRegistry,
        contributor: ResilienceAmendmentContributor,
    ) = ResilienceAmendmentApplier.applyOnce(circuitBreakerRegistry, TimeLimiterRegistry.ofDefaults(), contributor)

    /** Whether the registered instance's config classifies [error] as ignorable (not a backend failure). */
    private fun ignores(
        registry: CircuitBreakerRegistry,
        alias: String,
        error: Throwable,
    ): Boolean =
        registry
            .circuitBreaker(alias)
            .circuitBreakerConfig.ignoreExceptionPredicate
            .test(error)

    /** A real rejection instance - the exception type is only constructible through the bulkhead's factory. */
    private fun bulkheadRejection(): BulkheadFullException = BulkheadFullException.createBulkheadFullException(Bulkhead.of("probe", BulkheadConfig.ofDefaults()))

    /** A registry whose bulkheads hold exactly one permit and reject the next caller without waiting. */
    private fun singlePermitBulkheads(): BulkheadRegistry =
        BulkheadRegistry.of(
            BulkheadConfig
                .custom()
                .maxConcurrentCalls(1)
                .maxWaitDuration(Duration.ZERO)
                .build(),
        )

    /**
     * A decorator over the supplied registries. The time limiter is deliberately generous: these tests hold a
     * permit open on purpose, and a short budget would cancel that call and free the permit mid-test, turning a
     * deterministic rejection into a race.
     */
    private fun decoratorWith(
        circuitBreakerRegistry: CircuitBreakerRegistry,
        bulkheadRegistry: BulkheadRegistry,
    ): ReactiveResilienceDecorator =
        ReactiveResilienceDecorator(
            circuitBreakerRegistry,
            TimeLimiterRegistry.of(TimeLimiterConfig.custom().timeoutDuration(Duration.ofMinutes(5)).build()),
            bulkheadRegistry,
        )

    /** Props for the single accounts endpoint; individual=1000 at maxRetries=0 derives slowCall=800ms. */
    private fun propsFor(circuitBreakerEnabled: Boolean = false): MeshProperties =
        MeshProperties(
            defaults =
                MeshProperties.Defaults(
                    individualRequestTimeoutMs = 1000,
                    circuitBreaker = MeshProperties.BackendToggle(enabled = circuitBreakerEnabled),
                ),
            endpoints = listOf(MeshProperties.EndpointConfig(triplet)),
        )
}
