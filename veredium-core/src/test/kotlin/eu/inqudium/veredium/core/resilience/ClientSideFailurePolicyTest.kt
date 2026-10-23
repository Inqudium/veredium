package eu.inqudium.veredium.core.resilience

import eu.inqudium.veredium.core.resilience.TestMesh.ACCOUNTS_ALIAS
import eu.inqudium.veredium.core.resilience.TestMesh.ACCOUNTS_TRIPLET
import eu.inqudium.veredium.core.resilience.TestMesh.PAYMENTS_ALIAS
import eu.inqudium.veredium.core.resilience.TestMesh.PAYMENTS_TRIPLET
import eu.inqudium.veredium.core.resilience.TestMesh.ignores
import eu.inqudium.veredium.core.resilience.TestMesh.propsFor
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * Tests for the client-side-failure verdict the circuit-breaker customizer installs - the shared RULE, exercised
 * with the stand-in stack of [TestVerdicts]. Each client module proves the same verdict through its own
 * decorator and with its own exception shapes (`RestClientVerdictsTest`, `WebClientVerdictsTest`); what is
 * pinned here is what both inherit: the fold onto the operator's list, the cause walk and its bound, and that
 * every alias gets it regardless of the mesh toggle.
 *
 * The tests deliberately assert the customizer's BEHAVIOUR (which exceptions the breaker ends up ignoring),
 * not the contents of a stack's failure list, so extending that list stays a one-line change and does not
 * drag a test rewrite behind it. The customizer is applied through [CustomizedRegistries], the way the Boot
 * configuration applies it while building each instance.
 */
class ClientSideFailurePolicyTest {
    @Nested
    inner class `The policy` {
        @Test
        fun `should require the buffer-limit type to be one of the client-side failures`() {
            // What is tested: the one consistency rule the policy's constructor enforces - the type asked for by
            //   isBufferLimitFailure must also be discounted by isClientSideFailure.
            // Success criteria: a policy whose limit type is missing from its list is rejected at construction.
            // Why it matters: a stack that named a limit type without listing it would log "raise the limit" for a
            //   failure its breaker still charges to the backend - two consumers of one policy disagreeing, which is
            //   exactly what the policy object exists to rule out.
            //
            // Given / When / Then
            assertThatThrownBy { ClientSideFailurePolicy(listOf(TimeoutException::class.java), TestBufferLimitException::class.java) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("TestBufferLimitException")
        }

        @Test
        fun `should tell the buffer-limit failure apart from the other client-side failures`() {
            // What is tested: the narrow sibling - isBufferLimitFailure matches the limit type (bare and as a cause)
            //   and nothing else on the list.
            // Success criteria: the limit failure answers true on both predicates, a bulkhead rejection only on
            //   the broad one.
            // Why it matters: only the limit failure warrants a line naming a configuration value; a bulkhead
            //   rejection says nothing about the response and must not trigger that line.
            //
            // Given: the shared test policy
            val policy = TestVerdicts.clientSideFailure

            // When / Then
            assertThat(policy.isBufferLimitFailure(TestBufferLimitException())).isTrue()
            assertThat(policy.isBufferLimitFailure(IllegalStateException("wrapped", TestBufferLimitException()))).isTrue()
            assertThat(policy.isClientSideFailure(TestBufferLimitException())).isTrue()
            assertThat(policy.isClientSideFailure(TestVerdicts.bulkheadRejection())).isTrue()
            assertThat(policy.isBufferLimitFailure(TestVerdicts.bulkheadRejection())).isFalse()
        }
    }

    @Nested
    inner class `The customized breaker` {
        @Test
        fun `should install the ignore even when the mesh circuit-breaker toggle is off`() {
            // What is tested: that the verdict is NOT gated on ResolvedTimeouts.circuitBreakerEnabled.
            // Success criteria: with the toggle at its default (off), the resulting config still ignores a
            //   BulkheadFullException.
            // Why it matters: the toggle governs only whether mesh DERIVES the slow-call knobs, while the breaker
            //   is in the call path unconditionally. Gating this on the toggle would leave the invariant unmet in
            //   exactly the default configuration - the one that actually ships.
            //
            // Given: mesh defaults, i.e. the circuit-breaker toggle off
            val registry = CircuitBreakerRegistry.ofDefaults()
            val props = propsFor()
            assertThat(props.resolveAll().getValue(ACCOUNTS_TRIPLET).circuitBreakerEnabled).isFalse()

            // When
            CustomizedRegistries.circuitBreakers(registry, props)

            // Then
            assertThat(ignores(registry, TestVerdicts.bulkheadRejection())).isTrue()
        }

        @Test
        fun `should preserve an operator's own ignore list instead of replacing it`() {
            // What is tested: that the verdict is ADDITIVE. `ignoreException(predicate)` REPLACES the predicate
            //   the builder holds - which is what the operator's `ignore-exceptions` became - so a naive
            //   customizer would silently drop it; the customizer reads the configured predicate first and ORs
            //   it back in.
            // Success criteria: after the customizer the config ignores BOTH the operator's exception and
            //   the bulkhead rejection, and still records an unrelated one.
            // Why it matters: the customizer shapes a config it does not own. Clobbering an operator's ignore
            //   list would silently re-arm the breaker against errors they deliberately excluded - visible to
            //   them only as unexplained open circuits.
            //
            // Given: what YAML produced already ignores an exception of the operator's choosing
            val registry = CircuitBreakerRegistry.ofDefaults()
            registry.circuitBreaker(ACCOUNTS_ALIAS, CircuitBreakerConfig.custom().ignoreExceptions(IllegalStateException::class.java).build())

            // When
            CustomizedRegistries.circuitBreakers(registry, propsFor())

            // Then: both are ignored and an unrelated exception is still recorded
            assertThat(ignores(registry, IllegalStateException("operator's choice"))).isTrue()
            assertThat(ignores(registry, TestVerdicts.bulkheadRejection())).isTrue()
            assertThat(ignores(registry, TimeoutException("budget blown"))).isFalse()
        }

        @Test
        fun `should ignore a client-side failure bare and when it arrives wrapped`() {
            // What is tested: that a listed failure is discounted in the shape it is thrown AND in the shape a
            //   wrapper would give it.
            // Success criteria: the customized breaker ignores both the bare TestBufferLimitException and the
            //   same instance two causes deep behind an IllegalStateException.
            // Why it matters: both stacks wrap - the blocking client every IOException of an exchange, the
            //   reactive client a codec failure into a response exception reporting 200 - and such a wrapping
            //   turns a class-based ignore into a silent no-op: Resilience4j's ignoreExceptions(Class...) matches
            //   the THROWN type only; only walking the cause chain survives a wrapper.
            //
            // Given: the customized breaker, and the failure bare and behind a wrapper
            val registry = CircuitBreakerRegistry.ofDefaults()
            CustomizedRegistries.circuitBreakers(registry, propsFor())
            val bare = TestBufferLimitException()
            val wrapped = IllegalStateException("I/O error on GET request", RuntimeException("body too large", bare))

            // When
            val ignoresBare = ignores(registry, bare)
            val ignoresWrapped = ignores(registry, wrapped)

            // Then
            assertThat(ignoresBare).isTrue()
            assertThat(ignoresWrapped).isTrue()
        }

        @Test
        fun `should customize every configured mesh backend`() {
            // What is tested: that `CustomizedRegistries.circuitBreakers` installs the client-side-failure ignore
            //   for EVERY mesh endpoint, not only the first.
            // Success criteria: with two endpoints, the breaker of each derived alias ignores a bulkhead rejection.
            // Why it matters: one customizer is registered per alias; a loop that stopped early would leave a
            //   backend whose breaker charges its own bulkhead's rejections to the backend and opens under load.
            //
            // Given: two endpoints, so there is one customizer per alias rather than a single one
            val registry = CircuitBreakerRegistry.ofDefaults()
            val props = propsFor(PAYMENTS_TRIPLET)

            // When
            CustomizedRegistries.circuitBreakers(registry, props)

            // Then: every derived alias carries the ignore
            assertThat(ignores(registry, TestVerdicts.bulkheadRejection())).isTrue()
            assertThat(ignores(registry, TestVerdicts.bulkheadRejection(), alias = PAYMENTS_ALIAS)).isTrue()
        }

        @Test
        fun `should carry the derived slow-call knobs and the ignore in the one customizer`() {
            // What is tested: that the verdict and the mesh derivation do not compete - they are ONE customizer
            //   per alias (Resilience4j admits exactly one per instance name), so there is no order in which one
            //   could undo the other.
            // Success criteria: with the circuit-breaker derivation enabled, the config carries the derived
            //   duration (800ms at individual 1000, maxRetries 0), the 100% pin of the inversion case, AND the
            //   ignore.
            // Why it matters: two customizers on one instance would need an order in which neither undoes the
            //   other; with one customizer the property holds by construction, and this test is what says so.
            //
            // Given: circuit-breaker derivation enabled
            val registry = CircuitBreakerRegistry.ofDefaults()

            // When
            CustomizedRegistries.circuitBreakers(registry, propsFor(circuitBreakerEnabled = true))

            // Then
            val config = registry.circuitBreaker(ACCOUNTS_ALIAS).circuitBreakerConfig
            assertThat(config.slowCallDurationThreshold).isEqualTo(Duration.ofMillis(800))
            assertThat(config.slowCallRateThreshold).isEqualTo(100f)
            assertThat(config.ignoreExceptionPredicate.test(TestVerdicts.bulkheadRejection())).isTrue()
        }
    }

    @Nested
    inner class `The bound of the cause walk` {
        // The cause walk in ClientSideFailurePolicy is bounded by MAX_CAUSE_DEPTH "so a pathological cause
        // chain cannot spin". Both ends of that bound are asserted here, through the customized instance's
        // predicate rather than the policy function, so these stay statements about the customizer's behaviour.

        @Test
        fun `should still discount a client-side failure at the deepest position the walk reaches`() {
            // What is tested: the cause walk at its deepest reach - it inspects MAX_CAUSE_DEPTH candidates, the
            //   thrown exception being the first, and the client-side failure sits at exactly that last position.
            // Success criteria: a TestBufferLimitException behind MAX_CAUSE_DEPTH - 1 wrappers is ignored.
            // Why it matters: a root cause at exactly that last position must still be found - otherwise the
            //   effective depth is one less than the constant says, and nobody would notice.
            //
            // Given: the customized breaker, and the failure behind MAX_CAUSE_DEPTH - 1 wrappers
            val registry = CircuitBreakerRegistry.ofDefaults()
            CustomizedRegistries.circuitBreakers(registry, propsFor())
            val atLimit = nested(ClientSideFailurePolicy.MAX_CAUSE_DEPTH - 1, TestBufferLimitException())

            // When
            val ignored = ignores(registry, atLimit)

            // Then
            assertThat(ignored).isTrue()
        }

        @Test
        fun `should stop looking one layer beyond the bound`() {
            // What is tested: that the bound is real - a client-side failure buried DEEPER than the walk goes is
            //   not found, and the failure is therefore recorded.
            // Success criteria: the same chain as above with one more wrapper is NOT ignored.
            // Why it matters: this documents the accepted limitation rather than leaving it to be discovered.
            //   Discounting is the exception to normal breaker behaviour, so falling back to "record it" past
            //   the bound is the safe direction - a breaker that counts a client-side failure trips slightly
            //   early, whereas an unbounded walk on a cyclic chain would hang the predicate inside the
            //   breaker's error hook, on a request-serving thread.
            //
            // Given: the customized breaker, and the failure behind MAX_CAUSE_DEPTH wrappers
            val registry = CircuitBreakerRegistry.ofDefaults()
            CustomizedRegistries.circuitBreakers(registry, propsFor())
            val beyondLimit = nested(ClientSideFailurePolicy.MAX_CAUSE_DEPTH, TestBufferLimitException())

            // When
            val ignored = ignores(registry, beyondLimit)

            // Then
            assertThat(ignored).isFalse()
        }

        @Test
        @Timeout(value = 10, unit = TimeUnit.SECONDS)
        fun `should terminate on a cyclic cause chain`() {
            // What is tested: that a cause chain which loops back on itself does not spin the walk.
            // Success criteria: the predicate returns a verdict at all (false - no client-side failure
            //   is in the cycle). A walk without a bound would not fail the assertion but spin forever; the @Timeout
            //   turns that spin into a test failure instead of a hung build.
            // Why it matters: the walk's own self-cause guard only catches a throwable
            //   that is its OWN cause; a two-element A -> B -> A cycle passes straight through it and is
            //   stopped by the depth bound alone. Since this predicate runs inside the circuit breaker's error
            //   hook on a request-serving thread, a spin there would not be a slow test - it would wedge the
            //   caller.
            //
            // Given: the customized breaker, and A -> B -> A, built the only way the JDK allows (initCause on a
            // cause-less throwable)
            val registry = CircuitBreakerRegistry.ofDefaults()
            CustomizedRegistries.circuitBreakers(registry, propsFor())
            val first = IllegalStateException("first")
            val second = IllegalStateException("second")
            first.initCause(second)
            second.initCause(first)

            // When / Then
            assertThat(ignores(registry, first)).isFalse()
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
}
