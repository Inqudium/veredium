package eu.inqudium.veredium.webclient

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.springframework.boot.context.properties.bind.Bindable
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.context.properties.source.ConfigurationPropertySources
import org.springframework.boot.env.YamlPropertySourceLoader
import org.springframework.core.io.ClassPathResource
import org.springframework.util.unit.DataSize
import java.time.Duration

/**
 * Tests for the binding-time guards of [WebClientProperties].
 *
 * The class carries no behaviour beyond its invariants, so this is what there is to test: that a value which
 * cannot be meant is rejected WHERE IT IS SET, and that the message names the property key the operator has
 * to edit rather than the Kotlin field they cannot see.
 */
class WebClientPropertiesTest {
    @Nested
    inner class MaxInMemorySize {
        @Test
        fun `should reject a limit of zero, which would fail every call as a client-side refusal`() {
            // What is tested: the lower bound of the codec limit.
            // How success is judged: binding throws, naming `app.webclient.max-in-memory-size`.
            // Why this case is important: this is the value whose misconfiguration is hardest to trace. Zero
            //   binds happily, and then every response exceeds the buffer and fails with a
            //   DataBufferLimitException - the one exception ClientSideFailureIgnoreRegistrar keeps OFF the
            //   circuit breaker on purpose. So every backend fails, no breaker opens, and the monitoring
            //   picture says "the backends are down". The guard is what keeps the diagnosis pointing at the
            //   configuration instead.
            //
            // Given / When / Then
            assertThatThrownBy { WebClientProperties(maxInMemorySize = DataSize.ofBytes(0)) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("app.webclient.max-in-memory-size")
        }

        @Test
        fun `should reject a limit past Int MAX_VALUE, which would wrap to a negative buffer`() {
            // What is tested: the UPPER bound, which exists for a reason that is invisible at this class -
            //   ResilientWebClientFactory narrows the value to an Int for the codec and the diary's capture
            //   bound.
            // How success is judged: 3 GB is rejected here, rather than silently becoming a negative Int later.
            // Why this case is important: without the guard, a value chosen to be generous would end up at the
            //   zero case above, by a route nobody would suspect from reading the configured number.
            //
            // Given / When / Then
            assertThatThrownBy { WebClientProperties(maxInMemorySize = DataSize.ofGigabytes(3)) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("app.webclient.max-in-memory-size")
        }

        @Test
        fun `should accept the shipped default and the extremes that are still meaningful`() {
            // Why this test exists: a guard that rejects everything would satisfy the two tests above. This
            // states where the line actually runs - one byte and Int.MAX_VALUE bytes are both admitted.
            //
            // Given / When / Then
            assertThatCode {
                WebClientProperties()
                WebClientProperties(maxInMemorySize = DataSize.ofBytes(1))
                WebClientProperties(maxInMemorySize = DataSize.ofBytes(Int.MAX_VALUE.toLong()))
            }.doesNotThrowAnyException()
        }

        @Test
        fun `should default to 256 KB, the size one oversized access-profile response needed`() {
            // What is tested: the shipped default of the codec limit, as a value in its own right rather than
            //   as a by-product of the range check above.
            // How success is judged: the default is 256 KB - Spring's own ExchangeStrategies default - and
            //   NOT the 8 KB this property shipped with until 2026-08-08.
            // Why this case is important: the old value was pinned by a bare equality assertion, which catches
            //   an accidental change but says nothing about whether the number is right. It was not. On
            //   2026-08-07 one access-profile response exceeded 8 KB; the backend answered 200 OK in
            //   130-260 ms and the codec aborted mid-buffer on every one of the five retries a backfill row
            //   is allowed, which rejected the row. The limit applies per concurrently buffered response and
            //   the shared pool is bounded at 100 connections, so this default also bounds the worst case at
            //   ~25 MB of response buffers - the reason it is not simply raised further.
            //
            // Given / When / Then
            assertThat(WebClientProperties().maxInMemorySize)
                .isEqualTo(DataSize.ofKilobytes(256))
                .isNotEqualTo(DataSize.ofKilobytes(8))
        }

        @Test
        fun `should bind the shipped application yml to the same 256 KB ceiling`() {
            val sources = YamlPropertySourceLoader().load("application.yml", ClassPathResource("application.yml"))
            val bound =
                Binder(ConfigurationPropertySources.from(sources))
                    .bind("app.webclient", Bindable.of(WebClientProperties::class.java))
                    .orElseThrow { AssertionError("app.webclient did not bind from the shipped application.yml") }

            assertThat(bound.maxInMemorySize).isEqualTo(DataSize.ofKilobytes(256))
        }
    }

    @Nested
    inner class PoolSettings {
        @Test
        fun `should reject a pending-acquire count below minus one`() {
            // Given / When / Then: -1 is Reactor Netty's "unbounded", so -2 is the first value that means
            // nothing at all. The pool builder would reject it too, but only at factory construction and
            // without naming the property or its source.
            assertThatThrownBy { WebClientProperties.Pool(pendingAcquireMaxCount = -2) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("app.webclient.pool.pending-acquire-max-count")
        }

        @Test
        fun `should keep unbounded and never-queue as valid choices`() {
            // Why this case is important: both are deliberate operational settings, not accidents - -1 hands
            // the queueing decision to the pool, 0 declines to queue at all and fails fast on exhaustion. A
            // guard written as "must be positive" would have quietly outlawed both.
            //
            // Given / When / Then
            assertThatCode {
                WebClientProperties.Pool(pendingAcquireMaxCount = -1)
                WebClientProperties.Pool(pendingAcquireMaxCount = 0)
            }.doesNotThrowAnyException()
        }

        @Test
        fun `should reject a negative lifecycle duration on every pool knob`() {
            // What is tested: all three durations, because each is passed straight to the pool builder, which
            //   accepts a negative one and then means by it something no operator intended - a connection due
            //   for eviction the moment it is created.
            // How success is judged: each of the three throws, naming its own key. Asserting them one by one
            //   rather than as a group is what catches a guard that was copied but not adapted.
            //
            // Given / When / Then
            assertThatThrownBy { WebClientProperties.Pool(maxIdleTime = Duration.ofSeconds(-1)) }
                .hasMessageContaining("app.webclient.pool.max-idle-time")
            assertThatThrownBy { WebClientProperties.Pool(maxLifeTime = Duration.ofSeconds(-1)) }
                .hasMessageContaining("app.webclient.pool.max-life-time")
            assertThatThrownBy { WebClientProperties.Pool(evictInBackground = Duration.ofSeconds(-1)) }
                .hasMessageContaining("app.webclient.pool.evict-in-background")
        }

        @Test
        fun `should accept zero durations, which the pool reads as off`() {
            // Given / When / Then: Duration.ZERO is Reactor Netty's documented "background eviction disabled";
            // the guard must draw its line at negative, not at zero.
            assertThatCode {
                WebClientProperties.Pool(
                    maxIdleTime = Duration.ZERO,
                    maxLifeTime = Duration.ZERO,
                    evictInBackground = Duration.ZERO,
                )
            }.doesNotThrowAnyException()
        }
    }

    @Nested
    inner class ValidationToggle {
        @Test
        fun `should default to off with no per-endpoint overrides`() {
            // What/why: the shipped default. The DTO constraints come from the published OpenAPI specs, which
            //   are stricter than reality in places, so validation must be off until a backend opts in - the
            //   guarantee the whole feature rests on.
            // Given / When / Then
            val validation = WebClientProperties.Validation()
            assertThat(validation.enabled).isFalse()
            assertThat(validation.endpoints).isEmpty()
            assertThat(validation.isEnabledFor("any-backend")).isFalse()
            // Enforcing is the default: once a backend opts in, a violation fails the call rather than passing
            // silently. Report-only (false) is the deliberate rollout exception, not the shipped stance.
            assertThat(validation.failOnViolation).isTrue()
        }

        @Test
        fun `should let a per-alias override win over the shared default in both directions`() {
            // What is tested: that a per-backend entry beats the global default whichever way they disagree -
            //   not just to enable a backend above an off default, but also to disable one above an on default.
            // How success is judged: with the default ON, an alias mapped to false resolves to false while an
            //   alias with no entry inherits true; with the default OFF, an alias mapped to true resolves to
            //   true while an unmapped one inherits false. The override is the only variable in each pair.
            // Why it matters: the specs are wrong in different ways per endpoint, so validation has to be
            //   adoptable one backend at a time AND revocable for one backend without giving it up everywhere.
            //   An all-or-nothing switch would be unusable here.
            //
            // Given: default ON, one backend explicitly opted OUT
            val defaultOn = WebClientProperties.Validation(enabled = true, endpoints = mapOf("opted-out" to false))
            // Then
            assertThat(defaultOn.isEnabledFor("opted-out")).isFalse()
            assertThat(defaultOn.isEnabledFor("inherits")).isTrue()

            // Given: default OFF, one backend explicitly opted IN
            val defaultOff = WebClientProperties.Validation(enabled = false, endpoints = mapOf("opted-in" to true))
            // Then
            assertThat(defaultOff.isEnabledFor("opted-in")).isTrue()
            assertThat(defaultOff.isEnabledFor("inherits")).isFalse()
        }
    }
}
