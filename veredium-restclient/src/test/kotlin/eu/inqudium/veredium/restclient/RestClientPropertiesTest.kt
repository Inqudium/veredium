package eu.inqudium.veredium.restclient

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
 * Tests for the binding-time guards of [RestClientProperties].
 *
 * The class carries no behaviour beyond its invariants, so this is what there is to test: that a value which
 * cannot be meant is rejected WHERE IT IS SET, and that the message names the property key the operator has
 * to edit rather than the Kotlin field they cannot see.
 */
class RestClientPropertiesTest {
    @Nested
    inner class MaxInMemorySize {
        @Test
        fun `should reject a limit of zero, which would fail every call as a client-side refusal`() {
            // What is tested: the lower bound of the buffer limit.
            // How success is judged: binding throws, naming `app.restclient.max-in-memory-size`.
            // Why this case is important: this is the value whose misconfiguration is hardest to trace. Zero
            //   binds happily, and then every response exceeds the buffer and fails with a
            //   ResponseBodyLimitExceededException - the one exception ClientSideFailurePolicy keeps
            //   OFF the circuit breaker on purpose. So every backend fails, no breaker opens, and the monitoring
            //   picture says "the backends are down". The guard is what keeps the diagnosis pointing at the
            //   configuration instead.
            //
            // Given / When / Then
            assertThatThrownBy { RestClientProperties(maxInMemorySize = DataSize.ofBytes(0)) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("app.restclient.max-in-memory-size")
        }

        @Test
        fun `should reject a limit past Int MAX_VALUE, which would wrap to a negative buffer`() {
            // What is tested: the UPPER bound, which exists for a reason that is invisible at this class -
            //   ResilientRestClientFactory narrows the value to an Int for the bounded reader.
            // How success is judged: 3 GB is rejected here, rather than silently becoming a negative Int later.
            // Why this case is important: without the guard, a value chosen to be generous would end up at the
            //   zero case above, by a route nobody would suspect from reading the configured number.
            //
            // Given / When / Then
            assertThatThrownBy { RestClientProperties(maxInMemorySize = DataSize.ofGigabytes(3)) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("app.restclient.max-in-memory-size")
        }

        @Test
        fun `should accept the shipped default and the extremes that are still meaningful`() {
            // Why this test exists: a guard that rejects everything would satisfy the two tests above. This
            // states where the line actually runs - one byte and Int.MAX_VALUE bytes are both admitted.
            //
            // Given / When / Then
            assertThatCode {
                RestClientProperties()
                RestClientProperties(maxInMemorySize = DataSize.ofBytes(1))
                RestClientProperties(maxInMemorySize = DataSize.ofBytes(Int.MAX_VALUE.toLong()))
            }.doesNotThrowAnyException()
        }

        @Test
        fun `should default to 256 KB, the twin's ceiling and the size one oversized access-profile response needed`() {
            // What is tested: the shipped default of the buffer limit, as a value in its own right rather than
            //   as a by-product of the range check above.
            // How success is judged: the default is 256 KB and NOT the 8 KB the reactive module once shipped.
            // Why this case is important: the two modules front the SAME backends, and a response that fits one
            //   client's buffer and not the other's would be a difference nobody expects from a twin. 8 KB was
            //   measured to be too small on 2026-08-07; the limit applies per concurrently buffered response and
            //   the shared pool is bounded at 100 connections, so this default also bounds the worst case at
            //   ~25 MB of response buffers - the reason it is not simply raised further.
            //
            // Given / When / Then
            assertThat(RestClientProperties().maxInMemorySize)
                .isEqualTo(DataSize.ofKilobytes(256))
                .isNotEqualTo(DataSize.ofKilobytes(8))
        }

        @Test
        fun `should bind the shipped application yml to the same 256 KB ceiling`() {
            val sources = YamlPropertySourceLoader().load("application.yml", ClassPathResource("application.yml"))
            val bound =
                Binder(ConfigurationPropertySources.from(sources))
                    .bind("app.restclient", Bindable.of(RestClientProperties::class.java))
                    .orElseThrow { AssertionError("app.restclient did not bind from the shipped application.yml") }

            assertThat(bound.maxInMemorySize).isEqualTo(DataSize.ofKilobytes(256))
        }
    }

    @Nested
    inner class PoolSettings {
        @Test
        fun `should reject a negative lifecycle duration on every pool knob`() {
            // What is tested: all three durations, because each is passed on to Apache's pool, which accepts a
            //   negative one and then means by it something no operator intended - a connection due for
            //   eviction the moment it is created.
            // How success is judged: each of the three throws, naming its own key. Asserting them one by one
            //   rather than as a group is what catches a guard that was copied but not adapted.
            //
            // Given / When / Then
            assertThatThrownBy { RestClientProperties.Pool(maxIdleTime = Duration.ofSeconds(-1)) }
                .hasMessageContaining("app.restclient.pool.max-idle-time")
            assertThatThrownBy { RestClientProperties.Pool(maxLifeTime = Duration.ofSeconds(-1)) }
                .hasMessageContaining("app.restclient.pool.max-life-time")
            assertThatThrownBy { RestClientProperties.Pool(evictInBackground = Duration.ofSeconds(-1)) }
                .hasMessageContaining("app.restclient.pool.evict-in-background")
        }

        @Test
        fun `should accept zero durations, which the factory reads as off`() {
            // Given / When / Then: Duration.ZERO means "no background eviction" and "no lifetime bound"; the
            // guard must draw its line at negative, not at zero.
            assertThatCode {
                RestClientProperties.Pool(
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
            val validation = RestClientProperties.Validation()
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
            val defaultOn = RestClientProperties.Validation(enabled = true, endpoints = mapOf("opted-out" to false))
            // Then
            assertThat(defaultOn.isEnabledFor("opted-out")).isFalse()
            assertThat(defaultOn.isEnabledFor("inherits")).isTrue()

            // Given: default OFF, one backend explicitly opted IN
            val defaultOff = RestClientProperties.Validation(enabled = false, endpoints = mapOf("opted-in" to true))
            // Then
            assertThat(defaultOff.isEnabledFor("opted-in")).isTrue()
            assertThat(defaultOff.isEnabledFor("inherits")).isFalse()
        }
    }
}
