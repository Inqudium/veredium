package eu.inqudium.veredium.webclient

import eu.inqudium.veredium.core.Veredium
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
            // Success criteria: binding throws, naming `app.webclient.max-in-memory-size`.
            // Why it matters: this is the value whose misconfiguration is hardest to trace. Zero
            //   binds happily, and then every response exceeds the buffer and fails with a
            //   DataBufferLimitException - the one exception WebClientVerdicts.clientSideFailure keeps OFF the
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
            // Success criteria: 3 GB is rejected here, rather than silently becoming a negative Int later.
            // Why it matters: without the guard, a value chosen to be generous would end up at the
            //   zero case above, by a route nobody would suspect from reading the configured number.
            //
            // Given / When / Then
            assertThatThrownBy { WebClientProperties(maxInMemorySize = DataSize.ofGigabytes(3)) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("app.webclient.max-in-memory-size")
        }

        @Test
        fun `should accept the code default and the extremes that are still meaningful`() {
            // What is tested: the admitted side of the codec limit's range.
            // Success criteria: the code default, one byte and Int.MAX_VALUE bytes all construct without an
            //   exception.
            // Why it matters: a guard that rejects everything would satisfy the two tests above. This states
            //   where the line actually runs.
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
            // What is tested: the code default of the codec limit, as a value in its own right rather than
            //   as a by-product of the range check above.
            // Success criteria: the default is 256 KB - Spring's own ExchangeStrategies default - and
            //   NOT the 8 KB this property shipped with until 2026-08-08.
            // Why it matters: the old value was pinned by a bare equality assertion, which catches
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
        fun `should bind the reference application yml to the same 256 KB ceiling`() {
            // What is tested: `app.webclient.max-in-memory-size` of the reference configuration, bound the way
            //   Boot binds a config file.
            // Success criteria: the bound limit is 256 KB - the same as the code default above.
            // Why it matters: the reference spells the value out with a comment explaining it; a value that
            //   drifted from the code default would leave the documented example and the behaviour of a host
            //   that omits the key disagreeing about the same number.
            //
            // Given
            val sources = YamlPropertySourceLoader().load("application.yml", ClassPathResource("application.yml"))

            // When
            val bound =
                Binder(ConfigurationPropertySources.from(sources))
                    .bind("app.webclient", Bindable.of(WebClientProperties::class.java))
                    .orElseThrow { AssertionError("app.webclient did not bind from the reference application.yml") }

            // Then
            assertThat(bound.maxInMemorySize).isEqualTo(DataSize.ofKilobytes(256))
        }
    }

    @Nested
    inner class PoolSettings {
        @Test
        fun `should reject a pending-acquire count below minus one`() {
            // What is tested: the lower bound of `pool.pending-acquire-max-count`.
            // Success criteria: -2 is rejected at binding, naming `app.webclient.pool.pending-acquire-max-count`.
            // Why it matters: -1 is Reactor Netty's "unbounded", so -2 is the first value that means nothing at
            //   all. The pool builder would reject it too, but only at factory construction and without naming
            //   the property or its source.
            //
            // Given / When / Then
            assertThatThrownBy { WebClientProperties.Pool(pendingAcquireMaxCount = -2) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("app.webclient.pool.pending-acquire-max-count")
        }

        @Test
        fun `should keep unbounded and never-queue as valid choices`() {
            // What is tested: the two special values of `pool.pending-acquire-max-count`.
            // Success criteria: -1 and 0 both construct without an exception.
            // Why it matters: both are deliberate operational settings, not accidents - -1 hands the queueing
            //   decision to the pool, 0 declines to queue at all and fails fast on exhaustion. A guard written as
            //   "must be positive" would have quietly outlawed both.
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
            // Success criteria: each of the three throws, naming its own key. Asserting them one by one
            //   rather than as a group is what catches a guard that was copied but not adapted.
            // Why it matters: a negative duration binds, the context starts, and the pool then evicts every
            //   connection as it is created - a performance collapse that looks like a slow backend.
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
        fun `should accept zero on the two durations every engine reads as off`() {
            // What is tested: zero on max-life-time and evict-in-background.
            // Success criteria: the Pool constructs without an exception.
            // Why it matters: Duration.ZERO is "unbounded" for the life time and "background eviction disabled"
            //   for the sweep on every engine; the guard must draw its line at negative for these two, or it
            //   would outlaw the one way to switch them off.
            //
            // Given / When / Then
            assertThatCode {
                WebClientProperties.Pool(
                    maxLifeTime = Duration.ZERO,
                    evictInBackground = Duration.ZERO,
                )
            }.doesNotThrowAnyException()
        }

        @Test
        fun `should reject a zero max-idle-time, which the engines read in opposite ways`() {
            // What is tested: max-idle-time = 0.
            // Success criteria: binding fails, naming the key and saying it must be positive.
            // Why it matters: Reactor Netty evicts every released connection at an idle limit of 0 (no
            //   keep-alive, a connection per call) while Jetty reads it as "never close an idle connection" -
            //   the same line of YAML would mean the opposite on two engines, without a word at startup.
            //
            // Given / When / Then
            assertThatThrownBy { WebClientProperties.Pool(maxIdleTime = Duration.ZERO) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("app.webclient.pool.max-idle-time must be positive")
        }
    }

    @Nested
    inner class UserAgent {
        @Test
        fun `should send the library's token by default`() {
            // What is tested: the User-Agent of a host that configures none.
            // Success criteria: `userAgent` is unset and `effectiveUserAgent` is the library's token -
            //   `veredium/<version>`, the bare name here where no manifest is on the classpath.
            // Why it matters: a host that says nothing names this library to its peers, never the transport's
            //   own default, which says nothing about who called.
            //
            // Given / When / Then
            assertThat(WebClientProperties().userAgent).isNull()
            assertThat(WebClientProperties().effectiveUserAgent).isEqualTo(Veredium.userAgent())
        }

        @Test
        fun `should let the host name itself instead`() {
            // What is tested: `app.webclient.user-agent` set by the host.
            // Success criteria: the configured value, trimmed, is the effective User-Agent verbatim.
            // Why it matters: the override exists for peers that read their access logs per caller; a value
            //   merged with or losing to the library's token would make it pointless.
            //
            // Given / When / Then
            assertThat(WebClientProperties(userAgent = " my-service/2.3 ").effectiveUserAgent).isEqualTo("my-service/2.3")
        }

        @Test
        fun `should reject a blank token at bind time, naming the key`() {
            // What is tested: a blank `app.webclient.user-agent`.
            // Success criteria: binding throws IllegalArgumentException naming the key.
            // Why it matters: a blank or CR/LF-carrying value is refused by the transport per request, with a
            //   message that points nowhere near the configuration.
            //
            // Given / When / Then
            assertThatThrownBy { WebClientProperties(userAgent = "  ") }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("app.webclient.user-agent")
        }
    }
}
