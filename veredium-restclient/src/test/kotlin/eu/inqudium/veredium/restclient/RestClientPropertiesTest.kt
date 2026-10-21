package eu.inqudium.veredium.restclient

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
            // Success criteria: binding throws, naming `app.restclient.max-in-memory-size`.
            // Why it matters: this is the value whose misconfiguration is hardest to trace. Zero
            //   binds happily, and then every response exceeds the buffer and fails with a
            //   ResponseBodyLimitExceededException - the one exception RestClientVerdicts.clientSideFailure keeps
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
            // Success criteria: 3 GB is rejected here, rather than silently becoming a negative Int later.
            // Why it matters: without the guard, a value chosen to be generous would end up at the
            //   zero case above, by a route nobody would suspect from reading the configured number.
            //
            // Given / When / Then
            assertThatThrownBy { RestClientProperties(maxInMemorySize = DataSize.ofGigabytes(3)) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("app.restclient.max-in-memory-size")
        }

        @Test
        fun `should accept the code default and the extremes that are still meaningful`() {
            // What is tested: the admitted range of the buffer limit.
            // Success criteria: the code default, one byte and Int.MAX_VALUE bytes all construct without an
            //   exception.
            // Why it matters: a guard that rejects everything would satisfy the two tests above; this states
            //   where the line actually runs.
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
            // What is tested: the code default of the buffer limit, as a value in its own right rather than
            //   as a by-product of the range check above.
            // Success criteria: the default is 256 KB and NOT the 8 KB the reactive module once shipped.
            // Why it matters: the two modules front the SAME backends, and a response that fits one
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
        fun `should bind the reference application yml to the same 256 KB ceiling`() {
            // What is tested: `app.restclient.max-in-memory-size` of the reference configuration, bound the way
            //   Boot binds it.
            // Success criteria: the bound value is the same 256 KB as the code default.
            // Why it matters: the reference file is what hosts copy; a ceiling there that differed from the code
            //   default would make "configured as documented" and "not configured" two different clients.
            //
            // Given
            val sources = YamlPropertySourceLoader().load(ConfigurationFiles.REFERENCE, ClassPathResource(ConfigurationFiles.REFERENCE))

            // When
            val bound =
                Binder(ConfigurationPropertySources.from(sources))
                    .bind("app.restclient", Bindable.of(RestClientProperties::class.java))
                    .orElseThrow { AssertionError("app.restclient did not bind from the reference application.yml") }

            // Then
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
            // Success criteria: each of the three throws, naming its own key. Asserting them one by one
            //   rather than as a group is what catches a guard that was copied but not adapted.
            // Why it matters: a negative value is accepted by Apache and read as "already expired"; caught at
            //   binding, it names the key, caught nowhere, it shows as connections that never live.
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
            // What is tested: the lower end of the admitted range of the three pool durations.
            // Success criteria: Duration.ZERO for all three constructs without an exception.
            // Why it matters: zero means "no background eviction" and "no lifetime bound" - documented settings;
            //   the guard must draw its line at negative, not at zero.
            //
            // Given / When / Then
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
    inner class UserAgent {
        @Test
        fun `should send the library's token by default`() {
            // What is tested: the User-Agent of a host that configures none.
            // Success criteria: `userAgent` is unset and `effectiveUserAgent` is the library's token
            //   (`veredium/<version>`, the bare name here where no manifest is on the classpath).
            // Why it matters: a peer's access log should name what calls it - never the transport's own default,
            //   which says only "Apache-HttpClient".
            //
            // Given / When / Then
            assertThat(RestClientProperties().userAgent).isNull()
            assertThat(RestClientProperties().effectiveUserAgent).isEqualTo(Veredium.userAgent())
        }

        @Test
        fun `should let the host name itself instead`() {
            // What is tested: `app.restclient.user-agent` set by the host.
            // Success criteria: the configured value, trimmed, is the effective User-Agent.
            // Why it matters: a host read per caller in its peers' logs names itself; surrounding whitespace from
            //   a YAML edit must not end up on the wire.
            //
            // Given / When / Then
            assertThat(RestClientProperties(userAgent = " my-service/2.3 ").effectiveUserAgent).isEqualTo("my-service/2.3")
        }

        @Test
        fun `should reject a blank token at bind time, naming the key`() {
            // What is tested: the binding guard of `app.restclient.user-agent`.
            // Success criteria: a blank value throws IllegalArgumentException naming the key.
            // Why it matters: a blank or CR/LF-carrying value is refused by the transport per request, with a
            //   message that points nowhere near the configuration.
            //
            // Given / When / Then
            assertThatThrownBy { RestClientProperties(userAgent = "  ") }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("app.restclient.user-agent")
        }
    }
}
