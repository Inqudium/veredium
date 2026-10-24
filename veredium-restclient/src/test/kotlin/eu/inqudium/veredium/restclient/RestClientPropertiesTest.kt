package eu.inqudium.veredium.restclient

import eu.inqudium.veredium.core.Veredium
import eu.inqudium.veredium.testsupport.ConfigurationFiles
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.entry
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.springframework.boot.context.properties.bind.BindException
import org.springframework.boot.context.properties.bind.Bindable
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.context.properties.source.ConfigurationPropertySources
import org.springframework.boot.env.YamlPropertySourceLoader
import org.springframework.core.io.ByteArrayResource
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
    inner class `The buffer limit` {
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
            //   zero case, by a route nobody would suspect from reading the configured number.
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
            // Why it matters: a guard that rejects everything would satisfy the zero and the past-Int.MAX_VALUE
            //   cases; this states where the line actually runs.
            //
            // Given / When / Then
            assertThatCode {
                RestClientProperties()
                RestClientProperties(maxInMemorySize = DataSize.ofBytes(1))
                RestClientProperties(maxInMemorySize = DataSize.ofBytes(Int.MAX_VALUE.toLong()))
            }.doesNotThrowAnyException()
        }

        @Test
        fun `should default to 256 KB, the twin's ceiling, large enough for a typical JSON list response`() {
            // What is tested: the code default of the buffer limit, as a value in its own right rather than
            //   as a by-product of the range-check cases.
            // Success criteria: the default is 256 KB and NOT 8 KB.
            // Why it matters: the two modules front the SAME backends, and a response that fits one
            //   client's buffer and not the other's would be a difference nobody expects from a twin. 8 KB is
            //   too small for a typical JSON payload; the limit applies per concurrently buffered response and
            //   the reference configuration holds the mesh bulkheads' sum at 100 calls, so this default also bounds
            //   the worst case there at ~25 MB of response buffers - the reason it is not simply raised further.
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
            // Given / When
            val bound = ConfigurationFiles.bind(ConfigurationFiles.REFERENCE, "app.restclient", RestClientProperties::class.java)

            // Then
            assertThat(bound.maxInMemorySize).isEqualTo(DataSize.ofKilobytes(256))
        }
    }

    @Nested
    inner class `The pool settings` {
        @Test
        fun `should reject a negative lifecycle duration on every pool knob, and a zero idle time`() {
            // What is tested: all three durations, because each is passed on to Apache's pool, which accepts a
            //   negative one and then means by it something no operator intended - a connection due for
            //   eviction the moment it is created.
            // Success criteria: each of the three throws, naming its own key. Asserting them one by one
            //   rather than as a group is what catches a guard that was copied but not adapted.
            //   A zero max-idle-time throws as well, naming the key and why.
            // Why it matters: a negative value is accepted by Apache and read as "already expired"; caught at
            //   binding, it names the key, caught nowhere, it shows as connections that never live. A zero idle
            //   time means no keep-alive at all on Reactor Netty and no idle timeout on Jetty - two opposite
            //   readings of one value, depending on the engine.
            //
            // Given / When / Then
            assertThatThrownBy { RestClientProperties.Pool(maxIdleTime = Duration.ofSeconds(-1)) }
                .hasMessageContaining("app.restclient.pool.max-idle-time")
            assertThatThrownBy { RestClientProperties.Pool(maxIdleTime = Duration.ZERO) }
                .hasMessageContainingAll("app.restclient.pool.max-idle-time must be positive", "no keep-alive on Reactor Netty")
            assertThatThrownBy { RestClientProperties.Pool(maxLifeTime = Duration.ofSeconds(-1)) }
                .hasMessageContaining("app.restclient.pool.max-life-time")
            assertThatThrownBy { RestClientProperties.Pool(evictInBackground = Duration.ofSeconds(-1)) }
                .hasMessageContaining("app.restclient.pool.evict-in-background")
        }

        @Test
        fun `should accept zero durations where they mean off, and a millisecond of idle time`() {
            // What is tested: the lower end of the admitted range of the three pool durations.
            // Success criteria: Duration.ZERO for max-life-time and evict-in-background, and one millisecond for
            //   max-idle-time, construct without an exception.
            // Why it matters: zero means "no background eviction" and "no lifetime bound" - documented settings;
            //   the guard must draw its line at negative there, and at zero only for the idle time.
            //
            // Given / When / Then
            assertThatCode {
                RestClientProperties.Pool(
                    maxIdleTime = Duration.ofMillis(1),
                    maxLifeTime = Duration.ZERO,
                    evictInBackground = Duration.ZERO,
                )
            }.doesNotThrowAnyException()
        }
    }

    @Nested
    inner class `The User-Agent` {
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

    @Nested
    inner class `The engine selection` {
        @Test
        fun `should default to the Apache engine for every backend`() {
            // What is tested: the code default of `app.restclient.engine` and of the reference configuration.
            // Success criteria: an unset block and the reference application.yml both resolve every alias to
            //   APACHE, with no override.
            // Why it matters: the default IS the module's established wiring; a host that does not set the
            //   property must get exactly that, not a different engine.
            //
            // Given
            val reference = ConfigurationFiles.bind(ConfigurationFiles.REFERENCE, "app.restclient", RestClientProperties::class.java)

            // When / Then
            assertThat(RestClientProperties().engine.engineFor("any")).isEqualTo(RestClientEngine.APACHE)
            assertThat(reference.engine).isEqualTo(RestClientProperties.Engine())
        }

        @Test
        fun `should bind every engine by its YAML value, reactor-netty included`() {
            // What is tested: Boot's relaxed enum binding against the documented values, the hyphenated one in
            //   particular, and the constant name as an alternative spelling.
            // Success criteria: `apache`, `jdk`, `jetty`, `reactor-netty` and `REACTOR_NETTY` bind to their
            //   constants, and each constant's key is the documented value.
            // Why it matters: `reactor-netty` is not a Java identifier; if Boot did not fold the hyphen, the one
            //   value the docs give for that engine would fail every start.
            //
            // Given / When / Then
            mapOf(
                "apache" to RestClientEngine.APACHE,
                "jdk" to RestClientEngine.JDK,
                "jetty" to RestClientEngine.JETTY,
                "reactor-netty" to RestClientEngine.REACTOR_NETTY,
                "REACTOR_NETTY" to RestClientEngine.REACTOR_NETTY,
            ).forEach { (value, engine) ->
                assertThat(bind("app.restclient.engine.default: $value").engine.default).`as`(value).isEqualTo(engine)
            }
            assertThat(RestClientEngine.entries.map { it.key }).containsExactly("apache", "jdk", "jetty", "reactor-netty")
        }

        @Test
        fun `should bind a per-backend override under the alias exactly as written`() {
            // What is tested: the override map, keyed by mesh aliases - which carry upper-case HTTP methods.
            // Success criteria: the alias `api-example-com-GET-accounts` keeps its case as a map key, resolves
            //   to its override, and any other alias to the default; the key the failure messages name is the
            //   full property key of the override, or of the default.
            // Why it matters: the factory matches overrides against mesh aliases by equality; a key folded to
            //   lower case by the binder would never match and fail the start as an unknown alias.
            //
            // Given
            val engine =
                bind(
                    """
                    app.restclient.engine:
                      default: jdk
                      endpoints:
                        api-example-com-GET-accounts: reactor-netty
                    """,
                ).engine

            // When / Then
            assertThat(engine.endpoints).containsExactly(entry("api-example-com-GET-accounts", RestClientEngine.REACTOR_NETTY))
            assertThat(engine.engineFor("api-example-com-GET-accounts")).isEqualTo(RestClientEngine.REACTOR_NETTY)
            assertThat(engine.engineFor("api-example-com-GET-other")).isEqualTo(RestClientEngine.JDK)
            assertThat(engine.keyFor("api-example-com-GET-accounts")).isEqualTo("app.restclient.engine.endpoints.api-example-com-GET-accounts")
            assertThat(engine.keyFor("api-example-com-GET-other")).isEqualTo("app.restclient.engine.default")
        }

        @Test
        fun `should reject an unknown engine at bind time, naming the key`() {
            // What is tested: a value that is no engine.
            // Success criteria: binding fails, and the failure names `app.restclient.engine.default`.
            // Why it matters: a misspelt engine must not fall back to a default and run on something the
            //   operator did not choose.
            //
            // Given / When / Then
            assertThatThrownBy { bind("app.restclient.engine.default: netty") }
                .isInstanceOf(BindException::class.java)
                .hasMessageContaining("app.restclient.engine.default")
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /**
     * Binds `app.restclient` from [yaml] the way Boot binds an application.yml. A YAML that binds nothing fails
     * here instead of falling back to the defaults, which would let a case for the default value pass on a
     * binding that never happened.
     */
    private fun bind(yaml: String): RestClientProperties {
        val sources = YamlPropertySourceLoader().load("engine", ByteArrayResource(yaml.trimIndent().toByteArray()))
        return Binder(ConfigurationPropertySources.from(sources))
            .bind("app.restclient", Bindable.of(RestClientProperties::class.java))
            .orElseThrow { AssertionError("nothing bound under app.restclient from: $yaml") }
    }
}
