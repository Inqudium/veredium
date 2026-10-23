package eu.inqudium.veredium.webclient

import eu.inqudium.veredium.core.engine.EngineSelection
import eu.inqudium.veredium.core.mesh.MeshConnectionPool
import eu.inqudium.veredium.core.mesh.MeshProperties
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.springframework.boot.context.properties.bind.BindException
import org.springframework.boot.context.properties.bind.Bindable
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.context.properties.source.ConfigurationPropertySources
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource
import org.springframework.boot.env.YamlPropertySourceLoader
import org.springframework.boot.test.context.FilteredClassLoader
import org.springframework.core.io.ByteArrayResource
import org.springframework.core.io.ClassPathResource
import org.springframework.core.io.DefaultResourceLoader
import org.springframework.web.reactive.function.client.WebClient
import tools.jackson.databind.json.JsonMapper

/**
 * The binding of `app.webclient.engine` and the checks the factory runs on it at construction - each of them a
 * configuration mistake that would otherwise surface far from its cause: a misspelt override silently leaves a
 * backend on the default engine, a missing library fails with a `NoClassDefFoundError` at the first client, and
 * the JDK client rejects every request of a backend for a header the operator never set.
 *
 * Every failure message is asserted to name the property KEY, because the key is what the person reading the
 * failure has to edit - the convention of [WebClientProperties].
 */
class WebClientEngineConfigTest {
    @Nested
    inner class Binding {
        @ParameterizedTest(name = "{0} -> {1}")
        @CsvSource("reactor-netty, REACTOR_NETTY", "jdk, JDK", "jetty, JETTY", "apache, APACHE", "REACTOR_NETTY, REACTOR_NETTY")
        fun `should bind the documented YAML value onto the engine`(
            value: String,
            expected: WebClientEngine,
        ) {
            // What is tested: Boot's relaxed enum binding of app.webclient.engine.default, including the dashed
            //   `reactor-netty` the documentation uses for a constant spelt REACTOR_NETTY.
            // Success criteria: each documented value binds to its constant; the constant's own name does too.
            // Why it matters: the dashed form is the one the guide and the startup line print; were it not
            //   bindable, the documented default could not be written down explicitly.
            //
            // Given/When/Then
            assertThat(bind(mapOf("app.webclient.engine.default" to value)).engine.default).isEqualTo(expected)
        }

        @Test
        fun `should reject an engine name that is none of the four, naming the key`() {
            // What is tested: a value outside the enum, e.g. the plausible slip `netty`.
            // Success criteria: binding fails with a BindException naming app.webclient.engine.default.
            // Why it matters: falling back to the default would leave the operator believing a choice was made.
            //
            // Given/When/Then
            assertThatThrownBy { bind(mapOf("app.webclient.engine.default" to "netty")) }
                .isInstanceOf(BindException::class.java)
                .hasMessageContaining("app.webclient.engine.default")
        }

        @Test
        fun `should bind a per-alias override from YAML with the alias spelt as derived from the triplet`() {
            // What is tested: app.webclient.engine.endpoints from YAML, keyed by a derived alias that contains
            //   upper-case letters (the METHOD segment).
            // Success criteria: the map holds the alias verbatim, so engineFor(alias) returns the override and
            //   another alias the default.
            // Why it matters: derived aliases carry the HTTP method in upper case; a binder that lower-cased map
            //   keys would make the override match no backend - which the factory's alias check would then report,
            //   but for a key the operator typed correctly.
            //
            // Given
            val yaml =
                """
                app:
                  webclient:
                    engine:
                      default: jdk
                      endpoints:
                        apis-example-com-POST-v8-agreements-products-lookup: apache
                """.trimIndent()
            val sources = YamlPropertySourceLoader().load("engine.yml", ByteArrayResource(yaml.toByteArray()))

            // When
            val bound = Binder(ConfigurationPropertySources.from(sources)).bind("app.webclient", Bindable.of(WebClientProperties::class.java)).get()

            // Then
            assertThat(bound.engine.endpoints).containsEntry("apis-example-com-POST-v8-agreements-products-lookup", WebClientEngine.APACHE)
            assertThat(bound.engine.engineFor("apis-example-com-POST-v8-agreements-products-lookup")).isEqualTo(WebClientEngine.APACHE)
            assertThat(bound.engine.engineFor("anything-else")).isEqualTo(WebClientEngine.JDK)
        }

        @Test
        fun `should default to Reactor Netty in code and in the reference configuration`() {
            // What is tested: the code default of the engine and the value the reference configuration shows.
            // Success criteria: a bare WebClientProperties and the binding of the reference application.yml both
            //   select reactor-netty, with no override.
            // Why it matters: the switch was introduced with the promise that a host which does not set it keeps
            //   exactly the wiring it had.
            //
            // Given
            val sources = YamlPropertySourceLoader().load("application.yml", ClassPathResource("application.yml"))

            // When
            val reference = Binder(ConfigurationPropertySources.from(sources)).bind("app.webclient", Bindable.of(WebClientProperties::class.java)).get()

            // Then
            assertThat(WebClientProperties().engine).isEqualTo(WebClientProperties.Engine(WebClientEngine.REACTOR_NETTY, emptyMap()))
            assertThat(reference.engine.default).isEqualTo(WebClientEngine.REACTOR_NETTY)
            assertThat(reference.engine.endpoints).isEmpty()
        }

        private fun bind(values: Map<String, String>): WebClientProperties = Binder(MapConfigurationPropertySource(values)).bind("app.webclient", Bindable.of(WebClientProperties::class.java)).get()
    }

    @Nested
    inner class `Checks at factory construction` {
        @Test
        fun `should refuse an override key that names no mesh alias, naming the key and the known aliases`() {
            // What is tested: typo protection for app.webclient.engine.endpoints.
            // Success criteria: construction fails with an IllegalArgumentException naming the unknown key in full
            //   and the aliases that do exist.
            // Why it matters: a misspelt key would otherwise leave its backend on the default engine without a
            //   word - the very backend someone meant to move.
            //
            // Given
            val engine = WebClientProperties.Engine(endpoints = mapOf("orderz" to WebClientEngine.JETTY))

            // When/Then
            assertThatThrownBy { factory(engine, aliases = listOf("orders", "payments")) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContainingAll("app.webclient.engine.endpoints.orderz", "names no mesh endpoint", "[orders, payments]")
        }

        @ParameterizedTest(name = "{0} without {1}")
        @CsvSource(
            "JETTY, org.eclipse.jetty.reactive, org.eclipse.jetty:jetty-reactive-httpclient",
            "APACHE, org.apache.hc.core5.reactive, org.apache.httpcomponents.core5:httpcore5-reactive",
        )
        fun `should name the coordinates to add when a selected engine's reactive adapter is missing`(
            engine: WebClientEngine,
            hiddenPackage: String,
            coordinates: String,
        ) {
            // What is tested: the classpath check, against a class loader that hides ONLY the reactive adapter
            //   of an engine whose client library stays visible (jetty-client without jetty-reactive-httpclient,
            //   httpclient5 without httpcore5-reactive).
            // Success criteria: construction fails naming the selecting key app.webclient.engine.default, the
            //   engine, the backend and the adapter's Maven coordinates.
            // Why it matters: the half-present engine is the likely real mistake - the client library often
            //   arrives transitively, the adapter never does - and a check satisfied by ANY one of the required
            //   classes would pass it, leaving a NoClassDefFoundError at the first client.
            //
            // Given
            val hidden = DefaultResourceLoader(FilteredClassLoader(hiddenPackage))

            // When/Then
            assertThatThrownBy { factory(WebClientProperties.Engine(default = engine), resourceLoader = hidden) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContainingAll("app.webclient.engine.default selects the HTTP engine '$engine' for [orders]", coordinates)
        }

        @Test
        fun `should not require the library of an engine no backend selected`() {
            // What is tested: the classpath check is limited to the engines in use.
            // Success criteria: with Apache HttpClient hidden, a factory whose backends are on reactor-netty and
            //   jdk constructs without an exception.
            // Why it matters: the optional dependencies are optional because a host only adds the one it uses;
            //   a check that demanded all of them would turn them into mandatory ones.
            //
            // Given
            val hidden = DefaultResourceLoader(FilteredClassLoader("org.apache.hc"))
            val engine = WebClientProperties.Engine(endpoints = mapOf("payments" to WebClientEngine.JDK))

            // When/Then
            assertThatCode { factory(engine, aliases = listOf("orders", "payments"), resourceLoader = hidden) }.doesNotThrowAnyException()
        }

        @Test
        fun `should refuse a pending-acquire count of 0 on Jetty, which has no setting for no call waits`() {
            // What is tested: app.webclient.pool.pending-acquire-max-count=0 with the jetty engine.
            // Success criteria: construction fails naming the pool key, the engine key and the backend; the same
            //   value on reactor-netty constructs.
            // Why it matters: Reactor Netty reads 0 as "never queue"; Jetty's queue also holds the requests a
            //   connection is being opened for, so no capacity means that - a carried-over 0 would make every call
            //   of the backend fail, a larger one would let calls wait.
            //
            // Given
            val pool = WebClientProperties.Pool(pendingAcquireMaxCount = 0)

            // When/Then
            assertThatThrownBy { factory(WebClientProperties.Engine(default = WebClientEngine.JETTY), pool = pool) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContainingAll("app.webclient.pool.pending-acquire-max-count=0", "app.webclient.engine", "[orders]")
            assertThatCode { factory(WebClientProperties.Engine(), pool = pool) }.doesNotThrowAnyException()
        }

        @ParameterizedTest(name = "max-connections {0}, pending-acquire-max-count {1} -> {2}")
        @CsvSource("100, 100, 200", "1, 5, 6", "100, -1, 2147483647", "2147483647, 5, 2147483647")
        fun `should size Jetty's queue as the connections plus the calls that may wait`(
            maxConnections: Int,
            pendingAcquireMaxCount: Int,
            expected: Int,
        ) {
            // What is tested: the Jetty queue capacity derived from the mesh and pool settings.
            // Success criteria: connections + count, saturating at Int.MAX_VALUE; -1 (unbounded) is
            //   Int.MAX_VALUE.
            // Why it matters: Jetty enqueues every request before it gets a connection; a capacity of the count
            //   alone would reject a burst on a cold pool at half the concurrency Reactor Netty accepts for the
            //   same settings, and the sum must not wrap to a negative capacity.
            //
            // Given/When/Then
            assertThat(JettyTransport.jettyQueueCapacity(maxConnections, pendingAcquireMaxCount)).isEqualTo(expected)
        }

        @Test
        fun `should explain the JVM flag when the JDK client refuses the Host header`() {
            // What is tested: the message of the Host-header check, with the JDK's answer forced to "refused".
            // Success criteria: the failure names app.webclient.engine, the jdk backends and the flag
            //   -Djdk.httpclient.allowRestrictedHeaders=host; with the answer "allowed" the check passes, and it
            //   is not consulted at all when no backend is on jdk.
            // Why it matters: this JVM starts with the flag (see the module POM), so the refusal path is driven
            //   here through the check's seam; JdkEngineRestrictedHostHeaderTest pins it against a real JVM
            //   without the flag.
            //
            // Given
            val engines = mapOf("orders" to WebClientEngine.JDK, "payments" to WebClientEngine.REACTOR_NETTY)

            // When/Then
            assertThatThrownBy { WebClientEngineChecks.requireJdkHostHeader(engines) { false } }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContainingAll("app.webclient.engine selects 'jdk' for [orders]", "-Djdk.httpclient.allowRestrictedHeaders=host")
            assertThatCode { WebClientEngineChecks.requireJdkHostHeader(engines) { true } }.doesNotThrowAnyException()
            assertThatCode {
                WebClientEngineChecks.requireJdkHostHeader(mapOf("payments" to WebClientEngine.REACTOR_NETTY)) { error("must not be asked") }
            }.doesNotThrowAnyException()
        }

        @Test
        fun `should find the JDK client accepting the Host header in this test JVM`() {
            // What is tested: the premise of every jdk-engine test in this JVM.
            // Success criteria: the check's probe answers "allowed", and the property carries `host` - set by the
            //   POM for the test JVM, not by production code.
            // Why it matters: should the POM setting ever get lost, every jdk case would fail with the startup
            //   message; this test says why in one line.
            //
            // Given/When/Then
            assertThat(System.getProperty(EngineSelection.JDK_ALLOW_RESTRICTED_HEADERS)).isEqualTo("host")
            assertThat(EngineSelection.jdkAllowsHostHeader()).isTrue()
        }

        @Test
        fun `should report a pool per Reactor Netty backend and one per Jetty or Apache engine for the startup guard`() {
            // What is tested: connectionPools() - what the factory tells the core's startup guard about the pools
            //   the bulkheads are sized against.
            // Success criteria: two backends on reactor-netty give two pools of one backend each; two on apache
            //   share one pool, one on jetty has its own; every pool carries max-connections (100).
            // Why it matters: Reactor Netty keeps a sub-pool per backend (each backend's client is configured on
            //   its own), Jetty and Apache one per destination - which all backends behind the sidecar are. A guard
            //   that summed every bulkhead against one pool would warn about a correctly sized configuration.
            //
            // Given
            val engine =
                WebClientProperties.Engine(
                    endpoints = mapOf("stock" to WebClientEngine.JETTY, "billing" to WebClientEngine.APACHE, "ledger" to WebClientEngine.APACHE),
                )

            // When
            val pools = factory(engine, aliases = listOf("orders", "payments", "stock", "billing", "ledger")).connectionPools()

            // Then
            assertThat(pools).containsExactlyInAnyOrder(
                MeshConnectionPool("webclient-reactor-netty-orders", 100, setOf("orders")),
                MeshConnectionPool("webclient-reactor-netty-payments", 100, setOf("payments")),
                MeshConnectionPool("webclient-jetty", 100, setOf("stock")),
                MeshConnectionPool("webclient-apache", 100, setOf("billing", "ledger")),
            )
        }

        private fun factory(
            engine: WebClientProperties.Engine,
            aliases: List<String> = listOf("orders"),
            pool: WebClientProperties.Pool = WebClientProperties.Pool(),
            resourceLoader: DefaultResourceLoader? = null,
        ): ResilientWebClientFactory =
            ResilientWebClientFactory(
                WebClientProperties(engine = engine, pool = pool),
                MeshProperties(
                    endpoints =
                        aliases.map { alias ->
                            MeshProperties.EndpointConfig(triplet = "/endpoint/localhost/GET/$alias", alias = alias, baseUrl = "http://localhost:1")
                        },
                ),
                ReactiveResilienceDecorator(CircuitBreakerRegistry.ofDefaults(), TimeLimiterRegistry.ofDefaults(), BulkheadRegistry.ofDefaults()),
                JsonMapper.builder().build(),
                // Deliberately no Spring context: a bare builder stands in for Boot's bean. No client is built,
                // so no engine resource is created and nothing needs shutting down.
                webClientBuilder = WebClient.builder(),
                resourceLoader = resourceLoader,
            )
    }
}
