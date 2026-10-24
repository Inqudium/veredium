package eu.inqudium.veredium.webclient

import eu.inqudium.veredium.core.engine.EngineSelection
import eu.inqudium.veredium.core.mesh.MeshProperties
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.springframework.web.reactive.function.client.WebClient
import tools.jackson.databind.json.JsonMapper

/**
 * The jdk engine in a JVM started WITHOUT `jdk.httpclient.allowRestrictedHeaders` - the state a host is in when
 * it selects the engine and forgets the flag.
 *
 * Runs in its own Surefire execution (`jdk-restricted-host-header` in the module POM), because the main test JVM
 * starts with the flag. Assumption: the JDK reads it only once (JDK 21) - so no test in that JVM can see the
 * restriction, and no test may lift or impose it by setting the property, which would change every
 * `java.net.http` client of the JVM.
 */
class JdkEngineRestrictedHostHeaderTest {
    @Test
    fun `should fail the factory naming the flag when the JDK client refuses the Host header`() {
        // What is tested: a factory with one backend on jdk and one on reactor-netty, in a JVM whose JDK client
        //   refuses an explicit Host header.
        // Success criteria: the premise holds (no property, the probe answers "refused"); construction fails
        //   naming app.webclient.engine, the jdk backend only, and -Djdk.httpclient.allowRestrictedHeaders=host;
        //   the property is still unset afterwards; a factory without a jdk backend constructs in the same JVM.
        // Why it matters: every client of this module sends the mesh Host header. Without the check the jdk
        //   backend would fail every request with `restricted header name: "Host"` - an IllegalArgumentException
        //   that names no configuration. And the fix must stay the host's: setting the property from the library
        //   would lift the restriction for every other java.net.http client of the process.
        //
        // Given
        assertThat(System.getProperty(EngineSelection.JDK_ALLOW_RESTRICTED_HEADERS)).isNull()
        assertThat(EngineSelection.jdkAllowsHostHeader()).isFalse()
        val mixed = WebClientProperties.Engine(endpoints = mapOf("orders" to WebClientEngine.JDK))

        // When / Then
        assertThatThrownBy { factory(mixed) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContainingAll("app.webclient.engine selects 'jdk' for [orders]", "-Djdk.httpclient.allowRestrictedHeaders=host")
        assertThat(System.getProperty(EngineSelection.JDK_ALLOW_RESTRICTED_HEADERS)).isNull()
        assertThatCode { factory(WebClientProperties.Engine()).shutdown() }.doesNotThrowAnyException()
    }

    private fun factory(engine: WebClientProperties.Engine) =
        ResilientWebClientFactory(
            WebClientProperties(engine = engine),
            MeshProperties(
                endpoints =
                    listOf("orders", "payments").map { alias ->
                        MeshProperties.EndpointConfig(triplet = "/endpoint/localhost/GET/$alias", alias = alias, baseUrl = "http://localhost:1")
                    },
            ),
            ReactiveResilienceDecorator(CircuitBreakerRegistry.ofDefaults(), TimeLimiterRegistry.ofDefaults(), BulkheadRegistry.ofDefaults()),
            JsonMapper.builder().build(),
            // Deliberately no Spring context: a bare builder stands in for Boot's bean; no client is built.
            webClientBuilder = WebClient.builder(),
        )
}
