package eu.inqudium.veredium.restclient.mesh

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.context.properties.source.ConfigurationPropertySources
import org.springframework.boot.env.YamlPropertySourceLoader
import org.springframework.core.io.ClassPathResource

/**
 * Binds the test-owned `rest-client/src/test/resources/application-test.yml` into [MeshProperties] and verifies
 * the `app.mesh` block binds + derives as the runtime would read it.
 *
 * What/why: the mesh block is the single source of endpoint identity + the timeout ladder (see the
 * [MeshProperties] doc and `docs/mesh-timeout.md`). The pure [MeshPropertiesDerivationTest] proves the
 * derivation logic on hand-built inputs; this test pins a representative, COMPLETE `app.mesh` block and checks
 * it binds the way Spring Boot would (`YamlPropertySourceLoader` + `Binder`) - a changed or dropped endpoint, a
 * wrong `individual-request-timeout-ms`, an accidental `defaults` block, or a triplet that would derive a
 * colliding alias fails here. Success = every bound value equals what the fixture declares and what the code
 * derives from it.
 *
 * Deliberately binds the TEST fixture, not the shipped `src/main/resources/application.yml`: the shipped file is
 * operator-editable (e.g. enabling `app.mesh.defaults.circuit-breaker`, retuning an anchor), and a test must not
 * break on such an operational edit. The fixture is stable and test-owned. The one part of this block that is an
 * IDENTITY rather than a value - the alias set - is pinned in the shipped file as well, by `MeshAliasContractTest`
 * (see `ConfigurationFiles` for the rule).
 *
 * Determinism: a single fixture file (no profile-specific overrides), so there is no cross-file profile
 * precedence and the bound values are stable across platforms.
 */
class MeshApplicationYmlBindingTest {
    /** The `app.mesh` block bound from the test fixture, the way the container binds the shipped file at startup. */
    private val mesh: MeshProperties = bindMeshFromFixture()

    @Test
    fun `binds exactly the four mesh endpoints declared in the test fixture, in declaration order`() {
        // Given the fixture application-test.yml, When bound into MeshProperties,
        // Then the endpoint triplets are exactly those declared, in order.
        assertThat(mesh.endpoints.map { it.triplet })
            .containsExactlyElementsOf(EXPECTED.map { it.triplet })
    }

    @Test
    fun `binds each endpoint's individual request timeout and leaves every other endpoint field unset`() {
        // What/why: the file sets ONLY the triplet + the measured anchor per endpoint; everything else must
        //   fall through to the defaults/derivation. Asserting the other fields are unset proves the file did
        //   not silently pin an alias, retries, base-url or a backend toggle.
        // Given / When: the bound endpoints
        mesh.endpoints.forEachIndexed { i, endpoint ->
            val expected = EXPECTED[i]
            // Then: the anchor is the shipped value...
            assertThat(endpoint.individualRequestTimeoutMs)
                .describedAs("individual-request-timeout-ms for %s", expected.triplet)
                .isEqualTo(expected.timeoutMs)
            // ...and no other endpoint-level field is set in the file.
            assertThat(endpoint.alias).describedAs("alias for %s", expected.triplet).isNull()
            assertThat(endpoint.maxRetries).describedAs("max-retries for %s", expected.triplet).isNull()
            assertThat(endpoint.margins).describedAs("margins for %s", expected.triplet).isNull()
            assertThat(endpoint.timeLimiter).describedAs("time-limiter for %s", expected.triplet).isNull()
            assertThat(endpoint.circuitBreaker).describedAs("circuit-breaker for %s", expected.triplet).isNull()
        }
    }

    @Test
    fun `omits the defaults block so the shared defaults fall back to the Kotlin defaults`() {
        // What/why: the fixture, like the shipped file, deliberately omits `app.mesh.defaults` (every endpoint sets its own anchor;
        //   the policy knobs default in Kotlin). A bound `defaults` equal to a fresh Defaults() is the direct
        //   evidence that no defaults leaked in - important because a stray default would silently reshape
        //   every endpoint's derived ladder.
        // Given / When: the bound defaults, Then they equal the code defaults.
        assertThat(mesh.defaults).isEqualTo(MeshProperties.Defaults())
        // Spell out the load-bearing ones for a readable failure.
        assertThat(mesh.defaults.individualRequestTimeoutMs).isEqualTo(4000)
        assertThat(mesh.defaults.maxRetries).isEqualTo(0)
        assertThat(mesh.defaults.timeLimiter.enabled).isTrue()
        assertThat(mesh.defaults.circuitBreaker.enabled).isFalse()
        assertThat(mesh.defaults.transport.sidecarBaseUrl).isEqualTo("http://localhost:8042")
    }

    @Test
    fun `derives a unique, non-colliding alias from every endpoint triplet`() {
        // What/why: the derived alias is the R4j instance name / metric label and the adapters' ALIAS contract;
        //   MeshProperties.init throws on a collision, so a fresh bind already fails on duplicates - here we
        //   additionally pin the exact derived value per endpoint so a triplet edit that renames a label is caught.
        // Given / When: the derived aliases, Then each matches the expected label and all are distinct.
        val aliases = mesh.endpoints.map { it.effectiveAlias() }
        assertThat(aliases).containsExactlyElementsOf(EXPECTED.map { it.alias })
        assertThat(aliases).doesNotHaveDuplicates()
    }

    @Test
    fun `derives the host and the path template from every endpoint triplet`() {
        // Given / When: the per-endpoint host + path template derived from the triplet,
        // Then: each matches the upstream host and the {var}-preserving path from the file.
        mesh.endpoints.forEachIndexed { i, endpoint ->
            val expected = EXPECTED[i]
            assertThat(endpoint.host()).describedAs("host for %s", expected.triplet).isEqualTo(expected.host)
            assertThat(endpoint.pathTemplate())
                .describedAs("path template for %s", expected.triplet)
                .isEqualTo(expected.pathTemplate)
        }
    }

    @Test
    fun `resolves one ladder per triplet carrying the endpoint anchor and the shared default policy`() {
        // What/why: resolveAll() is what the factory consumes; this pins that the file's anchor reaches the
        //   resolved ladder unchanged and the omitted-defaults policy (retries 0, TimeLimiter on, CB off,
        //   sidecar base URL) is applied to every endpoint.
        // Given / When: the resolved map keyed by triplet,
        val resolved = mesh.resolveAll()

        // Then: exactly one entry per declared triplet...
        assertThat(resolved.keys).containsExactlyInAnyOrderElementsOf(EXPECTED.map { it.triplet })

        // ...and each entry carries the endpoint's anchor + alias/host/path and the shared default policy.
        EXPECTED.forEach { expected ->
            val ladder = resolved.getValue(expected.triplet)
            assertThat(ladder.alias).describedAs("resolved alias for %s", expected.triplet).isEqualTo(expected.alias)
            assertThat(ladder.host).describedAs("resolved host for %s", expected.triplet).isEqualTo(expected.host)
            assertThat(ladder.pathTemplate)
                .describedAs("resolved path for %s", expected.triplet)
                .isEqualTo(expected.pathTemplate)
            assertThat(ladder.individualRequestTimeoutMs)
                .describedAs("resolved anchor for %s", expected.triplet)
                .isEqualTo(expected.timeoutMs)
            assertThat(ladder.maxRetries).describedAs("resolved max-retries for %s", expected.triplet).isEqualTo(0)
            assertThat(ladder.baseUrl)
                .describedAs("resolved base-url for %s", expected.triplet)
                .isEqualTo("http://localhost:8042")
            assertThat(ladder.timeLimiterEnabled).describedAs("time-limiter on for %s", expected.triplet).isTrue()
            assertThat(ladder.circuitBreakerEnabled).describedAs("circuit-breaker off for %s", expected.triplet).isFalse()
        }
    }

    private fun bindMeshFromFixture(): MeshProperties {
        // Bind the TEST fixture the way Spring Boot binds a config file at startup (see the class doc for why the
        // fixture, not the shipped src/main application.yml).
        val sources = YamlPropertySourceLoader().load("application-test.yml", ClassPathResource("application-test.yml"))
        return Binder(ConfigurationPropertySources.from(sources))
            .bind("app.mesh", MeshProperties::class.java)
            .orElseThrow { AssertionError("app.mesh did not bind from application-test.yml") }
    }

    /** The endpoints the fixture declares, with the values the code derives from each triplet. */
    private data class ExpectedEndpoint(
        val triplet: String,
        val timeoutMs: Long,
        val alias: String,
        val host: String,
        val pathTemplate: String,
    )

    private companion object {
        val EXPECTED =
            listOf(
                ExpectedEndpoint(
                    triplet = "/endpoint/apis.example.com/POST/v8/agreements/products/lookup",
                    timeoutMs = 400,
                    alias = "apis-example-com-POST-v8-agreements-products-lookup",
                    host = "apis.example.com",
                    pathTemplate = "/v8/agreements/products/lookup",
                ),
                ExpectedEndpoint(
                    triplet = "/endpoint/api.example.com/GET/v5/access-profiles/by-involved-party/{id}",
                    timeoutMs = 300,
                    alias = "api-example-com-GET-v5-access-profiles-by-involved-party",
                    host = "api.example.com",
                    pathTemplate = "/v5/access-profiles/by-involved-party/{id}",
                ),
                ExpectedEndpoint(
                    triplet = "/endpoint/apis.example.com/POST/v5/involved-parties/lookup",
                    timeoutMs = 500,
                    alias = "apis-example-com-POST-v5-involved-parties-lookup",
                    host = "apis.example.com",
                    pathTemplate = "/v5/involved-parties/lookup",
                ),
                ExpectedEndpoint(
                    triplet = "/endpoint/apis.example.com/GET/v6/involved-parties/{uuid}",
                    timeoutMs = 400,
                    alias = "apis-example-com-GET-v6-involved-parties",
                    host = "apis.example.com",
                    pathTemplate = "/v6/involved-parties/{uuid}",
                ),
            )
    }
}
