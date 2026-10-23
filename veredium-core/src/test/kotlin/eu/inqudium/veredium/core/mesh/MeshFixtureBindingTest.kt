package eu.inqudium.veredium.core.mesh

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.context.properties.source.ConfigurationPropertySources
import org.springframework.boot.env.YamlPropertySourceLoader
import org.springframework.core.io.ClassPathResource

/**
 * Binds the test-owned `veredium-core/src/test/resources/application-test.yml` into [MeshProperties] and
 * verifies the `app.mesh` block binds + derives as the runtime would read it - for both client stacks, which
 * bind this one tree from here.
 *
 * The mesh block is the single source of endpoint identity + the timeout ladder (see the [MeshProperties] doc
 * and `docs/mesh-timeout.md`). The pure [MeshPropertiesDerivationTest] proves the derivation logic on hand-built
 * inputs; this test pins a representative, COMPLETE `app.mesh` block and checks it binds the way Spring Boot
 * binds a config file (`YamlPropertySourceLoader` + `Binder`), so every bound value equals what the fixture
 * declares and what the code derives from it - a changed or dropped endpoint, a wrong
 * `individual-request-timeout-ms`, an accidental `defaults` block, or a triplet that would derive a colliding
 * alias fails here.
 *
 * Deliberately binds a TEST fixture: a library ships no `application.yml`, and a host's file is operator-editable
 * (e.g. enabling `app.mesh.defaults.circuit-breaker`, retuning an anchor), so a test must not depend on it. The
 * fixture is stable and test-owned.
 *
 * Determinism: a single fixture file (no profile-specific overrides), so there is no cross-file profile
 * precedence and the bound values are stable across platforms.
 */
class MeshFixtureBindingTest {
    /** The `app.mesh` block bound from the test fixture, the way Spring Boot binds a config file at startup. */
    private val mesh: MeshProperties = bindMeshFromFixture()

    @Test
    fun `should bind exactly the four mesh endpoints declared in the test fixture, in declaration order`() {
        // What is tested: that binding the fixture's `app.mesh.endpoints` list through Spring Boot's
        //   `YamlPropertySourceLoader` + `Binder` yields exactly the declared endpoint triplets.
        // Success criteria: the bound triplets equal the four expected ones element by element, in the
        //   fixture's declaration order - no endpoint dropped, added or reordered.
        // Why it matters: the triplet is the endpoint's identity (alias, host, path and ladder derive from it);
        //   an endpoint lost in binding would leave its client without a ladder and fail only at the call site.
        //
        // Given: the fixture application-test.yml, bound into MeshProperties
        // When / Then: the endpoint triplets are exactly those declared, in order
        assertThat(mesh.endpoints.map { it.triplet })
            .containsExactlyElementsOf(EXPECTED.map { it.triplet })
    }

    @Test
    fun `should bind each endpoint's individual request timeout and leave every other endpoint field unset`() {
        // What is tested: that each bound endpoint carries the fixture's `individual-request-timeout-ms` and
        //   nothing else - alias, max-retries, margins, base-url, headers and both backend toggles stay unset.
        // Success criteria: every endpoint's anchor equals the expected value and every other endpoint-level
        //   field is null (headers empty).
        // Why it matters: the fixture sets ONLY the triplet + the measured anchor per endpoint; everything else
        //   must fall through to the defaults/derivation. Asserting the other fields are unset proves the fixture
        //   does not silently pin an alias, retries, base-url or a backend toggle.
        //
        // Given / When: the bound endpoints
        mesh.endpoints.forEachIndexed { i, endpoint ->
            val expected = EXPECTED[i]
            // Then: the anchor is the fixture's value...
            assertThat(endpoint.individualRequestTimeoutMs)
                .`as`("individual-request-timeout-ms for %s", expected.triplet)
                .isEqualTo(expected.timeoutMs)
            // ...and no other endpoint-level field is set in the fixture.
            assertThat(endpoint.alias).`as`("alias for %s", expected.triplet).isNull()
            assertThat(endpoint.maxRetries).`as`("max-retries for %s", expected.triplet).isNull()
            assertThat(endpoint.baseUrl).`as`("base-url for %s", expected.triplet).isNull()
            assertThat(endpoint.headers).`as`("headers for %s", expected.triplet).isEmpty()
            assertThat(endpoint.timeLimiter).`as`("time-limiter for %s", expected.triplet).isNull()
            assertThat(endpoint.circuitBreaker).`as`("circuit-breaker for %s", expected.triplet).isNull()
        }
    }

    @Test
    fun `should fall back to the Kotlin defaults when the fixture omits the defaults block`() {
        // What is tested: that the fixture deliberately omits `app.mesh.defaults` (every endpoint sets its own
        //   anchor; the policy knobs default in Kotlin), so the bound defaults are the code's.
        // Success criteria: the bound `defaults` equals a fresh `MeshProperties.Defaults()`; the load-bearing
        //   values (4000ms anchor, 0 retries, TimeLimiter on, breaker off, sidecar URL) are spelled out.
        // Why it matters: a bound `defaults` equal to a fresh Defaults() is the direct evidence that no defaults
        //   leaked in - important because a stray default would silently reshape every endpoint's derived ladder.
        //
        // Given / When: the bound defaults
        // Then: they equal the code defaults
        assertThat(mesh.defaults).isEqualTo(MeshProperties.Defaults())
        // Spell out the load-bearing ones for a readable failure.
        assertThat(mesh.defaults.individualRequestTimeoutMs).isEqualTo(4000)
        assertThat(mesh.defaults.maxRetries).isEqualTo(0)
        assertThat(mesh.defaults.timeLimiter.enabled).isTrue()
        assertThat(mesh.defaults.circuitBreaker.enabled).isFalse()
        assertThat(mesh.defaults.transport.sidecarBaseUrl).isEqualTo("http://localhost:8042")
    }

    @Test
    fun `should derive a unique, non-colliding alias from every endpoint triplet`() {
        // What is tested: `effectiveAlias()` of every bound endpoint, derived from its triplet.
        // Success criteria: the aliases equal the expected host-prefixed labels in order and contain no duplicate.
        // Why it matters: the derived alias is the R4j instance name / metric label and the adapters' ALIAS
        //   contract; MeshProperties.init throws on a collision, so a fresh bind already fails on duplicates - here
        //   we additionally pin the exact derived value per endpoint so a triplet edit that renames a label is caught.
        //
        // Given / When: the derived aliases
        // Then: each matches the expected label and all are distinct
        val aliases = mesh.endpoints.map { it.effectiveAlias() }
        assertThat(aliases).containsExactlyElementsOf(EXPECTED.map { it.alias })
        assertThat(aliases).doesNotHaveDuplicates()
    }

    @Test
    fun `should derive the host and the path template from every endpoint triplet`() {
        // What is tested: `host()` and `pathTemplate()` of every bound endpoint, both parsed out of the triplet.
        // Success criteria: each host equals the upstream host and each path template equals the path with its
        //   `{var}` placeholders preserved, as the fixture declares them.
        // Why it matters: the host selects the sidecar's upstream and the path template is the metric URI and the
        //   request path; a parse that dropped or rewrote a `{var}` would route or label the call wrongly.
        //
        // Given / When: the per-endpoint host + path template derived from the triplet,
        // Then: each matches the upstream host and the {var}-preserving path from the fixture.
        mesh.endpoints.forEachIndexed { i, endpoint ->
            val expected = EXPECTED[i]
            assertThat(endpoint.host()).`as`("host for %s", expected.triplet).isEqualTo(expected.host)
            assertThat(endpoint.pathTemplate())
                .`as`("path template for %s", expected.triplet)
                .isEqualTo(expected.pathTemplate)
        }
    }

    @Test
    fun `should resolve one ladder per triplet carrying the endpoint anchor and the shared default policy`() {
        // What is tested: `resolveAll()` over the bound fixture - one resolved ladder per triplet with the
        //   endpoint's alias, host, path and anchor plus the shared default policy.
        // Success criteria: the keys are exactly the declared triplets, and each ladder carries the expected
        //   alias/host/path/anchor, 0 retries, the sidecar base URL, no headers, TimeLimiter on and breaker off.
        // Why it matters: resolveAll() is what the factory consumes; this pins that the fixture's anchor reaches
        //   the resolved ladder unchanged and the omitted-defaults policy (retries 0, TimeLimiter on, CB off,
        //   sidecar base URL) is applied to every endpoint.
        //
        // Given / When: the resolved map keyed by triplet,
        val resolved = mesh.resolveAll()

        // Then: exactly one entry per declared triplet...
        assertThat(resolved.keys).containsExactlyInAnyOrderElementsOf(EXPECTED.map { it.triplet })

        // ...and each entry carries the endpoint's anchor + alias/host/path and the shared default policy.
        EXPECTED.forEach { expected ->
            val ladder = resolved.getValue(expected.triplet)
            assertThat(ladder.alias).`as`("resolved alias for %s", expected.triplet).isEqualTo(expected.alias)
            assertThat(ladder.host).`as`("resolved host for %s", expected.triplet).isEqualTo(expected.host)
            assertThat(ladder.pathTemplate)
                .`as`("resolved path for %s", expected.triplet)
                .isEqualTo(expected.pathTemplate)
            assertThat(ladder.individualRequestTimeoutMs)
                .`as`("resolved anchor for %s", expected.triplet)
                .isEqualTo(expected.timeoutMs)
            assertThat(ladder.maxRetries).`as`("resolved max-retries for %s", expected.triplet).isEqualTo(0)
            assertThat(ladder.baseUrl)
                .`as`("resolved base-url for %s", expected.triplet)
                .isEqualTo("http://localhost:8042")
            assertThat(ladder.headers).`as`("resolved headers for %s", expected.triplet).isEmpty()
            assertThat(ladder.timeLimiterEnabled).`as`("time-limiter on for %s", expected.triplet).isTrue()
            assertThat(ladder.circuitBreakerEnabled).`as`("circuit-breaker off for %s", expected.triplet).isFalse()
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private fun bindMeshFromFixture(): MeshProperties {
        // Binds the TEST fixture the way Spring Boot binds a config file at startup (the class doc says why a
        // test-owned fixture).
        val sources = YamlPropertySourceLoader().load("application-test.yml", ClassPathResource("application-test.yml"))
        return Binder(ConfigurationPropertySources.from(sources))
            .bind(MeshProperties.PREFIX, MeshProperties::class.java)
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
