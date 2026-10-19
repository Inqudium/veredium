package eu.inqudium.veredium.webclient

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

/**
 * Tests for [WebClientMetrics], the URI→tag mapper that protects the metric backend from a cardinality
 * explosion. Every request URI must collapse to a low-cardinality template (or the single
 * [WebClientMetrics.UNKNOWN] bucket), because a path variable left raw would mint one time series per call.
 * The templates are the mesh endpoints' path templates ([WebClientMetrics.tagsOf]); these tests pin the
 * contract: known paths map to their template, a per-call UUID never leaks, a query string is ignored, and
 * anything unforeseen falls into the catch-all bucket.
 */
class WebClientMetricsTest {
    private val productAgreements = "/v8/agreements/products/lookup"
    private val involvedPartiesLookup = "/v5/involved-parties/lookup"
    private val involvedPartyGet = "/v6/involved-parties/{uuid}"
    private val accessProfiles = "/v5/access-profiles/by-involved-party/{id}"

    private val tags =
        WebClientMetrics.tagsOf(listOf(productAgreements, involvedPartiesLookup, involvedPartyGet, accessProfiles))

    @Nested
    inner class KnownPathsMapToTheirTemplate {
        @Test
        fun `should map the product-agreements lookup path to its template`() {
            // Given / When / Then
            assertThat(WebClientMetrics.uriTag("/v8/agreements/products/lookup", tags)).isEqualTo(productAgreements)
        }

        @Test
        fun `should map the involved-parties POST lookup path to its POST template`() {
            // Given / When / Then
            assertThat(WebClientMetrics.uriTag("/v5/involved-parties/lookup", tags)).isEqualTo(involvedPartiesLookup)
        }

        @Test
        fun `should map a concrete involved-party GET path with a UUID to the templated GET tag`() {
            // Given: a real call carries a per-request UUID in the path - exactly the value that would explode
            // metric cardinality if it reached the tag raw.
            val uri = "/v6/involved-parties/123e4567-e89b-12d3-a456-426614174000"

            // When / Then: the UUID collapses into the fixed template, so all such calls share one series.
            assertThat(WebClientMetrics.uriTag(uri, tags)).isEqualTo(involvedPartyGet)
        }

        @Test
        fun `should map a concrete access-profiles path with an id to the access-profiles template`() {
            // Given / When / Then
            assertThat(WebClientMetrics.uriTag("/v5/access-profiles/by-involved-party/987654", tags))
                .isEqualTo(accessProfiles)
        }
    }

    @Nested
    inner class PerEndpointScoping {
        @Test
        fun `should tag a call with the endpoint's own template`() {
            // Given / When / Then: the mapper an endpoint's HttpClient receives knows one template - its own.
            val tagger = WebClientMetrics.uriTagger(involvedPartyGet)

            assertThat(tagger("/v6/involved-parties/123e4567-e89b-12d3-a456-426614174000")).isEqualTo(involvedPartyGet)
        }

        @Test
        fun `should never label a call with a foreign endpoint's template`() {
            // What is tested: that a per-endpoint mapper cannot attribute a request to a DIFFERENT endpoint.
            // How success is judged: asked about another endpoint's path, the mapper answers UNKNOWN rather
            //   than that endpoint's template.
            // Why it matters: every endpoint gets its own HttpClient, but the mapper used to be built from the
            //   templates of ALL of them, so a request could match a foreign template first and be counted
            //   under another endpoint's series - two backends' latencies in one number, silently. Scoping the
            //   mapper to the single template its client can actually call removes that by construction.
            //
            // Given: the mapper belonging to the access-profiles endpoint
            val tagger = WebClientMetrics.uriTagger(accessProfiles)

            // When / Then: another endpoint's path is not its business
            assertThat(tagger("/v6/involved-parties/abc")).isEqualTo(WebClientMetrics.UNKNOWN)
            assertThat(tagger(productAgreements)).isEqualTo(WebClientMetrics.UNKNOWN)
            // ... while its own still resolves
            assertThat(tagger("/v5/access-profiles/by-involved-party/987654")).isEqualTo(accessProfiles)
        }

        @Test
        fun `should order templates deterministically when two share a prefix`() {
            // What is tested: that templates reducing to the SAME static prefix get a defined order.
            // How success is judged: `/a/{id}` and `/a/{id}/details` both reduce to `/a/`, and tagsOf returns
            //   them in the same order regardless of the order they went in.
            // Why it matters: with a length-only sort the two are tied, so which one a URI is labelled with
            //   depended on the iteration order of the input - a mislabelling that would differ between runs
            //   and never reproduce. Production no longer passes more than one template, but the function is
            //   public and this removes the ambiguity at its source rather than only at the call site.
            //
            // Given: the same two templates in both input orders
            val forwards = WebClientMetrics.tagsOf(listOf("/a/{id}", "/a/{id}/details"))
            val backwards = WebClientMetrics.tagsOf(listOf("/a/{id}/details", "/a/{id}"))

            // When / Then
            assertThat(forwards).isEqualTo(backwards)
        }
    }

    @Nested
    inner class CardinalityGuards {
        @Test
        fun `should ignore a trailing query string when matching the prefix`() {
            // Given: a query string sits past the matched prefix and must not change the bucket.
            val uri = "/v8/agreements/products/lookup?productAgreementStatus=ACTV&fields=a,b"

            // When / Then
            assertThat(WebClientMetrics.uriTag(uri, tags)).isEqualTo(productAgreements)
        }

        @Test
        fun `should prefer the longer prefix when one path nests inside another`() {
            // What is tested: a static path whose prefix is contained in a shorter templated path's prefix must
            // still resolve to its own (more specific) tag. How: a "/a/b/lookup" static path next to a "/a/b/{id}"
            // template - the shorter prefix "/a/b/" would swallow "/a/b/lookup" if order were undefined.
            // Why it matters: tagsOf sorts longest-prefix-first precisely so a nested path cannot be mislabelled.
            val nested = WebClientMetrics.tagsOf(listOf("/a/b/{id}", "/a/b/lookup"))

            // When / Then
            assertThat(WebClientMetrics.uriTag("/a/b/lookup", nested)).isEqualTo("/a/b/lookup")
            assertThat(WebClientMetrics.uriTag("/a/b/42", nested)).isEqualTo("/a/b/{id}")
        }

        @Test
        fun `should collapse an unknown path into the catch-all bucket`() {
            // Given: an unforeseen path must never blow up the metric backend with a fresh series.
            // When / Then
            assertThat(WebClientMetrics.uriTag("/actuator/health", tags)).isEqualTo(WebClientMetrics.UNKNOWN)
        }

        @Test
        fun `should collapse an empty path into the catch-all bucket`() {
            // Given / When / Then
            assertThat(WebClientMetrics.uriTag("", tags)).isEqualTo(WebClientMetrics.UNKNOWN)
        }

        @Test
        fun `should not claim a longer path that merely starts with a variable-free template`() {
            // What is tested: that a template WITHOUT a path variable does not swallow unrelated paths that share
            //   its text. Such a template yields a prefix equal to the whole path, so a plain startsWith would
            //   match anything continuing it.
            // How success is judged: the sibling paths land in the catch-all, while the exact path, a deeper
            //   segment and a query-string variant still resolve to the template.
            // Why it matters: a mislabelled URI does not just tag one call wrongly - it merges a DIFFERENT
            //   endpoint's timings into this template's series, so the metric quietly reports on two endpoints at
            //   once and neither latency figure means what it claims.
            //
            // Given: a static (variable-free) template
            val staticOnly = WebClientMetrics.tagsOf(listOf("/v8/agreements/products/lookup"))

            // When / Then: a path that merely CONTINUES the text is a different endpoint -> catch-all
            assertThat(WebClientMetrics.uriTag("/v8/agreements/products/lookup-v2", staticOnly))
                .isEqualTo(WebClientMetrics.UNKNOWN)
            assertThat(WebClientMetrics.uriTag("/v8/agreements/products/lookupsomething", staticOnly))
                .isEqualTo(WebClientMetrics.UNKNOWN)

            // And: the legitimate forms still match - exact, deeper segment, and with a query string
            assertThat(WebClientMetrics.uriTag("/v8/agreements/products/lookup", staticOnly))
                .isEqualTo("/v8/agreements/products/lookup")
            assertThat(WebClientMetrics.uriTag("/v8/agreements/products/lookup/42", staticOnly))
                .isEqualTo("/v8/agreements/products/lookup")
            assertThat(WebClientMetrics.uriTag("/v8/agreements/products/lookup?fields=a", staticOnly))
                .isEqualTo("/v8/agreements/products/lookup")
        }
    }
}
