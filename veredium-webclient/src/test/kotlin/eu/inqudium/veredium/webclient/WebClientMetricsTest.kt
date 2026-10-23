package eu.inqudium.veredium.webclient

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

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
    inner class `Known paths map to their template` {
        @Test
        fun `should map the product-agreements lookup path to its template`() {
            // What is tested: a variable-free template - the path IS the template.
            // Success criteria: the exact path maps to its own template, not to another endpoint's.
            // Why it matters: the simplest case of the mapping; if it fails, every static endpoint's latency
            //   lands in the catch-all bucket and its own series stays empty.
            //
            // Given / When / Then
            assertThat(WebClientMetrics.uriTag("/v8/agreements/products/lookup", tags)).isEqualTo(productAgreements)
        }

        @Test
        fun `should map the involved-parties POST lookup path to its POST template`() {
            // What is tested: a static path next to a templated one on the same resource in another version
            //   (`/v5/involved-parties/lookup` and `/v6/involved-parties/{uuid}`).
            // Success criteria: the lookup path maps to the lookup template.
            // Why it matters: near-identical paths are where a loose matcher picks the wrong template, and two
            //   endpoints' numbers would then merge into one series.
            //
            // Given / When / Then
            assertThat(WebClientMetrics.uriTag("/v5/involved-parties/lookup", tags)).isEqualTo(involvedPartiesLookup)
        }

        @Test
        fun `should map a concrete involved-party GET path with a UUID to the templated GET tag`() {
            // What is tested: the cardinality guard proper - a path variable collapsed into its template.
            // Success criteria: a path carrying a UUID maps to `/v6/involved-parties/{uuid}`.
            // Why it matters: a real call carries a per-request UUID in the path - exactly the value that would
            //   mint one time series per call if it reached the tag raw, until the metric backend refuses more.
            //
            // Given: a concrete path with a per-request UUID
            val uri = "/v6/involved-parties/123e4567-e89b-12d3-a456-426614174000"

            // When / Then: the UUID collapses into the fixed template, so all such calls share one series.
            assertThat(WebClientMetrics.uriTag(uri, tags)).isEqualTo(involvedPartyGet)
        }

        @Test
        fun `should map a concrete access-profiles path with an id to the access-profiles template`() {
            // What is tested: a template whose variable follows several static segments.
            // Success criteria: `/v5/access-profiles/by-involved-party/987654` maps to its template.
            // Why it matters: the prefix is everything before the first variable; a deeper static part is where
            //   an off-by-one in that cut would show first.
            //
            // Given / When / Then
            assertThat(WebClientMetrics.uriTag("/v5/access-profiles/by-involved-party/987654", tags))
                .isEqualTo(accessProfiles)
        }
    }

    @Nested
    inner class `Per-endpoint scoping` {
        @Test
        fun `should tag a call with the endpoint's own template`() {
            // What is tested: uriTagger - the per-endpoint mapper an endpoint's HttpClient receives.
            // Success criteria: it maps a call on its own endpoint to its own template.
            // Why it matters: the production path uses this form, not tagsOf over all templates; the scoping test
            //   below is only meaningful next to this positive case.
            //
            // Given / When / Then: the mapper knows one template - its own
            val tagger = WebClientMetrics.uriTagger(involvedPartyGet)

            assertThat(tagger("/v6/involved-parties/123e4567-e89b-12d3-a456-426614174000")).isEqualTo(involvedPartyGet)
        }

        @Test
        fun `should never label a call with a foreign endpoint's template`() {
            // What is tested: that a per-endpoint mapper cannot attribute a request to a DIFFERENT endpoint.
            // Success criteria: asked about another endpoint's path, the mapper answers UNKNOWN rather
            //   than that endpoint's template.
            // Why it matters: every endpoint gets its own HttpClient; a mapper built from the templates of ALL
            //   of them would let a request match a foreign template first and be counted under another
            //   endpoint's series - two backends' latencies in one number, silently. Scoping the mapper to the
            //   single template its client can actually call rules that out by construction.
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
            // Success criteria: `/a/{id}` and `/a/{id}/details` both reduce to `/a/`, and tagsOf returns
            //   them in the same order regardless of the order they went in.
            // Why it matters: with a length-only sort the two are tied, so which one a URI is labelled with
            //   depends on the iteration order of the input - a mislabelling that would differ between runs
            //   and never reproduce. Production passes one template per endpoint, but the function is public
            //   and this removes the ambiguity at its source rather than only at the call site.
            //
            // Given: the same two templates in both input orders
            val forwards = WebClientMetrics.tagsOf(listOf("/a/{id}", "/a/{id}/details"))
            val backwards = WebClientMetrics.tagsOf(listOf("/a/{id}/details", "/a/{id}"))

            // When / Then
            assertThat(forwards).isEqualTo(backwards)
        }
    }

    @Nested
    inner class `Cardinality guards` {
        @Test
        fun `should ignore a trailing query string when matching the prefix`() {
            // What is tested: a query string on a templated path.
            // Success criteria: the path with a query maps to the same template as without.
            // Why it matters: query values are as unbounded as path variables; a matcher that folded them into
            //   the tag would leak cardinality through the back door.
            //
            // Given: a query string past the matched prefix
            val uri = "/v8/agreements/products/lookup?productAgreementStatus=ACTV&fields=a,b"

            // When / Then
            assertThat(WebClientMetrics.uriTag(uri, tags)).isEqualTo(productAgreements)
        }

        @Test
        fun `should prefer the longer prefix when one path nests inside another`() {
            // What is tested: a static path whose prefix is contained in a shorter templated path's prefix must
            //   still resolve to its own (more specific) tag.
            // Success criteria: next to a "/a/b/{id}" template, "/a/b/lookup" maps to "/a/b/lookup" and "/a/b/42"
            //   to "/a/b/{id}" - the shorter prefix "/a/b/" would swallow the lookup if order were undefined.
            // Why it matters: tagsOf sorts longest-prefix-first precisely so a nested path cannot be mislabelled.
            //
            // Given
            val nested = WebClientMetrics.tagsOf(listOf("/a/b/{id}", "/a/b/lookup"))

            // When / Then
            assertThat(WebClientMetrics.uriTag("/a/b/lookup", nested)).isEqualTo("/a/b/lookup")
            assertThat(WebClientMetrics.uriTag("/a/b/42", nested)).isEqualTo("/a/b/{id}")
        }

        @Test
        fun `should collapse an unknown path into the catch-all bucket`() {
            // What is tested: a path no template knows.
            // Success criteria: it maps to UNKNOWN.
            // Why it matters: an unforeseen path must never blow up the metric backend with a fresh series; one
            //   bounded bucket for everything unknown is what keeps the tag set finite.
            //
            // Given / When / Then
            assertThat(WebClientMetrics.uriTag("/actuator/health", tags)).isEqualTo(WebClientMetrics.UNKNOWN)
        }

        @Test
        fun `should collapse an empty path into the catch-all bucket`() {
            // What is tested: the empty URI.
            // Success criteria: it maps to UNKNOWN rather than matching some template's empty prefix.
            // Why it matters: an empty static prefix would match everything; this pins that no template claims
            //   a path it does not start.
            //
            // Given / When / Then
            assertThat(WebClientMetrics.uriTag("", tags)).isEqualTo(WebClientMetrics.UNKNOWN)
        }

        @Test
        fun `should not claim a longer path that merely starts with a variable-free template`() {
            // What is tested: that a template WITHOUT a path variable does not swallow unrelated paths that share
            //   its text. Such a template yields a prefix equal to the whole path, so a plain startsWith would
            //   match anything continuing it.
            // Success criteria: the sibling paths land in the catch-all, while the exact path, a deeper
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
