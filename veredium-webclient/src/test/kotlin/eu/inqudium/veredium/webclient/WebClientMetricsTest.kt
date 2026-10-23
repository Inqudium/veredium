package eu.inqudium.veredium.webclient

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

/**
 * Tests for [WebClientMetrics.uriTagger], the URI→tag mapper that protects the metric backend from a cardinality
 * explosion. Every request URI must collapse to a low-cardinality template (or the single
 * [WebClientMetrics.UNKNOWN] bucket), because a path variable left raw would mint one time series per call.
 * Each endpoint's mapper knows its own mesh path template only; these tests pin the contract: its own paths map to
 * its template, a per-call UUID never leaks, a query string is ignored, and anything else falls into the catch-all
 * bucket.
 */
class WebClientMetricsTest {
    private val productAgreements = "/v8/agreements/products/lookup"
    private val involvedPartiesLookup = "/v5/involved-parties/lookup"
    private val involvedPartyGet = "/v6/involved-parties/{uuid}"
    private val accessProfiles = "/v5/access-profiles/by-involved-party/{id}"

    @Nested
    inner class `Own paths map to the template` {
        @Test
        fun `should map a variable-free path to its template`() {
            // What is tested: a variable-free template - the path IS the template.
            // Success criteria: the exact path maps to its own template.
            // Why it matters: the simplest case of the mapping; if it fails, every static endpoint's latency
            //   lands in the catch-all bucket and its own series stays empty.
            //
            // Given
            val tagger = WebClientMetrics.uriTagger(productAgreements)

            // When / Then
            assertThat(tagger("/v8/agreements/products/lookup")).isEqualTo(productAgreements)
        }

        @Test
        fun `should map a concrete involved-party path with a UUID to its template`() {
            // What is tested: the cardinality guard proper - a path variable collapsed into its template.
            // Success criteria: a path carrying a UUID maps to `/v6/involved-parties/{uuid}`.
            // Why it matters: a real call carries a per-request UUID in the path - exactly the value that would
            //   mint one time series per call if it reached the tag raw, until the metric backend refuses more.
            //
            // Given: the endpoint's mapper, and a concrete path with a per-request UUID
            val tagger = WebClientMetrics.uriTagger(involvedPartyGet)
            val uri = "/v6/involved-parties/123e4567-e89b-12d3-a456-426614174000"

            // When / Then: the UUID collapses into the fixed template, so all such calls share one series.
            assertThat(tagger(uri)).isEqualTo(involvedPartyGet)
        }

        @Test
        fun `should map a concrete access-profiles path with an id to the access-profiles template`() {
            // What is tested: a template whose variable follows several static segments.
            // Success criteria: `/v5/access-profiles/by-involved-party/987654` maps to its template.
            // Why it matters: the prefix is everything before the first variable; a deeper static part is where
            //   an off-by-one in that cut would show first.
            //
            // Given
            val tagger = WebClientMetrics.uriTagger(accessProfiles)

            // When / Then
            assertThat(tagger("/v5/access-profiles/by-involved-party/987654")).isEqualTo(accessProfiles)
        }
    }

    @Nested
    inner class `Per-endpoint scoping` {
        @Test
        fun `should never label a call with a foreign endpoint's template`() {
            // What is tested: that a per-endpoint mapper cannot attribute a request to a DIFFERENT endpoint.
            // Success criteria: asked about other endpoints' paths - a near-identical one on the same resource
            //   included - the mapper answers UNKNOWN rather than a template.
            // Why it matters: every endpoint gets its own HttpClient; a mapper built from the templates of ALL
            //   of them would let a request match a foreign template first and be counted under another
            //   endpoint's series - two backends' latencies in one number, silently. Scoping the mapper to the
            //   single template its client can actually call rules that out by construction.
            //
            // Given: the mapper belonging to the involved-party endpoint
            val tagger = WebClientMetrics.uriTagger(involvedPartyGet)

            // When / Then: another endpoint's path is not its business
            assertThat(tagger(involvedPartiesLookup)).isEqualTo(WebClientMetrics.UNKNOWN)
            assertThat(tagger("/v5/access-profiles/by-involved-party/987654")).isEqualTo(WebClientMetrics.UNKNOWN)
            assertThat(tagger(productAgreements)).isEqualTo(WebClientMetrics.UNKNOWN)
            // ... while its own still resolves
            assertThat(tagger("/v6/involved-parties/abc")).isEqualTo(involvedPartyGet)
        }
    }

    @Nested
    inner class `Cardinality guards` {
        @Test
        fun `should ignore a trailing query string when matching the prefix`() {
            // What is tested: a query string on the endpoint's path.
            // Success criteria: the path with a query maps to the same template as without.
            // Why it matters: query values are as unbounded as path variables; a matcher that folded them into
            //   the tag would leak cardinality through the back door.
            //
            // Given: a query string past the matched prefix
            val tagger = WebClientMetrics.uriTagger(productAgreements)
            val uri = "/v8/agreements/products/lookup?productAgreementStatus=ACTV&fields=a,b"

            // When / Then
            assertThat(tagger(uri)).isEqualTo(productAgreements)
        }

        @Test
        fun `should collapse an unknown path into the catch-all bucket`() {
            // What is tested: a path the endpoint's template does not cover.
            // Success criteria: it maps to UNKNOWN.
            // Why it matters: an unforeseen path must never blow up the metric backend with a fresh series; one
            //   bounded bucket for everything unknown is what keeps the tag set finite.
            //
            // Given
            val tagger = WebClientMetrics.uriTagger(involvedPartyGet)

            // When / Then
            assertThat(tagger("/actuator/health")).isEqualTo(WebClientMetrics.UNKNOWN)
        }

        @Test
        fun `should collapse an empty path into the catch-all bucket`() {
            // What is tested: the empty URI.
            // Success criteria: it maps to UNKNOWN rather than matching the template's prefix.
            // Why it matters: an empty static prefix would match everything; this pins that a template claims no
            //   path it does not start.
            //
            // Given
            val tagger = WebClientMetrics.uriTagger(involvedPartyGet)

            // When / Then
            assertThat(tagger("")).isEqualTo(WebClientMetrics.UNKNOWN)
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
            val tagger = WebClientMetrics.uriTagger(productAgreements)

            // When / Then: a path that merely CONTINUES the text is a different endpoint -> catch-all
            assertThat(tagger("/v8/agreements/products/lookup-v2")).isEqualTo(WebClientMetrics.UNKNOWN)
            assertThat(tagger("/v8/agreements/products/lookupsomething")).isEqualTo(WebClientMetrics.UNKNOWN)

            // ... while the legitimate forms still match - exact, deeper segment, and with a query string
            assertThat(tagger("/v8/agreements/products/lookup")).isEqualTo(productAgreements)
            assertThat(tagger("/v8/agreements/products/lookup/42")).isEqualTo(productAgreements)
            assertThat(tagger("/v8/agreements/products/lookup?fields=a")).isEqualTo(productAgreements)
        }
    }
}
