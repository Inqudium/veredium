package eu.inqudium.veredium.core

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.NullAndEmptySource
import org.junit.jupiter.params.provider.ValueSource

/**
 * Pins the identity the client presents to its peers, and with it the test conventions the generated
 * test-evidence page relies on (the rationale block, the Given/When/Then stages).
 */
class VerediumTest {
    @Test
    fun `should name itself veredium`() {
        // What is tested: the NAME constant every logger name and artifact id derives from.
        // Success criteria: the constant equals the lowercase project name.
        // Why it matters: the name is part of the operator-facing contract (logger names, artifact
        //   ids, the User-Agent token); a drift would rename what dashboards and log filters key on.
        // Given/When/Then
        assertThat(Veredium.NAME).isEqualTo("veredium")
    }

    @Test
    fun `should build the User-Agent token from the name and the version`() {
        // What is tested: the product token for a known version.
        // Success criteria: `veredium/<version>`, name and version joined by a single slash.
        // Why it matters: the token is what a peer's access log shows for this client; the
        //   `product/version` shape is the one RFC 9110 defines, and operators grep for it.
        // Given/When/Then
        assertThat(Veredium.userAgent("1.2.3")).isEqualTo("veredium/1.2.3")
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = ["  "])
    fun `should fall back to the bare name when the version is unknown`(version: String?) {
        // What is tested: the product token when no version is known - null (no manifest), empty
        //   and blank.
        // Success criteria: the bare name, without a trailing slash.
        // Why it matters: `veredium/` with nothing after the slash is not a valid product token;
        //   a peer that validates the header would reject the call for a cosmetic reason.
        // Given/When/Then
        assertThat(Veredium.userAgent(version)).isEqualTo("veredium")
    }

    @Test
    fun `should report no version and the bare token when not loaded from a jar`() {
        // What is tested: the manifest-backed version property, read from the classes directory
        //   this test runs against.
        // Success criteria: the version is null and the token is the bare name - the fallback is
        //   what every IDE run and this suite see.
        // Why it matters: the property must not throw outside a jar, where there is no manifest,
        //   and the public token must agree with the internal rule it delegates to.
        // Given/When/Then
        assertThat(Veredium.version).isNull()
        assertThat(Veredium.userAgent()).isEqualTo("veredium")
    }
}
