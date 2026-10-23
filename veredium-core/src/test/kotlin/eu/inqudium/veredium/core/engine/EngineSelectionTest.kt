package eu.inqudium.veredium.core.engine

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * The start rules both client stacks apply to their engine selection ([EngineSelection]), once for both: an override
 * key that names no mesh endpoint, and an engine whose library is missing. Each stack's own tests show the rules wired
 * into its factory; these pin the rules and their messages.
 */
class EngineSelectionTest {
    @Test
    fun `should name the key a backend's engine is selected by`() {
        // What is tested: keyFor for a backend with an override and one without.
        // Success criteria: the override's key is `<prefix>.endpoints.<alias>`, the other `<prefix>.default`.
        // Why it matters: every start failure names the key to edit; naming the default for a backend that has an
        //   override of its own would send the operator to the wrong line.
        //
        // Given / When / Then
        assertThat(EngineSelection.keyFor(PREFIX, "orders", overridden = true)).isEqualTo("$PREFIX.endpoints.orders")
        assertThat(EngineSelection.keyFor(PREFIX, "orders", overridden = false)).isEqualTo("$PREFIX.default")
    }

    @Test
    fun `should refuse an override key that names no mesh endpoint, listing the known aliases`() {
        // What is tested: requireKnownAliases with one misspelt override next to a correct one.
        // Success criteria: an IllegalArgumentException naming the misspelt key, and the known aliases sorted; the
        //   correct override alone passes.
        // Why it matters: a misspelt key leaves the intended backend on the default engine without a word - the
        //   failure is the only place that says so, on both stacks alike.
        //
        // Given
        val aliases = listOf("payments", "orders")

        // When / Then
        assertThatThrownBy { EngineSelection.requireKnownAliases(PREFIX, listOf("orders", "oders"), aliases) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContainingAll("$PREFIX.endpoints.oders names no mesh endpoint", "Known aliases: [orders, payments]")
        assertThatCode { EngineSelection.requireKnownAliases(PREFIX, listOf("orders"), aliases) }.doesNotThrowAnyException()
    }

    @Test
    fun `should refuse an engine whose library is missing, naming its keys, backends and dependency`() {
        // What is tested: requireLibraries with two backends on an engine whose library is absent - one through the
        //   default, one through an override - and one backend on a present engine.
        // Success criteria: an IllegalArgumentException naming both keys, both backends, the missing class and the
        //   dependency to add; the present engine is not mentioned.
        // Why it matters: without the check a missing library is a NoClassDefFoundError at the first call, naming
        //   neither the key nor the artifact.
        //
        // Given
        val selected = mapOf("orders" to TestEngine.MISSING, "payments" to TestEngine.MISSING, "stock" to TestEngine.PRESENT)

        // When / Then
        assertThatThrownBy { EngineSelection.requireLibraries(PREFIX, selected, overrides = listOf("payments")) { it == TestEngine.PRESENT } }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContainingAll(
                "$PREFIX.default, $PREFIX.endpoints.payments selects the HTTP engine 'missing' for [orders, payments]",
                "com.example.Missing",
                "Add com.example:missing",
            ).hasMessageNotContaining("'present'")
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private enum class TestEngine(
        override val key: String,
        override val requiredClasses: List<String>,
        override val dependency: String,
    ) : HttpEngine {
        PRESENT("present", listOf("java.lang.String"), "nothing"),
        MISSING("missing", listOf("com.example.Missing"), "com.example:missing"),
    }

    private companion object {
        const val PREFIX = "app.test.engine"
    }
}
