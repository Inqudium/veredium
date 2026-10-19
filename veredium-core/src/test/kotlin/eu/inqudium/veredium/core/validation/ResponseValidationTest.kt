package eu.inqudium.veredium.core.validation

import jakarta.validation.Validation
import jakarta.validation.ValidatorFactory
import jakarta.validation.constraints.Size
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.Locale

/**
 * Tests for the response-validation side both client stacks share: the properties' toggle semantics and the
 * one validator every client derives from the host's factory - against a real provider (Hibernate Validator,
 * test scope), because the English pin is a property of the interpolation and nothing else proves it.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ResponseValidationTest {
    private lateinit var validatorFactory: ValidatorFactory

    @BeforeAll
    fun openFactory() {
        validatorFactory = Validation.buildDefaultValidatorFactory()
    }

    @AfterAll
    fun closeFactory() {
        validatorFactory.close()
    }

    @Nested
    inner class Properties {
        @Test
        fun `should default to off with no per-endpoint overrides`() {
            // What/why: the shipped default. The DTO constraints come from published OpenAPI specs, which are
            //   stricter than reality in places, so validation must be off until a backend opts in - the
            //   guarantee the whole feature rests on.
            // Given / When / Then
            val validation = ResponseValidationProperties()
            assertThat(validation.enabled).isFalse()
            assertThat(validation.endpoints).isEmpty()
            assertThat(validation.isEnabledFor("any-backend")).isFalse()
            // Enforcing is the default: once a backend opts in, a violation fails the call rather than passing
            // silently. Report-only (false) is the deliberate rollout exception, not the shipped stance.
            assertThat(validation.failOnViolation).isTrue()
        }

        @Test
        fun `should let a per-alias override win over the shared default in both directions`() {
            // What is tested: that a per-backend entry beats the global default whichever way they disagree -
            //   not just to enable a backend above an off default, but also to disable one above an on default.
            // How success is judged: with the default ON, an alias mapped to false resolves to false while an
            //   alias with no entry inherits true; with the default OFF, an alias mapped to true resolves to
            //   true while an unmapped one inherits false. The override is the only variable in each pair.
            // Why it matters: the specs are wrong in different ways per endpoint, so validation has to be
            //   adoptable one backend at a time AND revocable for one backend without giving it up everywhere.
            //   An all-or-nothing switch would be unusable here.
            //
            // Given: default ON, one backend explicitly opted OUT
            val defaultOn = ResponseValidationProperties(enabled = true, endpoints = mapOf("opted-out" to false))
            // Then
            assertThat(defaultOn.isEnabledFor("opted-out")).isFalse()
            assertThat(defaultOn.isEnabledFor("inherits")).isTrue()

            // Given: default OFF, one backend explicitly opted IN
            val defaultOff = ResponseValidationProperties(enabled = false, endpoints = mapOf("opted-in" to true))
            // Then
            assertThat(defaultOff.isEnabledFor("opted-in")).isTrue()
            assertThat(defaultOff.isEnabledFor("inherits")).isFalse()
        }
    }

    @Nested
    inner class Resolution {
        @Test
        fun `should hand out the validator only for an alias that validates`() {
            // What is tested: the per-backend resolution a client factory makes ONCE - a validator for an alias
            //   that is on, null for one that is off - so the per-call path carries no toggle.
            // Given: default off, one backend opted in
            val validation = ResponseValidation(ResponseValidationProperties(endpoints = mapOf("opted-in" to true)), validatorFactory)

            // When / Then
            assertThat(validation.validatorFor("opted-in")).isNotNull()
            assertThat(validation.validatorFor("other")).isNull()
            assertThat(validation.validatedAliases(listOf("other", "opted-in"))).containsExactly("opted-in")
            assertThat(validation.providerVerdict).isEqualTo("present, messages pinned to English")
        }

        @Test
        fun `should report a missing provider and validate for nobody, whatever the properties say`() {
            // Why it matters: no Bean Validation provider on the classpath is the trap a startup line exists for -
            // every backend is "on" in the properties and none validates, and nothing else would say so.
            //
            // Given: validation on for everyone, no provider
            val validation = ResponseValidation(ResponseValidationProperties(enabled = true), validatorFactory = null)

            // When / Then
            assertThat(validation.validator).isNull()
            assertThat(validation.validatorFor("any")).isNull()
            assertThat(validation.isEnabledFor("any")).`as`("the properties still say on - the provider is what is missing").isTrue()
            assertThat(validation.providerVerdict).startsWith("MISSING")
            assertThat(validation.validatedAliases(emptyList())).containsExactly("none")
        }

        @Test
        fun `should render constraint messages in English whatever the default locale is`() {
            // What is tested: the interpolator pin - the one reason the validator is derived from the host's
            //   factory rather than taken as it is.
            // How success is judged: with the JVM's default locale set to German for the duration of the check,
            //   the violation message still reads in English, while the host's own validator would read German.
            // Why it matters: these messages end up in log lines people search and alert on; a text that depends
            //   on the locale of the machine the process happens to run on belongs nowhere near them.
            //
            // Given: a constrained value and a German default locale
            val validation = ResponseValidation(ResponseValidationProperties(enabled = true), validatorFactory)
            val previous = Locale.getDefault()
            Locale.setDefault(Locale.GERMAN)
            try {
                // When
                val pinned =
                    validation.validator!!
                        .validate(TooLong())
                        .single()
                        .message
                val hosts =
                    validatorFactory.validator
                        .validate(TooLong())
                        .single()
                        .message

                // Then
                assertThat(pinned).isEqualTo("size must be between 0 and 3")
                assertThat(hosts).`as`("the host's own validator follows the default locale - which is the point").isNotEqualTo(pinned)
            } finally {
                Locale.setDefault(previous)
            }
        }
    }

    /** A value with one constraint it violates. */
    private class TooLong {
        @field:Size(max = 3)
        @Suppress("unused")
        val value: String = "too long"
    }
}
