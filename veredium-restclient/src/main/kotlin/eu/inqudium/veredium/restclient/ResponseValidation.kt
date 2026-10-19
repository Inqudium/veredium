package eu.inqudium.veredium.restclient

import jakarta.validation.MessageInterpolator
import jakarta.validation.Validator
import jakarta.validation.ValidatorFactory
import java.util.Locale

/**
 * The Bean Validation side of a [ResilientRestClient]'s response handling, resolved once from the host's
 * [ValidatorFactory] and the `app.restclient.validation` properties: which backends validate, whether a violation
 * fails the call, and the one [Validator] every client shares.
 *
 * The FACTORY rather than a ready-made Validator, because a Validator's message interpolator is fixed at
 * construction while `usingContext()` is the standard way to derive one that differs - here, one pinned to
 * English. Deriving keeps that choice inside this module: the host's own Validator bean, which its controllers
 * use, is left exactly as it configured it. A `null` factory - no Bean Validation provider on the classpath -
 * means validation cannot run for ANY backend, whatever the properties say; [providerVerdict] says so for the
 * startup line.
 */
class ResponseValidation(
    private val validation: RestClientProperties.Validation,
    validatorFactory: ValidatorFactory?,
) {
    /**
     * The validator the clients use: the host's, but with its messages pinned to [Locale.ENGLISH].
     *
     * Constraint messages are interpolated against `Locale.getDefault()`, so the text of a violation otherwise
     * depends on the locale of the machine the process happens to run on - the same defect would read
     * "size must be between 1 and ..." on one host and "Groesse muss zwischen 1 und ... sein" on the next.
     * That belongs nowhere near a log line: this codebase is English throughout, and log consumers, alert
     * rules and anyone searching for a message have to be able to rely on its wording.
     */
    val validator: Validator? =
        validatorFactory?.let { factory ->
            factory
                .usingContext()
                .messageInterpolator(EnglishMessageInterpolator(factory.messageInterpolator))
                .validator
        }

    /** Whether a detected violation FAILS the call or is only REPORTED - one posture for the whole app. */
    val failOnViolation: Boolean get() = validation.failOnViolation

    /** Whether [alias] validates at all: its own override when set, otherwise the shared default. */
    fun isEnabledFor(alias: String): Boolean = validation.isEnabledFor(alias)

    /** The validator for [alias], or `null` when validation is off for it - so the per-call path carries no toggle. */
    fun validatorFor(alias: String): Validator? = validator.takeIf { isEnabledFor(alias) }

    /** For the startup line: whether a provider is there at all. */
    val providerVerdict: String =
        if (validator != null) "present, messages pinned to English" else "MISSING - validation cannot run for any backend"
}

/**
 * A [MessageInterpolator] that renders every constraint message in English, whatever locale is asked for or
 * ambient.
 *
 * Bean Validation interpolates against `Locale.getDefault()` unless told otherwise, which makes the text of a
 * violation a property of the host the process runs on rather than of the defect. These messages end up in log
 * lines and exception texts that people search and alert on, so they have to read the same everywhere.
 *
 * The locale-taking overload deliberately ignores its argument too. It is not called on this path - the engine
 * uses the other one - so honouring it would achieve nothing except leaving a way for the guarantee to be
 * bypassed later.
 */
private class EnglishMessageInterpolator(
    private val delegate: MessageInterpolator,
) : MessageInterpolator {
    override fun interpolate(
        messageTemplate: String,
        context: MessageInterpolator.Context,
    ): String = delegate.interpolate(messageTemplate, context, Locale.ENGLISH)

    override fun interpolate(
        messageTemplate: String,
        context: MessageInterpolator.Context,
        locale: Locale,
    ): String = delegate.interpolate(messageTemplate, context, Locale.ENGLISH)
}
