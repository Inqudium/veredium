package eu.inqudium.veredium.core.validation

import jakarta.validation.MessageInterpolator
import jakarta.validation.Validator
import jakarta.validation.ValidatorFactory
import java.util.Locale

/**
 * Governs whether a backend's deserialised response DTO is checked against its Bean Validation constraints -
 * the `validation` block of both client stacks' properties (`app.restclient.validation`,
 * `app.webclient.validation`), one shape, bound by each.
 *
 * A shared [enabled] default plus per-backend overrides in [endpoints] (keyed by the backend ALIAS, i.e. the
 * mesh-derived R4j instance name): a per-alias entry wins in BOTH directions - it can switch a single backend
 * on above an off default, or off above an on default. This granularity is load-bearing, not cosmetic: the
 * constraints are generated from published OpenAPI specs, and those specs are demonstrably stricter than
 * reality in places, differently per endpoint. Validation is therefore adopted one backend at a time, against
 * real traffic - which is exactly what the per-alias override is for.
 *
 * The check itself lives OUTSIDE the Resilience4j decoration in both clients: a response that violates its
 * own schema is a contract problem, not an outage, and must not open the circuit breaker.
 *
 * @property enabled Default `false`: the constraints promise a guarantee the specs do not keep everywhere, so
 *   the safe default is off and each backend opts in deliberately.
 * @property endpoints Per-backend-alias overrides; an alias not present inherits [enabled].
 * @property failOnViolation Whether a detected violation FAILS the call (a
 *   [jakarta.validation.ConstraintViolationException]) or is only REPORTED (logged at WARN, the response
 *   returned unchanged). Default `true` (enforce). SHARED only, deliberately not per-endpoint: report-only is a
 *   rollout stance - turn validation on for a backend and watch the WARN lines against real traffic before
 *   enforcing - and that stance applies to the whole adopting wave, not to one backend at a time. The per-alias
 *   granularity that matters is WHICH backends validate ([endpoints]); WHETHER a violation is fatal is a
 *   single posture.
 */
data class ResponseValidationProperties(
    val enabled: Boolean = false,
    val endpoints: Map<String, Boolean> = emptyMap(),
    val failOnViolation: Boolean = true,
) {
    /** The effective toggle for [alias]: its own override when set, otherwise the shared [enabled] default. */
    fun isEnabledFor(alias: String): Boolean = endpoints[alias] ?: enabled
}

/**
 * The Bean Validation side of a client's response handling, resolved once from the host's [ValidatorFactory]
 * and the stack's [ResponseValidationProperties]: which backends validate, whether a violation fails the call,
 * and the one [Validator] every client of the stack shares.
 *
 * The FACTORY rather than a ready-made Validator, because a Validator's message interpolator is fixed at
 * construction while `usingContext()` is the standard way to derive one that differs - here, one pinned to
 * English. Deriving keeps that choice inside the client: the host's own Validator bean, which its controllers
 * use, is left exactly as it configured it. A `null` factory - no Bean Validation provider on the classpath -
 * means validation cannot run for ANY backend, whatever the properties say; [providerVerdict] says so for the
 * startup line.
 */
class ResponseValidation(
    private val validation: ResponseValidationProperties,
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

    /** The shared default, for the startup line. */
    val enabledByDefault: Boolean get() = validation.enabled

    /** Whether a detected violation FAILS the call or is only REPORTED - one posture for the whole app. */
    val failOnViolation: Boolean get() = validation.failOnViolation

    /** Whether [alias] validates at all: its own override when set, otherwise the shared default. */
    fun isEnabledFor(alias: String): Boolean = validation.isEnabledFor(alias)

    /** The validator for [alias], or `null` when validation is off for it - so the per-call path carries no toggle. */
    fun validatorFor(alias: String): Validator? = validator.takeIf { isEnabledFor(alias) }

    /** For the startup line: whether a provider is there at all. */
    val providerVerdict: String =
        if (validator != null) "present, messages pinned to English" else "MISSING - validation cannot run for any backend"

    /**
     * The aliases of [aliases] that validate, sorted, or `none` - the startup line's second argument. Worth a
     * line because both ways validation silently does nothing are invisible at runtime: a backend simply not
     * listed, or no Bean Validation provider on the classpath at all, in which case no configuration can switch
     * it on for anyone.
     */
    fun validatedAliases(aliases: Collection<String>): List<String> = aliases.filter(::isEnabledFor).sorted().ifEmpty { listOf("none") }
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
