package eu.inqudium.veredium.webclient.resilience

import org.slf4j.Logger
import org.slf4j.event.Level
import org.slf4j.spi.LoggingEventBuilder

/**
 * The structured log vocabulary of the amendment applier - what remains of a larger `resilience_*` schema.
 *
 * **This file used to define the fields for three resilience LOGGERS** (circuit-breaker transitions, bulkhead
 * saturation, time-limiter timeouts). Those loggers are gone: the `resilience-observation` module supersedes
 * them, and keeping a second producer writing into the same `resilience_*` namespace would have meant two
 * components disagreeing about what a field means - `resilience_failure_type` held a simple class name here
 * and a fully-qualified one there, and `resilience_limit` carried a `Duration`, an `Int` and a formatted
 * percentage depending on which layer wrote it, so one field arrived at the index as three JSON types.
 *
 * What survives is only what [ResilienceAmendmentApplier] needs, because an amendment failure is a
 * CONFIGURATION event rather than a resilience event: it reports that a computed change did not reach the
 * instance the registry serves. It stays in the shared namespace so that one query over [FIELD_LAYER] still
 * covers behaviour changes and their failures alike.
 *
 * The three envelope fields keep their names deliberately - they are the same fields `resilience-observation`
 * writes, with the same meanings, so a dashboard filtering by layer or instance sees both producers as one
 * series. Everything that did NOT mean the same in both schemas went with the loggers instead of being carried
 * forward.
 *
 * The naming follows what this application already does: structured key-values through SLF4J's fluent API,
 * `lower_snake_case`, and a prefix, so a bare `instance` or `event` cannot collide with a same-named field
 * from another producer in a shared index. `traceId` / `spanId` are deliberately absent - the encoder emits
 * them from the MDC; legatium's exchange event follows the same rule (the ids ride the MDC, never the
 * key-values), and duplicating them as key-values would put two spellings of one id into one document.
 */
internal const val FIELD_LAYER = "resilience_layer"

/** The instance name - identical to the `name` tag on that instance's Resilience4j meters. */
internal const val FIELD_INSTANCE = "resilience_instance"

/**
 * What kind of line this is. A closed vocabulary means an alert or a saved search keys on a term rather than
 * on message text, which is prose and gets improved.
 */
internal const val FIELD_EVENT = "resilience_event"

// The layer spellings match Resilience4j's own (artifacts, packages and meter names), which is why they are
// squashed rather than snake_case. LAYER_BULKHEAD went with the bulkhead logger - amendments only ever target
// these two.
internal const val LAYER_CIRCUIT_BREAKER = "circuitbreaker"
internal const val LAYER_TIME_LIMITER = "timelimiter"

/**
 * A config amendment that was computed and did not land on the instance the registry serves - a mechanism that
 * failed.
 */
internal const val EVENT_AMENDMENT_NOT_APPLIED = "amendment_not_applied"

/**
 * An amendment aimed at a name the deployment does not have. A separate value from
 * [EVENT_AMENDMENT_NOT_APPLIED] because the fix differs: this one is a misspelling or an endpoint that is not
 * configured here, not a mechanism that failed.
 */
internal const val EVENT_AMENDMENT_UNKNOWN_ALIAS = "amendment_unknown_alias"

/**
 * Opens a log event already carrying the three fields every line in this vocabulary must have. The caller adds
 * the message and its arguments and calls `log()`.
 */
internal fun Logger.resilienceEvent(
    level: Level,
    layer: String,
    instance: String,
    event: String,
): LoggingEventBuilder =
    atLevel(level)
        .addKeyValue(FIELD_LAYER, layer)
        .addKeyValue(FIELD_INSTANCE, instance)
        .addKeyValue(FIELD_EVENT, event)
