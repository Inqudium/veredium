package eu.inqudium.veredium.core.mesh

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener

/**
 * Logs the fully derived per-endpoint mesh timeout ladder once, on application startup.
 *
 * The ladder is otherwise implicit: only the MEASURED anchor (`individual-request-timeout-ms`) is maintained
 * in config; every other rung - global / responseTimeout / slowCall / timeLimiter - is DERIVED by
 * [MeshProperties.resolveAll]. This startup line is the app-side inspectability guarantee, so an operator can
 * read the effective budgets straight from the log instead of recomputing them. [ladderLines] is pure (no
 * logging) so it can be asserted directly.
 *
 * A bean of [MeshResilienceConfiguration], not a scanned component: the mesh wiring is imported by each client stack's
 * configuration, so a host that scans only its stack's package still gets this line.
 */
class MeshLadderLogger(
    private val props: MeshProperties,
) {
    private val logger: Logger = LoggerFactory.getLogger(MeshLadderLogger::class.java)

    /** Writes [ladderLines] at INFO once the application is ready - after every binding the ladder reads. */
    @EventListener(ApplicationReadyEvent::class)
    fun logResolvedLadder() = ladderLines().forEach(logger::info)

    /** One line per endpoint (sorted by alias) - the fully derived ladder, for the startup log and tests. */
    fun ladderLines(): List<String> =
        props
            .resolveAll()
            .values
            .sortedBy { it.alias }
            .map { endpoint ->
                "mesh ladder [${endpoint.alias}] ${endpoint.triplet}: " +
                    "individual=${endpoint.individualRequestTimeoutMs}ms global=${endpoint.sidecarGlobalMs}ms " +
                    "responseTimeout=${endpoint.responseTimeout.toMillis()}ms " +
                    "slowCall=${endpoint.slowCallDuration.toMillis()}ms timeLimiter=${endpoint.timeLimiterTimeout.toMillis()}ms"
            }
}
