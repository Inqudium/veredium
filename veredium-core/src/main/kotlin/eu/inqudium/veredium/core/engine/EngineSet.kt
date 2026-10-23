package eu.inqudium.veredium.core.engine

import org.slf4j.Logger
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * The resources of one HTTP engine as a client factory owns them - its pool, client or threads, shared by every backend
 * on that engine - reduced to what their lifecycle needs. Each client stack's engine transport implements it.
 */
interface EngineResources {
    /** The engine's key, as [HttpEngine.key] - named in the lines about its lifecycle. */
    val key: String

    /** Whether the resources are released - read off the engine itself, not a flag. */
    val isClosed: Boolean

    /**
     * Releases the resources, waiting for in-flight exchanges no later than [deadlineNanos] on [nanoTime]'s clock - a
     * deadline SHARED by every engine of the factory, so that releasing several engines one after another takes the drain
     * bound once, not once per engine. An expiry is an outcome the implementation reports itself ([EngineDrain]), never an
     * exception: this runs from a destroy callback.
     */
    fun shutdown(
        deadlineNanos: Long,
        nanoTime: () -> Long,
    )
}

/**
 * The engines one client factory uses, with the lifecycle both client stacks give them: created together when the factory
 * is, used only while it is open, released together against one deadline.
 *
 * - **Created at construction, one per engine in [engines]**, each by [create] - so a misconfigured engine fails the
 *   context start, not the first call. An engine nobody selected is never created and needs no library on the classpath.
 *   When a creation fails, the engines created before it are released at once (nothing can be in flight on them yet) and
 *   the failure is rethrown: it fails the factory's construction, so no destroy callback would ever release them, and a
 *   started Jetty client's non-daemon threads would keep the JVM of a failed start alive.
 * - **Used under a lock** ([use]), so a client built while the factory shuts down cannot hand out an engine after
 *   [shutdown] closed it. A lock rather than `synchronized`, because a client may be built on a virtual thread.
 * - **Released together** ([shutdown]): each engine on its own against the one deadline - a release that throws is
 *   reported on [logger] and does not keep the engines after it open. Idempotent.
 *
 * @param owner The factory's name, for the refusal after [shutdown].
 */
class EngineSet<E : HttpEngine, T : EngineResources>(
    engines: Collection<E>,
    private val owner: String,
    private val logger: Logger,
    create: (E) -> T,
) {
    /** Every engine in use, in the order [engines] named them. */
    val all: Map<E, T> = createAll(engines, create)

    private val lifecycle = ReentrantLock()

    /** Set by [shutdown]; afterwards [use] refuses instead of handing out a released engine. */
    private var closed = false

    /**
     * Runs [block] on the resources of [engine] for the client of [backend], unless the factory was shut down.
     *
     * @throws IllegalStateException after [shutdown], or for an engine that is not in use - a programming error, because
     *   the factory resolves every backend's engine at construction.
     */
    fun <R> use(
        engine: E,
        backend: String,
        block: (T) -> R,
    ): R =
        lifecycle.withLock {
            check(!closed) { "$owner was shut down; the client of backend '$backend' would run on a released HTTP engine" }
            block(all[engine] ?: error("HTTP engine '${engine.key}' was not selected for any backend of $owner"))
        }

    /** Releases every engine against the one shared [deadlineNanos] on [nanoTime]'s clock (class KDoc). */
    fun shutdown(
        deadlineNanos: Long,
        nanoTime: () -> Long,
    ) {
        lifecycle.withLock { closed = true }
        release(all.values, deadlineNanos, nanoTime)
    }

    private fun createAll(
        engines: Collection<E>,
        create: (E) -> T,
    ): Map<E, T> {
        val created = LinkedHashMap<E, T>()
        try {
            engines.forEach { engine -> created[engine] = create(engine) }
        } catch (e: Throwable) {
            val now = System.nanoTime()
            release(created.values, now) { now }
            throw e
        }
        return created
    }

    private fun release(
        resources: Collection<T>,
        deadlineNanos: Long,
        nanoTime: () -> Long,
    ) {
        resources.forEach { engine ->
            try {
                engine.shutdown(deadlineNanos, nanoTime)
            } catch (e: Exception) {
                logger.warn("HTTP engine '{}' failed to release its resources", engine.key, e)
            }
        }
    }
}
