package eu.inqudium.veredium.core.engine

import org.springframework.util.ClassUtils
import java.net.URI
import java.net.http.HttpRequest

/**
 * One selectable HTTP engine of a client stack - the stack-neutral face of `RestClientEngine` and `WebClientEngine`,
 * which the start-time checks of [EngineSelection] read.
 *
 * @property key The engine's value under `<prefix>.default` and `<prefix>.endpoints.<alias>`, and its name in the
 *   startup lines.
 * @property requiredClasses The classes that must be on the classpath for the engine to work.
 * @property dependency What to add when one of [requiredClasses] is missing, named in the start failure.
 */
interface HttpEngine {
    val key: String
    val requiredClasses: List<String>
    val dependency: String

    /** Whether every one of [requiredClasses] is on the classpath of [classLoader]. */
    fun isPresent(classLoader: ClassLoader? = HttpEngine::class.java.classLoader): Boolean = requiredClasses.all { ClassUtils.isPresent(it, classLoader) }
}

/**
 * The start-time checks of a stack's `<prefix>` engine selection, shared by both client stacks: a configuration the
 * factory cannot honour fails its construction - the context start - with an `IllegalArgumentException` naming the
 * key, because a wrong engine setting does not announce itself otherwise. A misspelt override key silently leaves its
 * backend on the default engine; a missing library fails with a `NoClassDefFoundError` at the first call.
 */
object EngineSelection {
    /** The JVM-wide property that lifts the JDK client's restriction on the Host header; never set by veredium. */
    const val JDK_ALLOW_RESTRICTED_HEADERS: String = "jdk.httpclient.allowRestrictedHeaders"

    /** The key a backend's engine is selected by: its `endpoints` entry when [overridden], the `default` otherwise. */
    fun keyFor(
        prefix: String,
        alias: String,
        overridden: Boolean,
    ): String = if (overridden) "$prefix.endpoints.$alias" else "$prefix.default"

    /**
     * Every override key in [overrides] must be a mesh alias of [aliases], the stack's own - typo protection a boolean
     * per-alias setting lacks. A key that names an endpoint another stack owns ([foreign], alias -> owning stack) is
     * refused too, naming that stack: this stack's engine would never carry it.
     */
    fun requireKnownAliases(
        prefix: String,
        overrides: Collection<String>,
        aliases: Collection<String>,
        foreign: Map<String, String> = emptyMap(),
    ) {
        val (owned, unknown) = overrides.filterNot { it in aliases }.sorted().partition { it in foreign }
        val problems =
            owned.map { "$prefix.endpoints.$it names a mesh endpoint the ${foreign.getValue(it)} stack owns, not this one" } +
                if (unknown.isEmpty()) {
                    emptyList()
                } else {
                    listOf(
                        "${unknown.joinToString { "$prefix.endpoints.$it" }} names no mesh endpoint - the key must be the alias " +
                            "of an app.mesh.endpoints[] entry. Known aliases: ${aliases.sorted()}",
                    )
                }
        require(problems.isEmpty()) { problems.joinToString("; ") }
    }

    /**
     * Every engine of [engineByBackend] must be [present]; the failure names, per engine, the keys selecting it, its
     * backends and the [HttpEngine.dependency] to add. [overrides] are the aliases with an `endpoints` entry.
     */
    fun <E : HttpEngine> requireLibraries(
        prefix: String,
        engineByBackend: Map<String, E>,
        overrides: Collection<String>,
        present: (E) -> Boolean,
    ) {
        val missing =
            engineByBackend.entries
                .groupBy({ it.value }, { it.key })
                .filterKeys { !present(it) }
                .toSortedMap(compareBy { it.key })
        require(missing.isEmpty()) {
            missing.entries.joinToString("; ") { (engine, aliases) ->
                val keys = aliases.map { keyFor(prefix, it, it in overrides) }.distinct().sorted()
                "${keys.joinToString()} selects the HTTP engine '${engine.key}' for ${aliases.sorted()}, but its library " +
                    "is not on the classpath (${engine.requiredClasses.joinToString(" and ")}). Add ${engine.dependency}, " +
                    "or select another engine."
            }
        }
    }

    /**
     * Whether this JVM's `java.net.http` lets a request carry an explicit `Host` header - asked of the JDK itself rather
     * than read off [JDK_ALLOW_RESTRICTED_HEADERS], because the JDK reads the property once, when its HTTP client classes
     * initialise: a value set later is visible in `System.getProperty` and ignored by the client.
     */
    fun jdkAllowsHostHeader(): Boolean =
        runCatching {
            HttpRequest
                .newBuilder(URI.create("http://localhost/"))
                .header("Host", "probe")
        }.isSuccess
}
