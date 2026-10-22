package eu.inqudium.veredium.core.mesh

/**
 * One connection pool that mesh backends share, as a client module built it: what the bulkheads of [aliases] are
 * sized against ([eu.inqudium.veredium.core.resilience.MeshResilienceStartupGuard], "What it warns about").
 *
 * @property name The pool's name in the startup line, e.g. the engine it belongs to.
 * @property maxConnections The pool's connection bound, or `null` when the engine has none (the JDK client).
 * @property aliases The mesh aliases whose exchanges lease from this pool.
 */
data class MeshConnectionPool(
    val name: String,
    val maxConnections: Int?,
    val aliases: Set<String>,
)

/**
 * The connection pools a client module holds for the mesh backends - implemented by its client factory, which is
 * the one place that knows which backend runs on which engine. The startup guard reads every bean of this type;
 * without one it falls back to the single pool `app.mesh.defaults.transport.max-connections` describes.
 */
fun interface MeshConnectionPools {
    fun connectionPools(): List<MeshConnectionPool>
}
