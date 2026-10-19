package eu.inqudium.veredium.core

/**
 * The library's identity as consumers and peers see it: its name, its version and the `User-Agent`
 * product token built from both.
 */
public object Veredium {
    /** The project name, also the Maven artifact prefix and the root of every logger name. */
    public const val NAME: String = "veredium"

    /**
     * The implementation version from the jar manifest (`Implementation-Version`), or `null` when the
     * classes are not loaded from a jar - in the IDE and in this module's own tests.
     */
    public val version: String?
        get() = Veredium::class.java.`package`?.implementationVersion

    /**
     * The `User-Agent` product token the client identifies itself with: `veredium/<version>`, or the
     * bare [NAME] when the [version] is unknown - a token with an empty version (`veredium/`) would be
     * malformed by RFC 9110 section 10.1.5.
     */
    public fun userAgent(): String = userAgent(version)

    internal fun userAgent(version: String?): String = if (version.isNullOrBlank()) NAME else "$NAME/$version"
}
