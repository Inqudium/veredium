package eu.inqudium.veredium.core

/**
 * The library's identity as consumers and peers see it: its name, its version and the `User-Agent`
 * product token built from both.
 */
object Veredium {
    /** The project name, also the Maven artifact prefix and the root of every logger name. */
    const val NAME: String = "veredium"

    /**
     * The implementation version from the jar manifest (`Implementation-Version`), or `null` when the
     * classes are not loaded from a jar - in the IDE and in this module's own tests.
     */
    val version: String?
        get() = Veredium::class.java.`package`?.implementationVersion

    /**
     * The `User-Agent` product token the client identifies itself with: `veredium/<version>`, or the
     * bare [NAME] when the [version] is unknown - a token with an empty version (`veredium/`) would be
     * malformed by RFC 9110 section 10.1.5.
     */
    fun userAgent(): String = userAgent(version)

    internal fun userAgent(version: String?): String = if (version.isNullOrBlank()) NAME else "$NAME/$version"

    /**
     * The `User-Agent` a client sends: [configured] when the host set one, else [userAgent]. A host overrides
     * the library's token to name ITSELF to its peers - `my-service/2.3`, say - where an access log is read per
     * caller rather than per library; the clients bind it under `app.restclient.user-agent` /
     * `app.webclient.user-agent`.
     *
     * Validated at bind time, naming [key]: a blank value or one with a control character (CR, LF, a tab) is
     * not a header value and would otherwise reach the transport, which rejects it per request with a message
     * that names neither the key nor the file. Surrounding whitespace is trimmed.
     *
     * @throws IllegalArgumentException when [configured] is blank or carries a control character.
     */
    fun resolveUserAgent(
        configured: String?,
        key: String,
    ): String {
        if (configured == null) return userAgent()
        val token = configured.trim()
        require(token.isNotEmpty() && token.none { it < ' ' || it == '\u007f' }) {
            "$key must be a non-blank header value without control characters, but was '$configured'"
        }
        return token
    }
}
