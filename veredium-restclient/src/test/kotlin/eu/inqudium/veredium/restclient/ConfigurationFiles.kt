package eu.inqudium.veredium.restclient

import org.springframework.boot.context.properties.bind.Bindable
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.context.properties.source.ConfigurationPropertySources
import org.springframework.boot.env.YamlPropertySourceLoader
import org.springframework.core.io.ClassPathResource

/**
 * The two configuration files the contract tests bind, and the one rule that says which test binds which.
 *
 * [SHIPPED] is `src/main/resources/application.yml`, the file the artefact runs on. [FIXTURE] is the test-owned
 * `src/test/resources/application-test.yml`, a copy an operational edit never reaches.
 *
 * An IDENTITY or an INVARIANT - the alias set, the instance keys, the logger name the level line must address,
 * the bulkhead sum against the pool - is pinned in BOTH files ([BOTH]): nobody legitimately changes those by
 * hand, and a drift in the shipped file has to fail the build, which a test bound to the fixture alone cannot
 * see. A VALUE an operator may tune - a level, a body mode, a threshold - is pinned in the [FIXTURE] only, so
 * an operational edit to the shipped file cannot break the suite.
 *
 * Binding goes through `YamlPropertySourceLoader` + `Binder`, the way Spring Boot reads a config file at
 * startup: no context, no I/O beyond the classpath.
 */
object ConfigurationFiles {
    const val SHIPPED = "application.yml"
    const val FIXTURE = "application-test.yml"

    /** For a `@ValueSource`: the two files an identity or invariant is pinned in. */
    val BOTH = listOf(SHIPPED, FIXTURE)

    /**
     * Bind [prefix] of [file] into [type].
     *
     * `T : Any` is required, not decorative: with the implicit `Any?` bound, none of Binder's Java overloads
     * match and the call does not compile.
     */
    fun <T : Any> bind(
        file: String,
        prefix: String,
        type: Class<T>,
    ): T =
        binder(file)
            .bind(prefix, type)
            .orElseThrow { AssertionError("$prefix did not bind from $file") }

    /** Bind [prefix] of [file] flattened to a string map, for blocks that have no properties class here. */
    fun bindMap(
        file: String,
        prefix: String,
    ): Map<String, String> =
        binder(file)
            .bind(prefix, Bindable.mapOf(String::class.java, String::class.java))
            .orElseThrow { AssertionError("$prefix did not bind from $file") }

    private fun binder(file: String): Binder {
        val sources = YamlPropertySourceLoader().load(file, ClassPathResource(file))
        return Binder(ConfigurationPropertySources.from(sources))
    }
}
