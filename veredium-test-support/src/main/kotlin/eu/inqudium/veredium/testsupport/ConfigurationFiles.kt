package eu.inqudium.veredium.testsupport

import org.assertj.core.api.Assertions.assertThat
import org.springframework.boot.context.properties.bind.Bindable
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.context.properties.source.ConfigurationPropertySources
import org.springframework.boot.env.YamlPropertySourceLoader
import org.springframework.core.env.EnumerablePropertySource
import org.springframework.core.io.ClassPathResource

/**
 * The two configuration files a client module's contract tests bind, and the one rule that says which test binds
 * which.
 *
 * [REFERENCE] is the module's `src/test/resources/application.yml`, its REFERENCE configuration: the documented,
 * worked example of how a host configures the client - every key with its reasoning, the shape the docs quote. It is
 * a TEST resource. The client modules are libraries and ship no `application.yml`; no consumer receives this file,
 * and a host writes its own. What the tests bound to it pin is therefore the documentation's truth, not an artefact's
 * runtime behaviour. [FIXTURE] is the test-owned `src/test/resources/application-test.yml`, a copy an edit of the
 * reference never reaches.
 *
 * An IDENTITY or an INVARIANT - the alias set, the instance keys, the logger name the level line must address, the
 * bulkhead sum against the pool - is pinned in BOTH files, with `@ValueSource(strings = [REFERENCE, FIXTURE])`:
 * nobody legitimately changes those by hand, and a drift in the reference has to fail the build, which a test bound
 * to the fixture alone cannot see. A VALUE an operator may tune - a level, a body mode, a threshold - is pinned in the
 * [FIXTURE] only, so an edit of a recommended value in the reference cannot break the suite.
 *
 * Binding goes through `YamlPropertySourceLoader` + `Binder`, the way Spring Boot reads a config file at startup: no
 * context, no I/O beyond the classpath.
 */
object ConfigurationFiles {
    const val REFERENCE = "application.yml"
    const val FIXTURE = "application-test.yml"

    /**
     * Bind [prefix] of [file] into [type].
     *
     * Compatibility: `T : Any` is required, not decorative - with the implicit `Any?` bound, none of Binder's Java
     * overloads match and the call does not compile.
     */
    fun <T : Any> bind(
        file: String,
        prefix: String,
        type: Class<T>,
    ): T =
        binder(file)
            .bind(prefix, type)
            .orElseThrow { AssertionError("$prefix did not bind from $file") }

    /** Bind [prefix] of [file] flattened to a string map, for blocks that have no properties class. */
    fun bindMap(
        file: String,
        prefix: String,
    ): Map<String, String> =
        binder(file)
            .bind(prefix, Bindable.mapOf(String::class.java, String::class.java))
            .orElseThrow { AssertionError("$prefix did not bind from $file") }

    /** Every property of [file] whose key starts with [prefix], flattened as Boot flattens YAML (`a.b[0].c`). */
    fun flatten(
        file: String,
        prefix: String = "",
    ): Map<String, String> =
        YamlPropertySourceLoader()
            .load(file, ClassPathResource(file))
            .flatMap { source ->
                val enumerable = source as EnumerablePropertySource<*>
                enumerable.propertyNames.filter { it.startsWith(prefix) }.map { it to enumerable.getProperty(it).toString() }
            }.toMap()

    /**
     * The keys of the reference configuration ([REFERENCE]) under [prefixes], flattened the way Boot loads them - the
     * property source of a context that runs on the documented blocks. Read from the real file rather than restated
     * in a test: a copy would keep passing after the reference value changed, which is the one thing a test bound to
     * the reference exists to catch.
     *
     * Fails the test when a prefix matches no key: a prefix that silently matches nothing would turn every assertion
     * on the context into a tautology about code defaults.
     */
    fun referenceProperties(vararg prefixes: String): Map<String, Any> =
        prefixes
            .map { prefix ->
                flatten(REFERENCE, prefix).also { keys ->
                    assertThat(keys).`as`("reference application.yml must carry '%s*' keys", prefix).isNotEmpty()
                }
            }.fold(emptyMap()) { all, keys -> all + keys }

    private fun binder(file: String): Binder {
        val sources = YamlPropertySourceLoader().load(file, ClassPathResource(file))
        return Binder(ConfigurationPropertySources.from(sources))
    }
}
