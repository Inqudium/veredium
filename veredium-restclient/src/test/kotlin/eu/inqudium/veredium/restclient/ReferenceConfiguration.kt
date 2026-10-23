package eu.inqudium.veredium.restclient

import eu.inqudium.veredium.testsupport.ConfigurationFiles
import org.assertj.core.api.Assertions.assertThat

/**
 * The keys of the reference configuration ([ConfigurationFiles.REFERENCE]) under [prefixes], flattened the way Boot
 * loads them - the property source of a context that runs on the documented blocks. Read from the real file rather
 * than restated in a test: a copy would keep passing after the reference value changed, which is the one thing a
 * test bound to the reference exists to catch.
 *
 * Fails the test when a prefix matches no key: a prefix that silently matches nothing would turn every assertion
 * on the context into a tautology about code defaults.
 */
fun referenceProperties(vararg prefixes: String): Map<String, Any> =
    prefixes
        .map { prefix ->
            ConfigurationFiles.flatten(ConfigurationFiles.REFERENCE, prefix).also { keys ->
                assertThat(keys).`as`("reference application.yml must carry '%s*' keys", prefix).isNotEmpty()
            }
        }.fold(emptyMap()) { all, keys -> all + keys }
