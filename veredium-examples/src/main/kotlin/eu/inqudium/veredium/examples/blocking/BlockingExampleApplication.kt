package eu.inqudium.veredium.examples.blocking

import eu.inqudium.veredium.webclient.VerediumWebClientAutoConfiguration
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

/**
 * A host of the blocking client. It takes no wiring step: the dependency on `veredium-restclient` brings the module's
 * auto-configuration - its factory, its decorator and the mesh wiring of `veredium-core`. Everything else is
 * `application.yml`. The one exclusion is this module's, not a host's: the examples have the reactive client on their
 * classpath too, and this example runs both of its backends on the blocking one - [the mixed
 * example][eu.inqudium.veredium.examples.mixed.MixedExampleApplication] shows both stacks in one context.
 */
@SpringBootApplication(exclude = [VerediumWebClientAutoConfiguration::class])
class BlockingExampleApplication

fun main(args: Array<String>) {
    runApplication<BlockingExampleApplication>(*args)
}
