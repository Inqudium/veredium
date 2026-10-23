package eu.inqudium.veredium.examples.reactive

import eu.inqudium.veredium.restclient.VerediumRestClientAutoConfiguration
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

/**
 * A host of the reactive client. As for the blocking one, the dependency on `veredium-webclient` is the whole wiring:
 * its auto-configuration brings the module's factory, its decorator and the mesh wiring of `veredium-core`. The
 * exclusion of the blocking client is the examples' own, which have both clients on their classpath.
 */
@SpringBootApplication(exclude = [VerediumRestClientAutoConfiguration::class])
class ReactiveExampleApplication

fun main(args: Array<String>) {
    runApplication<ReactiveExampleApplication>(*args)
}
