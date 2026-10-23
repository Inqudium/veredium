package eu.inqudium.veredium.examples.async

import eu.inqudium.veredium.webclient.VerediumWebClientAutoConfiguration
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

/**
 * A host of the blocking client that calls it without blocking. The wiring is the blocking example's: none beyond the
 * dependency, and the exclusion of the reactive client that only the examples need. What differs is only which
 * methods of the client its adapter calls.
 */
@SpringBootApplication(exclude = [VerediumWebClientAutoConfiguration::class])
class AsyncExampleApplication

fun main(args: Array<String>) {
    runApplication<AsyncExampleApplication>(*args)
}
