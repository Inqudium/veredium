package eu.inqudium.veredium.examples.async

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

/**
 * A host of the blocking client that calls it without blocking. The wiring is the blocking example's: the scan of
 * `eu.inqudium.veredium.restclient`. What differs is only which methods of the client its adapter calls.
 */
@SpringBootApplication(scanBasePackages = ["eu.inqudium.veredium.examples.async", "eu.inqudium.veredium.restclient"])
class AsyncExampleApplication

fun main(args: Array<String>) {
    runApplication<AsyncExampleApplication>(*args)
}
