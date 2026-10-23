package eu.inqudium.veredium.examples.reactive

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

/**
 * A host of the reactive client. As for the blocking one, the scan of `eu.inqudium.veredium.webclient` is the
 * whole wiring: the module's configuration, its factory and the mesh wiring of `veredium-core`.
 */
@SpringBootApplication(scanBasePackages = ["eu.inqudium.veredium.examples.reactive", "eu.inqudium.veredium.webclient"])
class ReactiveExampleApplication

fun main(args: Array<String>) {
    runApplication<ReactiveExampleApplication>(*args)
}
