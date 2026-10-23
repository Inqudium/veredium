package eu.inqudium.veredium.examples.blocking

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

/**
 * A host of the blocking client. The one step it takes is the scan of `eu.inqudium.veredium.restclient`: that
 * brings in the module's configuration, its factory and - through the configuration - the mesh wiring of
 * `veredium-core`. Everything else is `application.yml`.
 */
@SpringBootApplication(scanBasePackages = ["eu.inqudium.veredium.examples.blocking", "eu.inqudium.veredium.restclient"])
class BlockingExampleApplication

fun main(args: Array<String>) {
    runApplication<BlockingExampleApplication>(*args)
}
