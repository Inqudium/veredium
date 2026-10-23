package eu.inqudium.veredium.examples.decorator

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

/**
 * A host that bounds a blocking call of its own - a native Apache HttpClient 4 lookup - with the core's
 * [eu.inqudium.veredium.core.resilience.BlockingResilienceDecorator]. It scans no client package: there is no
 * `RestClient`, no mesh wiring and no exchange log, only the decorator over the three Resilience4j registries
 * that `resilience4j-spring-boot4` auto-configures ([DecoratorConfiguration]).
 */
@SpringBootApplication(scanBasePackages = ["eu.inqudium.veredium.examples.decorator"])
class DecoratorExampleApplication

fun main(args: Array<String>) {
    runApplication<DecoratorExampleApplication>(*args)
}
