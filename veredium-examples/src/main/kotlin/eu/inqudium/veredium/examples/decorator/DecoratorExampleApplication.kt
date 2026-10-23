package eu.inqudium.veredium.examples.decorator

import eu.inqudium.veredium.restclient.VerediumRestClientAutoConfiguration
import eu.inqudium.veredium.webclient.VerediumWebClientAutoConfiguration
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

/**
 * A host that bounds a blocking call of its own - a native Apache HttpClient 4 lookup - with the core's
 * [eu.inqudium.veredium.core.resilience.BlockingResilienceDecorator]. It uses no client module: there is no
 * `RestClient`, no mesh wiring and no exchange log, only the decorator over the three Resilience4j registries
 * that `resilience4j-spring-boot4` auto-configures ([DecoratorConfiguration]). A host of the decorator alone would
 * not have the client modules on its classpath; the examples have both, so their auto-configurations are excluded.
 */
@SpringBootApplication(exclude = [VerediumRestClientAutoConfiguration::class, VerediumWebClientAutoConfiguration::class])
class DecoratorExampleApplication

fun main(args: Array<String>) {
    runApplication<DecoratorExampleApplication>(*args)
}
