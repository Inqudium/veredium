package eu.inqudium.veredium.examples

import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.annotation.AliasFor
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.TestConstructor
import kotlin.reflect.KClass

/**
 * Starts [application] as a host starts it (`@SpringBootTest`), pointed at the stand-in sidecar
 * ([Sidecar.Properties]), and injects its beans through the test class's constructor.
 *
 * Every example's context runs in one test JVM, so none joins Micrometer's global registry: a context would join it
 * after another one had registered meters there. Assumption: Micrometer (1.17.1) warns about that. A single host does
 * not need the setting.
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@SpringBootTest(properties = ["management.metrics.use-global-registry=false"])
@ContextConfiguration(initializers = [Sidecar.Properties::class])
@TestConstructor(autowireMode = TestConstructor.AutowireMode.ALL)
annotation class ExampleContext(
    /** The example's application class. */
    @get:AliasFor(annotation = SpringBootTest::class, attribute = "classes")
    val application: KClass<*>,
)
