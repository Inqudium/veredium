package eu.inqudium.veredium.webclient

import org.springframework.boot.context.properties.EnableConfigurationProperties

/**
 * The `app.webclient` binding alone, for a hand-assembled test context: [WebClientConfiguration] itself also
 * pulls in the core's mesh wiring, whose startup guard needs Resilience4j's auto-configuration, which those
 * contexts leave out. Binding from the Environment is what the contexts reproduce, so a configured
 * `app.webclient.*` value that never arrives fails a test instead of silently degrading to the code default.
 */
@EnableConfigurationProperties(WebClientProperties::class)
class BoundWebClientProperties
