package eu.inqudium.veredium.examples.mixed

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

/**
 * A host of BOTH clients in one context: the customer directory on the blocking client, the order search on the
 * reactive one. Both modules are dependencies and both auto-configurations are active - no exclusion. What makes that
 * work is one key per endpoint, `app.mesh.endpoints[].stack`, which the `mixed` profile sets
 * (`application-mixed.yml`); without it the start fails, naming the key (ADR-0003, "each mesh endpoint is owned by
 * one client stack").
 */
@SpringBootApplication
class MixedExampleApplication

fun main(args: Array<String>) {
    runApplication<MixedExampleApplication>(*args) { setAdditionalProfiles(PROFILE) }
}

/** The profile that assigns each mesh endpoint to its stack. */
const val PROFILE: String = "mixed"
