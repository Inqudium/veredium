package eu.inqudium.veredium.restclient

import eu.inqudium.veredium.core.mesh.MeshProperties
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import java.time.Duration

/**
 * The TimeLimiter budget of every client a test does not give a budget of its own: far beyond any loopback
 * exchange, so only a test ABOUT timeouts ever sees one (Resilience4j's 1 s default would make every real-HTTP test
 * depend on the build machine's load).
 */
internal val GENEROUS_BUDGET: TimeLimiterConfig = TimeLimiterConfig.custom().timeoutDuration(Duration.ofSeconds(30)).build()

/**
 * A mesh with one `GET` endpoint per alias in [aliases], all routed to the sidecar at [sidecarBaseUrl] with the
 * triplet host [host] - the Host header the backend sees. The JDK engine sends only the target's own authority, so
 * a test that runs on it passes the sidecar's `host:port`.
 */
internal fun meshOf(
    sidecarBaseUrl: String,
    host: String,
    vararg aliases: String,
    individualRequestTimeoutMs: Long = 1000,
): MeshProperties =
    MeshProperties(
        defaults =
            MeshProperties.Defaults(
                individualRequestTimeoutMs = individualRequestTimeoutMs,
                transport = MeshProperties.Transport(sidecarBaseUrl = sidecarBaseUrl),
            ),
        endpoints = aliases.map { MeshProperties.EndpointConfig(triplet = "/endpoint/$host/GET/$it", alias = it) },
    )
