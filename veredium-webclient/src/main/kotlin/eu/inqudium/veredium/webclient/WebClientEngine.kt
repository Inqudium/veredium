package eu.inqudium.veredium.webclient

import eu.inqudium.veredium.core.engine.HttpEngine

/**
 * The HTTP engine underneath the clients of [ResilientWebClientFactory] - which library opens the connections,
 * selected per backend with `app.webclient.engine` ([WebClientProperties.Engine]).
 *
 * Each value maps onto one of Spring's own reactive connectors; the factory builds that connector itself, on
 * resources it owns, and sets it on its clone of Boot's `WebClient.Builder` only. Nothing about the choice
 * reaches another client of the host: Boot's own switch `spring.http.clients.reactive.connector` would change
 * every WebClient the host builds from Boot's builder, which is why this module has a switch of its own and
 * never reads or writes Boot's.
 *
 * The YAML values are the lower-case, dashed names in [key] (`reactor-netty`, `jdk`, `jetty`, `apache`); Boot's
 * relaxed enum binding maps them onto the constants.
 *
 * Everything above the connector is the same for every engine: the codec limit, the status mapping, the
 * empty-body contract, the default headers, legatium's exchange event, the observation and the Resilience4j
 * decoration. What differs is how far an engine can carry the mesh transport settings and the `app.webclient.pool`
 * knobs, and which transport meters exist; the factory logs that per engine in use at startup, and
 * `webclient-config-guide.md` tabulates it.
 *
 * [key], [requiredClasses] and [dependency] are those of [HttpEngine], shared with the blocking module's engines,
 * which the start-time checks of `eu.inqudium.veredium.core.engine.EngineSelection` read.
 *
 * @property library What the engine is, for the startup lines.
 */
enum class WebClientEngine(
    override val key: String,
    val library: String,
    override val requiredClasses: List<String>,
    override val dependency: String,
) : HttpEngine {
    /** Spring's `ReactorClientHttpConnector` over Reactor Netty - the default, and the wiring this module always had. */
    REACTOR_NETTY(
        key = "reactor-netty",
        library = "ReactorClientHttpConnector over Reactor Netty",
        requiredClasses = listOf("reactor.netty.http.client.HttpClient"),
        dependency = "io.projectreactor.netty:reactor-netty-http (comes with spring-boot-starter-webflux)",
    ),

    /** Spring's `JdkClientHttpConnector` over the JDK's `java.net.http.HttpClient`; no extra dependency. */
    JDK(
        key = "jdk",
        library = "JdkClientHttpConnector over java.net.http.HttpClient",
        requiredClasses = listOf("java.net.http.HttpClient"),
        dependency = "the java.net.http module of the JDK (missing from a jlink image that left it out)",
    ),

    /** Spring's `JettyClientHttpConnector` over Jetty's `HttpClient` and its reactive adapter. */
    JETTY(
        key = "jetty",
        library = "JettyClientHttpConnector over Jetty HttpClient",
        requiredClasses = listOf("org.eclipse.jetty.client.HttpClient", "org.eclipse.jetty.reactive.client.ReactiveRequest"),
        dependency = "org.eclipse.jetty:jetty-client and org.eclipse.jetty:jetty-reactive-httpclient",
    ),

    /** Spring's `HttpComponentsClientHttpConnector` over Apache HttpClient 5's async client. */
    APACHE(
        key = "apache",
        library = "HttpComponentsClientHttpConnector over Apache HttpClient 5 (async)",
        requiredClasses =
            listOf(
                "org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient",
                "org.apache.hc.core5.reactive.ReactiveResponseConsumer",
            ),
        dependency = "org.apache.httpcomponents.client5:httpclient5 and org.apache.httpcomponents.core5:httpcore5-reactive",
    ),
    ;

    override fun toString(): String = key
}
