package eu.inqudium.veredium.webclient

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
 * The YAML values are the lower-case, dashed names in [propertyValue] (`reactor-netty`, `jdk`, `jetty`,
 * `apache`); Boot's relaxed enum binding maps them onto the constants.
 *
 * Everything above the connector is the same for every engine: the codec limit, the status mapping, the
 * empty-body contract, the default headers, legatium's exchange event, the observation and the Resilience4j
 * decoration. What differs is how far an engine can carry the mesh transport settings and the `app.webclient.pool`
 * knobs, and which transport meters exist; the factory logs that per engine in use at startup, and
 * `webclient-config-guide.md` tabulates it.
 *
 * @property propertyValue The value as written under `app.webclient.engine`, also used on the startup lines.
 * @property library What the engine is, for the startup lines.
 * @property requiredClasses Classes whose presence proves the engine's library is on the classpath.
 * @property coordinates What a host adds to its build when [requiredClasses] are missing - named in the
 *   startup failure.
 */
enum class WebClientEngine(
    val propertyValue: String,
    val library: String,
    val requiredClasses: List<String>,
    val coordinates: String,
) {
    /** Spring's `ReactorClientHttpConnector` over Reactor Netty - the default, and the wiring this module always had. */
    REACTOR_NETTY(
        propertyValue = "reactor-netty",
        library = "ReactorClientHttpConnector over Reactor Netty",
        requiredClasses = listOf("reactor.netty.http.client.HttpClient"),
        coordinates = "io.projectreactor.netty:reactor-netty-http (comes with spring-boot-starter-webflux)",
    ),

    /** Spring's `JdkClientHttpConnector` over the JDK's `java.net.http.HttpClient`; no extra dependency. */
    JDK(
        propertyValue = "jdk",
        library = "JdkClientHttpConnector over java.net.http.HttpClient",
        requiredClasses = listOf("java.net.http.HttpClient"),
        coordinates = "the java.net.http module of the JDK (missing from a jlink image that left it out)",
    ),

    /** Spring's `JettyClientHttpConnector` over Jetty's `HttpClient` and its reactive adapter. */
    JETTY(
        propertyValue = "jetty",
        library = "JettyClientHttpConnector over Jetty HttpClient",
        requiredClasses = listOf("org.eclipse.jetty.client.HttpClient", "org.eclipse.jetty.reactive.client.ReactiveRequest"),
        coordinates = "org.eclipse.jetty:jetty-client and org.eclipse.jetty:jetty-reactive-httpclient",
    ),

    /** Spring's `HttpComponentsClientHttpConnector` over Apache HttpClient 5's async client. */
    APACHE(
        propertyValue = "apache",
        library = "HttpComponentsClientHttpConnector over Apache HttpClient 5 (async)",
        requiredClasses =
            listOf(
                "org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient",
                "org.apache.hc.core5.reactive.ReactiveResponseConsumer",
            ),
        coordinates = "org.apache.httpcomponents.client5:httpclient5 and org.apache.httpcomponents.core5:httpcore5-reactive",
    ),
    ;

    override fun toString(): String = propertyValue
}
