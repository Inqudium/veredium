package eu.inqudium.veredium.restclient

import eu.inqudium.veredium.core.engine.HttpEngine

/**
 * The HTTP engine underneath a [ResilientRestClient]: which of Spring's blocking request factories carries the
 * exchange of a backend. Selected per factory with `app.restclient.engine.default` and per backend with
 * `app.restclient.engine.endpoints.<mesh-alias>` ([RestClientProperties.Engine]); the YAML values are [key]
 * (`apache`, `jdk`, `jetty`, `reactor-netty` - Boot's relaxed enum binding also accepts the constant names).
 *
 * The selection is THIS module's, for THIS module's clients only. It never reads or writes Boot's own switch
 * (`spring.http.clients.imperative.factory`), and the factory registers no request factory, HTTP client,
 * `ClientHttpRequestFactoryBuilder` or `HttpClientSettings` bean: every engine is an object the factory owns,
 * set on its private clone of Boot's `RestClient.Builder`. Boot's switch selects the engine of every RestClient
 * in the host, which is exactly what a library must not do to a host that has other clients - the guide's
 * "Engines" section has the long form.
 *
 * What each engine maps from the mesh ladder and the pool settings, and what it cannot express, is stated on one
 * startup line per engine in use and in the guide's mapping table; the per-engine classes carry the detail.
 *
 * The three properties are those of [HttpEngine], shared with the reactive module's engines, which the start-time
 * checks of [eu.inqudium.veredium.core.engine.EngineSelection] read.
 */
enum class RestClientEngine(
    override val key: String,
    override val requiredClasses: List<String>,
    override val dependency: String,
) : HttpEngine {
    /**
     * Apache HttpClient 5 through Spring's `HttpComponentsClientHttpRequestFactory` - the default, and the only
     * engine whose library is a regular dependency of this module. One shared pool with every knob the mesh
     * transport and `app.restclient.pool` express, and the pool meters (`restclient.pool.*`).
     */
    APACHE(
        key = "apache",
        requiredClasses = listOf("org.apache.hc.client5.http.impl.classic.HttpClients"),
        dependency = "org.apache.httpcomponents.client5:httpclient5 (a regular dependency of veredium-restclient)",
    ),

    /**
     * The JDK's `java.net.http.HttpClient` through Spring's `JdkClientHttpRequestFactory`. No extra dependency,
     * but no pool knob either, and the `Host` header is a restricted header the JDK replaces by the URI's
     * authority (the factory refuses to start where that would change what the sidecar routes on).
     */
    JDK(
        key = "jdk",
        requiredClasses = listOf("java.net.http.HttpClient"),
        dependency = "the JDK module java.net.http (part of every full JDK; add it to a jlink image)",
    ),

    /** Eclipse Jetty's `HttpClient` through Spring's `JettyClientHttpRequestFactory`. */
    JETTY(
        key = "jetty",
        requiredClasses = listOf("org.eclipse.jetty.client.HttpClient"),
        dependency = "org.eclipse.jetty:jetty-client (version managed by the Spring Boot BOM)",
    ),

    /** Reactor Netty's `HttpClient` through Spring's `ReactorClientHttpRequestFactory`, on resources the factory owns. */
    REACTOR_NETTY(
        key = "reactor-netty",
        requiredClasses = listOf("reactor.netty.http.client.HttpClient"),
        dependency = "io.projectreactor.netty:reactor-netty-http (version managed by the Spring Boot BOM)",
    ),
    ;

    override fun toString(): String = key
}
