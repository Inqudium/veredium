package eu.inqudium.veredium.restclient

import org.springframework.util.ClassUtils

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
 * @property key The value the property takes in YAML and the name the startup lines use.
 * @property markerClass A class of the engine's library; its absence on the classpath is a failed start that
 *   names [dependency].
 * @property dependency What a host adds to select the engine, named in the failure when [markerClass] is absent.
 */
enum class RestClientEngine(
    val key: String,
    val markerClass: String,
    val dependency: String,
) {
    /**
     * Apache HttpClient 5 through Spring's `HttpComponentsClientHttpRequestFactory` - the default, and the only
     * engine whose library is a regular dependency of this module. One shared pool with every knob the mesh
     * transport and `app.restclient.pool` express, and the pool meters (`restclient.pool.*`).
     */
    APACHE(
        key = "apache",
        markerClass = "org.apache.hc.client5.http.impl.classic.HttpClients",
        dependency = "org.apache.httpcomponents.client5:httpclient5 (a regular dependency of veredium-restclient)",
    ),

    /**
     * The JDK's `java.net.http.HttpClient` through Spring's `JdkClientHttpRequestFactory`. No extra dependency,
     * but no pool knob either, and the `Host` header is a restricted header the JDK replaces by the URI's
     * authority (the factory refuses to start where that would change what the sidecar routes on).
     */
    JDK(
        key = "jdk",
        markerClass = "java.net.http.HttpClient",
        dependency = "the JDK module java.net.http (part of every full JDK; add it to a jlink image)",
    ),

    /** Eclipse Jetty's `HttpClient` through Spring's `JettyClientHttpRequestFactory`. */
    JETTY(
        key = "jetty",
        markerClass = "org.eclipse.jetty.client.HttpClient",
        dependency = "org.eclipse.jetty:jetty-client (version managed by the Spring Boot BOM)",
    ),

    /** Reactor Netty's `HttpClient` through Spring's `ReactorClientHttpRequestFactory`, on resources the factory owns. */
    REACTOR_NETTY(
        key = "reactor-netty",
        markerClass = "reactor.netty.http.client.HttpClient",
        dependency = "io.projectreactor.netty:reactor-netty-http (version managed by the Spring Boot BOM)",
    ),
    ;

    /** Whether the engine's library is on the classpath this module was loaded from. */
    fun isPresent(): Boolean = ClassUtils.isPresent(markerClass, RestClientEngine::class.java.classLoader)

    override fun toString(): String = key
}
