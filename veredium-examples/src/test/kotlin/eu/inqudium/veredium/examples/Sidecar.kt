package eu.inqudium.veredium.examples

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.equalToJson
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.notFound
import com.github.tomakehurst.wiremock.client.WireMock.okJson
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.options
import org.springframework.test.context.DynamicPropertyRegistry

/**
 * The stand-in for the local sidecar both examples call: one WireMock server, started once per JVM, that answers
 * like the two backends behind it - routed, as the sidecar routes, by the Host header the triplet names.
 */
object Sidecar {
    val server: WireMockServer = WireMockServer(options().dynamicPort()).apply { start() }

    /** Points the examples' mesh at this server instead of the sidecar on `localhost:8042`. */
    fun register(registry: DynamicPropertyRegistry) {
        registry.add("app.mesh.defaults.transport.sidecar-base-url") { server.baseUrl() }
    }

    /** Clears the previous test's stubs and requests and stubs the two backends again. */
    fun reset() {
        server.resetAll()
        server.stubFor(
            get(urlEqualTo("/v1/customers/c-1"))
                .withHeader("Host", equalTo("customers.example.com"))
                .willReturn(okJson("""{"id":"c-1","name":"Ada"}""")),
        )
        server.stubFor(get(urlEqualTo("/v1/customers/c-404")).willReturn(notFound()))
        server.stubFor(
            post(urlEqualTo("/v1/orders/search"))
                .withHeader("Host", equalTo("orders.example.com"))
                .withRequestBody(equalToJson("""{"customerId":"c-1","limit":20}"""))
                .willReturn(okJson("""{"orders":[{"id":"o-1","total":"12.50"},{"id":"o-2","total":"3.00"}]}""")),
        )
        server.stubFor(
            post(urlEqualTo("/v1/orders/search"))
                .withRequestBody(equalToJson("""{"customerId":"c-404","limit":20}"""))
                .willReturn(okJson("""{"orders":[]}""")),
        )
    }

    /** The customer and orders the stubs answer for `c-1`. */
    val ADA = Customer("c-1", "Ada")
    val ADAS_ORDERS = listOf(Order("o-1", "12.50"), Order("o-2", "3.00"))
}
