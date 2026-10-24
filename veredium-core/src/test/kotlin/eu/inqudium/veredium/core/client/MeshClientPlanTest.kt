package eu.inqudium.veredium.core.client

import eu.inqudium.veredium.core.mesh.MeshProperties
import eu.inqudium.veredium.core.validation.ResponseValidationProperties
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.time.Duration

/**
 * What [MeshClientPlan] derives from `app.mesh` for one stack, on hand-built [MeshProperties] with the default
 * margins and transport (connect 100 ms, pending-acquire 200 ms): the drain window, the endpoint lookup and the
 * default headers - the parts both client factories take as they are.
 */
class MeshClientPlanTest {
    @Test
    fun `should drain for the longest response timeout of the stack's endpoints, plus the pre-response phases and a second`() {
        // What is tested: drainWindow over three endpoints whose response timeouts are 700, 1400 and 900 ms (anchors
        //   300, 1000 and 500 ms), the longest in the middle of the list, and over a stack without endpoints.
        // Success criteria: 1400 + 100 + 200 + 1000 = 2700 ms; without endpoints 100 + 200 + 1000 = 1300 ms.
        // Why it matters: no exchange can outlive its own response timeout, so the window has to cover the longest
        //   of them - the shortest, the first or the last would cut off the slowest backend's calls at every
        //   shutdown. The longest sits in the middle so that none of those three readings matches it by accident.
        //
        // Given
        val endpoints =
            listOf(
                endpoint("fast", anchorMs = 300),
                endpoint("slow", anchorMs = 1_000),
                endpoint("mid", anchorMs = 500),
            )

        // When
        val window = plan(MeshProperties(endpoints = endpoints)).drainWindow
        val empty = plan(MeshProperties()).drainWindow

        // Then
        assertThat(window).isEqualTo(Duration.ofMillis(2_700))
        assertThat(empty).isEqualTo(Duration.ofMillis(1_300))
    }

    @Test
    fun `should tell a backend of the other stack from one that is not configured`() {
        // What is tested: endpoint() for an alias of this stack, an alias another stack owns, and an alias no endpoint
        //   has.
        // Success criteria: the own alias resolves; the foreign one fails with an IllegalArgumentException naming the
        //   owning stack and its factory; the missing one with an IllegalStateException naming the key to add.
        // Why it matters: both fail the client's construction at startup. The two causes need different fixes - build
        //   the client with the other factory, or add the endpoint - and a message that named the wrong one would
        //   send the reader to the wrong place.
        //
        // Given
        val plan =
            plan(
                MeshProperties(
                    endpoints =
                        listOf(
                            endpoint("ledger", anchorMs = 300, stack = "restclient"),
                            endpoint("stream", anchorMs = 300, stack = "webclient"),
                        ),
                ),
            )

        // When / Then
        assertThat(plan.endpoint("ledger").alias).isEqualTo("ledger")
        assertThatThrownBy { plan.endpoint("stream") }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessage(
                "Backend 'stream' is owned by the webclient stack (app.mesh.endpoints[].stack=webclient), not by the " +
                    "restclient stack: build its client with the webclient stack's factory.",
            )
        assertThatThrownBy { plan.endpoint("ghost") }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessage("No mesh endpoint configured for backend 'ghost'. Add an app.mesh.endpoints[] entry whose alias equals 'ghost'.")
    }

    @Test
    fun `should set Host and User-Agent first, the endpoint's own headers after them and Accept last`() {
        // What is tested: defaultHeaders for an endpoint with two static headers of its own, one of them User-Agent.
        // Success criteria: Host (from the triplet), User-Agent (the stack's), the endpoint's two headers in their
        //   configured order, then Accept: application/json.
        // Why it matters: the headers are set on the client builder in this order, so a later one replaces an
        //   earlier one of the same name - an endpoint can override the User-Agent or Host, but not the JSON
        //   Accept the response decoding relies on.
        //
        // Given
        val endpoint =
            MeshProperties
                .EndpointConfig(
                    triplet = "/endpoint/ledger.example.com/GET/v1/entries",
                    alias = "ledger",
                    individualRequestTimeoutMs = 300,
                    headers = linkedMapOf("X-Tenant" to "t-1", "User-Agent" to "ledger-client/2"),
                )
        val plan = plan(MeshProperties(endpoints = listOf(endpoint)))

        // When
        val headers = plan.defaultHeaders(plan.endpoint("ledger"), "veredium-test")

        // Then
        assertThat(headers).containsExactly(
            "Host" to "ledger.example.com",
            "User-Agent" to "veredium-test",
            "X-Tenant" to "t-1",
            "User-Agent" to "ledger-client/2",
            "Accept" to "application/json",
        )
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private fun endpoint(
        alias: String,
        anchorMs: Long,
        stack: String? = null,
    ) = MeshProperties.EndpointConfig(
        triplet = "/endpoint/$alias.example.com/GET/v1/$alias",
        alias = alias,
        individualRequestTimeoutMs = anchorMs,
        stack = stack,
    )

    private fun plan(mesh: MeshProperties) =
        MeshClientPlan(
            mesh,
            stack = "restclient",
            propertiesPrefix = "app.restclient",
            validationProperties = ResponseValidationProperties(),
            validatorFactory = null,
            logger = LoggerFactory.getLogger(MeshClientPlanTest::class.java),
        )
}
