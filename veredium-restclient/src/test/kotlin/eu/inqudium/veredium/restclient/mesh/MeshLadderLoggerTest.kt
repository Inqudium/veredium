package eu.inqudium.veredium.restclient.mesh

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class MeshLadderLoggerTest {
    @Test
    fun `builds one derived ladder line per endpoint, sorted by alias`() {
        // What is tested: the startup log content - one line per endpoint carrying the fully derived ladder,
        //   built by the pure ladderLines() that the @EventListener(ApplicationReadyEvent) hook logs on startup.
        // How it is deemed successful: two endpoints with different anchors produce exactly the two expected
        //   lines, ordered by alias (payments given first but sorted after accounts, since GET < POST).
        // Why it matters: the ladder is otherwise implicit (only the anchor is maintained); the startup log is
        //   the app-side inspectability guarantee, so its exact content is worth pinning.

        // Given two endpoints with different anchors (globalMarginMs pinned to 1000; at maxRetries=0 the globals
        // derive to 1500/2750 = 1*individual + 1000)
        val props =
            MeshProperties(
                defaults =
                    MeshProperties.Defaults(
                        individualRequestTimeoutMs = 1000,
                        margins = MeshProperties.Margins(globalMarginMs = 1000),
                    ),
                endpoints =
                    listOf(
                        MeshProperties.EndpointConfig("/endpoint/api.example.com/POST/payments", individualRequestTimeoutMs = 1750),
                        MeshProperties.EndpointConfig("/endpoint/api.example.com/GET/accounts/{accountId}", individualRequestTimeoutMs = 500),
                    ),
            )

        // When the ladder lines are built
        val lines = MeshLadderLogger(props).ladderLines()

        // Then one line per endpoint, sorted by the host-prefixed alias, each carrying the full derived ladder
        assertThat(lines).containsExactly(
            "mesh ladder [api-example-com-GET-accounts] /endpoint/api.example.com/GET/accounts/{accountId}: " +
                "individual=500ms global=1500ms responseTimeout=1700ms slowCall=300ms timeLimiter=2100ms",
            "mesh ladder [api-example-com-POST-payments] /endpoint/api.example.com/POST/payments: " +
                "individual=1750ms global=2750ms responseTimeout=2950ms slowCall=1550ms timeLimiter=3350ms",
        )
    }
}
