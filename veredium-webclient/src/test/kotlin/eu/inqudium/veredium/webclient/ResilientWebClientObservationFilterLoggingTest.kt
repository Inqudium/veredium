package eu.inqudium.veredium.webclient

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import eu.inqudium.veredium.webclient.mesh.MeshProperties
import io.github.resilience4j.bulkhead.BulkheadConfig
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import io.micrometer.observation.Observation
import io.micrometer.observation.ObservationHandler
import io.micrometer.observation.ObservationRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.mock.env.MockEnvironment
import org.springframework.web.reactive.function.client.WebClient
import tools.jackson.databind.json.JsonMapper

/**
 * Verifies the two observation lines [ResilientWebClientFactory] writes at startup.
 *
 * 1. What is tested: (a) the factory reads and logs `management.observations.enable.all` and
 *    `management.observations.enable.http.client.requests` - the ONLY central Spring switch, a name-prefixed
 *    DEACTIVATION filter (default true, NOT an enable switch) - and flags when a `false` there suppresses the
 *    client observation even though the module toggle is on; and (b) that the module-toggle line reports what
 *    the toggle ACHIEVES, distinguishing an `on` with a live `ObservationRegistry` behind it from an `on` with
 *    no registry at all.
 * 2. Success criterion: the disable-filter line shows the effective values (default true when unset, false
 *    when the property is set false) and carries the "SUPPRESSED centrally" note exactly when the module
 *    toggle is on but the central filter is off; the module-toggle line names the registry's actual state.
 * 3. Why it matters: both switches fail SILENTLY and in the same direction - no span, no timer, nothing in the
 *    log to say why. The central switch is a DISABLE-only, prefix-matched override that is easy to miss; and a
 *    toggle turned on in a host with no `ObservationRegistry` bean is indistinguishable at runtime from one
 *    left off. Reporting either as a plain "on" is what sends someone hunting through Micrometer configuration
 *    for a span the process was never able to create.
 */
class ResilientWebClientObservationFilterLoggingTest {
    private val factoryLogger = LoggerFactory.getLogger(ResilientWebClientFactory::class.java) as Logger
    private val logAppender = ListAppender<ILoggingEvent>()

    @BeforeEach
    fun attachAppender() {
        logAppender.start()
        factoryLogger.addAppender(logAppender)
    }

    @AfterEach
    fun detachAppender() {
        factoryLogger.detachAppender(logAppender)
    }

    @Test
    fun `should log the central disable-filter as true by default when the properties are unset`() {
        // Given: no management.observations.enable.* properties set, module toggle on
        // When: the factory is constructed (it logs in its init block)
        newFactory(observationEnabled = true, environment = MockEnvironment())

        // Then: the disable-filter line reports both switches as their default true, and does NOT suppress
        val line = disableFilterLine()
        assertThat(line).contains("management.observations.enable.all=true")
        assertThat(line).contains("management.observations.enable.http.client.requests=true")
        assertThat(line).doesNotContain("SUPPRESSED")
    }

    @Test
    fun `should log http-client-requests false and flag central suppression when that switch is off`() {
        // Given: the client observation is centrally disabled while the module toggle is on
        val environment =
            MockEnvironment().withProperty("management.observations.enable.http.client.requests", "false")

        // When
        newFactory(observationEnabled = true, environment = environment)

        // Then: the line shows the false value AND warns the wired observation is suppressed centrally
        val line = disableFilterLine()
        assertThat(line).contains("management.observations.enable.http.client.requests=false")
        assertThat(line).contains("'http.client.requests' is SUPPRESSED centrally despite the module toggle being on")
    }

    @Test
    fun `should log all false and flag central suppression when the global switch is off`() {
        // Given: ALL observations are centrally disabled while the module toggle is on
        val environment = MockEnvironment().withProperty("management.observations.enable.all", "false")

        // When
        newFactory(observationEnabled = true, environment = environment)

        // Then: the global switch reads false and the suppression note is present
        val line = disableFilterLine()
        assertThat(line).contains("management.observations.enable.all=false")
        assertThat(line).contains("SUPPRESSED centrally")
    }

    @Test
    fun `should not flag suppression when the module toggle is off regardless of the central defaults`() {
        // Given: module toggle off; the central filter defaults are irrelevant because nothing is wired
        // When
        newFactory(observationEnabled = false, environment = MockEnvironment())

        // Then: the disable-filter line is still logged (for visibility) but carries no suppression note
        val line = disableFilterLine()
        assertThat(line).contains("management.observations.enable.all=true")
        assertThat(line).doesNotContain("SUPPRESSED")
    }

    /** The single central disable-filter log line emitted by the factory's init block. */
    private fun disableFilterLine(): String =
        logAppender.list
            .map { it.formattedMessage }
            .single { it.startsWith("Central Micrometer observation disable-filter") }

    /** The single module-toggle log line emitted by the factory's init block. */
    private fun moduleToggleLine(): String =
        logAppender.list
            .map { it.formattedMessage }
            .single { it.startsWith("WebClient HTTP observation module toggle") }

    /**
     * The module-toggle line must say what the toggle ACHIEVES, not merely what it asks for.
     *
     * The distinction is invisible at runtime and was, until this group existed, invisible in the log too: with
     * no `ObservationRegistry` bean the host's `ObservationWebClientCustomizer` never runs (it is
     * `@ConditionalOnBean` on one), so a toggle reported as plain "on" produces exactly what "off" produces.
     */
    @Nested
    inner class `What the toggle achieves` {
        @Test
        fun `should report a plain on when a recording ObservationRegistry is behind the toggle`() {
            // Given: the toggle on AND a registry that will actually record - i.e. one with a handler
            // When
            newFactory(
                observationEnabled = true,
                environment = MockEnvironment(),
                observationRegistry = recordingRegistry(),
            )

            // Then: the unqualified "on" - Boot's registry is on the builder and stays there
            assertThat(moduleToggleLine())
                .contains("http-observation-enabled=true")
                .contains("on (Boot's registry stays on the builder)")
                .doesNotContain("nothing is observed")
        }

        @Test
        fun `should report on but nothing observed when the host has no ObservationRegistry bean`() {
            // Given: the toggle on, no registry bean at all - the state of any host that did not switch
            // Micrometer observation on, and the state this module's own hand-assembled test contexts are in
            // When
            newFactory(observationEnabled = true, environment = MockEnvironment(), observationRegistry = null)

            // Then: still reported as configured `=true`, but the line says the toggle buys nothing
            assertThat(moduleToggleLine())
                .contains("http-observation-enabled=true")
                .contains("NO ObservationRegistry bean")
                .contains("nothing is observed (no span, no timer, no traceparent)")
                .doesNotContain("stays on the builder")
        }

        @Test
        fun `should report on but nothing observed when the registry is the NOOP instance`() {
            // Given: a registry bean exists but is Micrometer's NOOP
            // When
            newFactory(
                observationEnabled = true,
                environment = MockEnvironment(),
                observationRegistry = ObservationRegistry.NOOP,
            )

            // Then
            assertThat(moduleToggleLine())
                .contains("http-observation-enabled=true")
                .contains("records nothing")
                .doesNotContain("stays on the builder")
        }

        @Test
        fun `should report on but nothing observed when a real registry has no ObservationHandler`() {
            // What is tested: the case a bean-PRESENCE check would get wrong. `ObservationRegistry.create()`
            //   returns a genuine SimpleObservationRegistry - not the NOOP, not null - which nonetheless records
            //   nothing until a handler is added.
            // Why it matters: Micrometer defines `SimpleObservationRegistry.isNoop()` as
            //   `this == NOOP || observationConfig().getObservationHandlers().isEmpty()`, so "a registry exists"
            //   and "something will be recorded" are NOT the same question. The factory must answer the second
            //   one, because that is the one an operator hunting a missing span is actually asking.
            //
            // Given: a real registry with no handler registered
            // When
            newFactory(
                observationEnabled = true,
                environment = MockEnvironment(),
                observationRegistry = ObservationRegistry.create(),
            )

            // Then: reported as recording nothing, with the handler named as a possible cause
            assertThat(moduleToggleLine())
                .contains("http-observation-enabled=true")
                .contains("records nothing")
                .contains("no ObservationHandler")
                .doesNotContain("stays on the builder")
        }

        @Test
        fun `should report off when the toggle is off even though a recording registry exists`() {
            // Given: a perfectly good registry, toggle off - the factory replaces it with NOOP for ITS clients
            // while every other WebClient of the host keeps observing. The line must name that, not the registry.
            // When
            newFactory(
                observationEnabled = false,
                environment = MockEnvironment(),
                observationRegistry = recordingRegistry(),
            )

            // Then
            assertThat(moduleToggleLine())
                .contains("http-observation-enabled=false")
                .contains("off (registry replaced by NOOP)")
        }

        /**
         * A registry that reports itself as live: a plain [ObservationRegistry.create] would not, because
         * Micrometer counts a handler-less registry as noop (see the handler case above). The handler accepts
         * every context and does nothing - what it RECORDS is irrelevant here, only that one is present.
         */
        private fun recordingRegistry(): ObservationRegistry =
            ObservationRegistry.create().apply {
                observationConfig().observationHandler(ObservationHandler<Observation.Context> { true })
            }
    }

    private fun newFactory(
        observationEnabled: Boolean,
        environment: MockEnvironment,
        observationRegistry: ObservationRegistry? = null,
    ): ResilientWebClientFactory {
        val resilience =
            ReactiveResilienceDecorator(
                CircuitBreakerRegistry.of(CircuitBreakerConfig.ofDefaults()),
                TimeLimiterRegistry.of(TimeLimiterConfig.ofDefaults()),
                BulkheadRegistry.of(BulkheadConfig.ofDefaults()),
            )
        val meshProperties =
            MeshProperties(
                defaults = MeshProperties.Defaults(individualRequestTimeoutMs = 1000),
                endpoints =
                    listOf(
                        MeshProperties.EndpointConfig(
                            triplet = "/endpoint/localhost/GET/backend",
                            alias = "backend",
                            baseUrl = "http://localhost:1",
                        ),
                    ),
            )
        val properties =
            WebClientProperties(observability = WebClientProperties.Observability(httpObservationEnabled = observationEnabled))
        return ResilientWebClientFactory(
            properties,
            meshProperties,
            resilience,
            JsonMapper.builder().build(),
            environment = environment,
            observationRegistry = observationRegistry,
            // Deliberately no Spring context here: a bare builder stands in for Boot's bean.
            webClientBuilder = WebClient.builder(),
        )
    }
}
