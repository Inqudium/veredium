package eu.inqudium.veredium.core.observation

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
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

/**
 * Verifies the two observation lines [HttpObservationReport] writes at startup - once, for both stacks, which
 * subclass it with nothing but their name and their property key (`RestClientObservationReport`,
 * `WebClientObservationReport`; each stack's own test pins that pair).
 *
 * The report reads and logs `management.observations.enable.all` and
 * `management.observations.enable.http.client.requests` - the ONLY central Spring switch, a name-prefixed
 * DEACTIVATION filter (default true, NOT an enable switch) - and flags when a `false` there suppresses the client
 * observation even though the module toggle is on. Its module-toggle line reports what the toggle ACHIEVES,
 * distinguishing an `on` with a live `ObservationRegistry` behind it from an `on` with no registry at all.
 *
 * Both switches fail SILENTLY and in the same direction - no span, no timer, nothing in the log to say why. The
 * central switch is a DISABLE-only, prefix-matched override that is easy to miss; and a toggle turned on in a
 * host with no `ObservationRegistry` bean is indistinguishable at runtime from one left off. Reporting either as
 * a plain "on" is what sends someone hunting through Micrometer configuration for a span the process cannot
 * create - so the tests below pin the effective values, the "SUPPRESSED centrally" note and the registry's
 * actual state line by line.
 */
class HttpObservationReportTest {
    private val reportLogger = LoggerFactory.getLogger(HttpObservationReport::class.java) as Logger
    private val logAppender = ListAppender<ILoggingEvent>()

    @BeforeEach
    fun attachAppender() {
        logAppender.start()
        reportLogger.addAppender(logAppender)
    }

    @AfterEach
    fun detachAppender() {
        reportLogger.detachAppender(logAppender)
    }

    @Test
    fun `should log the central disable-filter as true by default when the properties are unset`() {
        // What is tested: the disable-filter line `HttpObservationReport` logs at construction when neither
        //   `management.observations.enable.all` nor `...enable.http.client.requests` is set.
        // Success criteria: the line reports both switches as `=true` (Spring's default for this deactivation
        //   filter) and carries no "SUPPRESSED" note.
        // Why it matters: the central switch is a DISABLE-only filter that defaults to true; logging an unset
        //   property as anything but its effective value would mislead an operator into a non-existent cause.
        //
        // Given: no management.observations.enable.* properties set, module toggle on
        // When: the report is constructed (it logs in its init block)
        newReport(observationEnabled = true, environment = MockEnvironment())

        // Then: the disable-filter line reports both switches as their default true, and does NOT suppress
        val line = disableFilterLine()
        assertThat(line).contains("management.observations.enable.all=true")
        assertThat(line).contains("management.observations.enable.http.client.requests=true")
        assertThat(line).doesNotContain("SUPPRESSED")
    }

    @Test
    fun `should log http-client-requests false and flag central suppression when that switch is off`() {
        // What is tested: the disable-filter line when `management.observations.enable.http.client.requests`
        //   is false while the module toggle is on.
        // Success criteria: the line shows `...http.client.requests=false` and the warning that
        //   'http.client.requests' is SUPPRESSED centrally despite the module toggle being on.
        // Why it matters: this prefix-matched switch silently drops every client observation the module wires;
        //   without the note the only symptom is a span that never appears, with nothing in the log saying why.
        //
        // Given: the client observation is centrally disabled while the module toggle is on
        val environment =
            MockEnvironment().withProperty("management.observations.enable.http.client.requests", "false")

        // When
        newReport(observationEnabled = true, environment = environment)

        // Then: the line shows the false value AND warns the wired observation is suppressed centrally
        val line = disableFilterLine()
        assertThat(line).contains("management.observations.enable.http.client.requests=false")
        assertThat(line).contains("'http.client.requests' is SUPPRESSED centrally despite the module toggle being on")
    }

    @Test
    fun `should log all false and flag central suppression when the global switch is off`() {
        // What is tested: the disable-filter line when the global `management.observations.enable.all` is
        //   false while the module toggle is on.
        // Success criteria: the line shows `management.observations.enable.all=false` and carries the
        //   "SUPPRESSED centrally" note.
        // Why it matters: the global switch suppresses the client observation just as the specific one does;
        //   the report must flag either switch, not only the one with the client's own name.
        //
        // Given: ALL observations are centrally disabled while the module toggle is on
        val environment = MockEnvironment().withProperty("management.observations.enable.all", "false")

        // When
        newReport(observationEnabled = true, environment = environment)

        // Then: the global switch reads false and the suppression note is present
        val line = disableFilterLine()
        assertThat(line).contains("management.observations.enable.all=false")
        assertThat(line).contains("SUPPRESSED centrally")
    }

    @Test
    fun `should not flag suppression when the module toggle is off regardless of the central defaults`() {
        // What is tested: the disable-filter line with the module toggle off and the central switches unset.
        // Success criteria: the line is still logged with `management.observations.enable.all=true` but
        //   carries no "SUPPRESSED" note.
        // Why it matters: suppression can only be reported for an observation the module actually wires; a
        //   note on a toggled-off client would be a false alarm that teaches operators to ignore the warning.
        //
        // Given: module toggle off; the central filter defaults are irrelevant because nothing is wired
        // When
        newReport(observationEnabled = false, environment = MockEnvironment())

        // Then: the disable-filter line is still logged (for visibility) but carries no suppression note
        val line = disableFilterLine()
        assertThat(line).contains("management.observations.enable.all=true")
        assertThat(line).doesNotContain("SUPPRESSED")
    }

    /**
     * The module-toggle line must say what the toggle ACHIEVES, not merely what it asks for.
     *
     * The distinction is invisible at runtime, so the log line is the only place it shows: with no
     * `ObservationRegistry` bean the host's `ObservationRestClientCustomizer` never runs (it is
     * `@ConditionalOnBean` on one), so a toggle reported as plain "on" produces exactly what "off" produces.
     */
    @Nested
    inner class `What the toggle achieves` {
        @Test
        fun `should report a plain on when a recording ObservationRegistry is behind the toggle`() {
            // What is tested: the module-toggle line with the toggle on and a registry that has an
            //   ObservationHandler, i.e. one that will actually record.
            // Success criteria: the line names `http-observation-enabled=true` and the plain "on (Boot's
            //   registry stays on the builder)", without any "nothing is observed" qualification.
            // Why it matters: the healthy case must read as healthy; a qualification here would make the
            //   warning of the degraded cases below meaningless.
            //
            // Given: the toggle on AND a registry that will actually record - i.e. one with a handler
            // When
            newReport(
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
            // What is tested: the module-toggle line with the toggle on and no ObservationRegistry bean
            //   (`observationRegistry = null`).
            // Success criteria: the line still shows the configured `=true` but says "NO ObservationRegistry
            //   bean" and "nothing is observed (no span, no timer, no traceparent)", not "stays on the builder".
            // Why it matters: without a registry bean the host's observation customizer never runs, so the
            //   toggle produces exactly what "off" produces; a plain "on" would send someone hunting through
            //   Micrometer configuration for a span the process cannot create.
            //
            // Given: the toggle on, no registry bean at all - the state of any host that did not switch
            // Micrometer observation on, and the state this module's own hand-assembled test contexts are in
            // When
            newReport(observationEnabled = true, environment = MockEnvironment(), observationRegistry = null)

            // Then: still reported as configured `=true`, but the line says the toggle buys nothing
            assertThat(moduleToggleLine())
                .contains("http-observation-enabled=true")
                .contains("NO ObservationRegistry bean")
                .contains("nothing is observed (no span, no timer, no traceparent)")
                .doesNotContain("stays on the builder")
        }

        @Test
        fun `should report on but nothing observed when the registry is the NOOP instance`() {
            // What is tested: the module-toggle line with the toggle on and `ObservationRegistry.NOOP` as the bean.
            // Success criteria: the line shows `http-observation-enabled=true` and "records nothing", not
            //   "stays on the builder".
            // Why it matters: a NOOP bean is present, so a bean-presence check would report "on"; the report must
            //   judge by what the registry records, which for NOOP is nothing.
            //
            // Given: a registry bean exists but is Micrometer's NOOP
            // When
            newReport(
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
            // Success criteria: the line shows `http-observation-enabled=true`, "records nothing" and names
            //   "no ObservationHandler" as the cause, not "stays on the builder".
            // Why it matters: Micrometer defines `SimpleObservationRegistry.isNoop()` as
            //   `this == NOOP || observationConfig().getObservationHandlers().isEmpty()`, so "a registry exists"
            //   and "something will be recorded" are NOT the same question. The report must answer the second
            //   one, because that is the one an operator hunting a missing span is actually asking.
            //
            // Given: a real registry with no handler registered
            // When
            newReport(
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
            // What is tested: the module-toggle line with the toggle off while a recording registry exists.
            // Success criteria: the line shows `http-observation-enabled=false` and "off (registry replaced by
            //   NOOP)".
            // Why it matters: the state of the registry must not mask the toggle - an operator who switched the
            //   stack's observation off has to see that its clients are deliberately unobserved.
            //
            // Given: a perfectly good registry, toggle off - the stack's factory replaces it with NOOP for ITS
            // clients while every other client of the host keeps observing. The line must name that, not the registry.
            // When
            newReport(
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

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /** The single central disable-filter log line emitted by the report's init block. */
    private fun disableFilterLine(): String =
        logAppender.list
            .map { it.formattedMessage }
            .single { it.startsWith("Central Micrometer observation disable-filter") }

    /** The single module-toggle log line emitted by the report's init block. */
    private fun moduleToggleLine(): String =
        logAppender.list
            .map { it.formattedMessage }
            .single { it.startsWith("Stack HTTP observation module toggle") }

    private fun newReport(
        observationEnabled: Boolean,
        environment: MockEnvironment,
        observationRegistry: ObservationRegistry? = null,
    ): HttpObservationReport =
        HttpObservationReport(
            stackName = "Stack",
            toggleProperty = "app.stack.observability.http-observation-enabled",
            observationEnabled = observationEnabled,
            environment = environment,
            observationRegistry = observationRegistry,
        )
}
