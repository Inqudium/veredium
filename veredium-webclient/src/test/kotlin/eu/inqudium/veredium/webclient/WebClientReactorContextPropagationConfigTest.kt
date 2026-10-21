package eu.inqudium.veredium.webclient

import io.micrometer.context.ContextRegistry
import io.micrometer.context.ThreadLocalAccessor
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import reactor.core.publisher.Hooks
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.time.Duration

/**
 * Tests for [WebClientReactorContextPropagationConfig], the one place this module changes JVM-wide state:
 * Reactor's automatic context propagation, switched on with the HTTP observation toggle and only then.
 *
 * The hook is process-global, so every test here saves Reactor's state before it runs and puts it back after
 * ([Hooks.isAutomaticContextPropagationEnabled] is the public read side of the switch). The ThreadLocal accessor
 * the round-trip uses is registered for the duration of one test and removed again, so nothing of this class
 * reaches another test class in the same JVM.
 */
class WebClientReactorContextPropagationConfigTest {
    private var propagationBefore = false

    @BeforeEach
    fun saveReactorState() {
        propagationBefore = Hooks.isAutomaticContextPropagationEnabled()
        // Each test states its own premise: the hook OFF, whatever ran before in this JVM.
        Hooks.disableAutomaticContextPropagation()
    }

    @AfterEach
    fun restoreReactorState() {
        if (propagationBefore) Hooks.enableAutomaticContextPropagation() else Hooks.disableAutomaticContextPropagation()
    }

    @Test
    fun `should enable Reactor's automatic context propagation when HTTP observation is on`() {
        // What is tested: the switch's ON branch, and what it buys - a ThreadLocal value that crosses a thread
        //   hop inside a Reactor pipeline.
        // Success criteria: after constructing the configuration with `http-observation-enabled=true`, Reactor
        //   reports automatic propagation as enabled, and a value written into the Reactor Context arrives in a
        //   ThreadLocal read by a plain `map` on another thread.
        // Why it matters: the observation's "current observation" is a ThreadLocal; without the hook the
        //   `http.client.requests` span starts a fresh root on the event loop instead of nesting under the
        //   caller's trace. The integration tests run without this configuration, i.e. in a different mode than
        //   production with observation on - this is the one test that runs the production branch.
        //
        // Given: the toggle on
        val properties = WebClientProperties(observability = WebClientProperties.Observability(httpObservationEnabled = true))

        // When
        WebClientReactorContextPropagationConfig(properties)

        // Then
        assertThat(Hooks.isAutomaticContextPropagationEnabled()).isTrue()
        assertThat(threadLocalAfterHop()).`as`("the Reactor Context value restored into the ThreadLocal").isEqualTo(VALUE)
    }

    @Test
    fun `should leave Reactor's context propagation alone when HTTP observation is off`() {
        // What is tested: the OFF branch - the code default - changes nothing JVM-wide.
        // Success criteria: after constructing the configuration with the toggle off, Reactor still reports
        //   automatic propagation as disabled, and the same round-trip as above sees no value on the other
        //   thread - which is also what makes the ON test's round-trip evidence of the hook rather than of the
        //   accessor alone.
        // Why it matters: the hook cannot be undone by the library and changes how EVERY Reactor pipeline of the
        //   host treats ThreadLocals. A host that did not opt into the observation must not get that as a side
        //   effect of having this module on its classpath.
        //
        // Given: the toggle at its code default (off)
        val properties = WebClientProperties()

        // When
        WebClientReactorContextPropagationConfig(properties)

        // Then
        assertThat(Hooks.isAutomaticContextPropagationEnabled()).isFalse()
        assertThat(threadLocalAfterHop()).isNull()
    }

    /**
     * Writes [VALUE] into the Reactor Context of a pipeline that hops onto a fresh thread, and returns
     * what the ThreadLocal holds there in a plain `map` - restored only by automatic propagation (a `handle` or
     * `tap` would restore it even without the hook). The accessor that teaches Micrometer's context
     * propagation about the ThreadLocal is registered for the call and removed after.
     */
    private fun threadLocalAfterHop(): String? {
        val registry = ContextRegistry.getInstance()
        registry.registerThreadLocalAccessor(ProbeAccessor)
        // A fresh thread per round-trip: a pooled worker could still hold the value from an earlier one.
        val hop = Schedulers.newSingle("propagation-probe")
        try {
            return Mono
                .just("probe")
                .publishOn(hop)
                .map { PROBE.get() ?: NONE }
                .contextWrite { it.put(KEY, VALUE) }
                .block(Duration.ofSeconds(5))
                .takeUnless { it == NONE }
        } finally {
            hop.dispose()
            registry.removeThreadLocalAccessor(KEY)
            PROBE.remove()
        }
    }

    /** A ThreadLocalAccessor over [PROBE], the shape a host registers per key it wants carried. */
    private object ProbeAccessor : ThreadLocalAccessor<String> {
        override fun key(): Any = KEY

        override fun getValue(): String? = PROBE.get()

        override fun setValue(value: String) = PROBE.set(value)

        override fun setValue() = PROBE.remove()
    }

    private companion object {
        const val KEY = "veredium.test.propagation-probe"
        const val VALUE = "carried"

        /** What the `map` returns when the ThreadLocal is empty - a Mono cannot emit null. */
        const val NONE = "<none>"
        val PROBE = ThreadLocal<String>()
    }
}
