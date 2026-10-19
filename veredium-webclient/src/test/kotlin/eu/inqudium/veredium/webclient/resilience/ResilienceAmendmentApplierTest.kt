package eu.inqudium.veredium.webclient.resilience

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.core.registry.EntryAddedEvent
import io.github.resilience4j.core.registry.EntryRemovedEvent
import io.github.resilience4j.core.registry.EntryReplacedEvent
import io.github.resilience4j.core.registry.RegistryEventConsumer
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Optional
import java.util.concurrent.atomic.AtomicInteger

/**
 * Tests for [ResilienceAmendmentApplier]: collecting several contributions and applying them once per instance.
 *
 * The property that motivates the class is not "the config is right" - [RegistryAmendmentsTest] covers the fold
 * itself - but "the instance is recreated ONCE, and the result does not depend on contribution order". Both are
 * invisible in normal use and only surface as a re-bound metric or an ordering-dependent config.
 */
class ResilienceAmendmentApplierTest {
    @Test
    fun `should recreate an instance once even when several contributors target it`() {
        // What is tested: that N contributions to the same alias cost ONE registration, not N.
        // How success is judged: a registry event consumer counts entry-added events for the alias. Two
        //   contributors touch it; exactly one addition is expected (the recreate after the single remove).
        // Why this case is important: every recreate makes the metric and health binders re-bind to a new
        //   instance - a gauge briefly pointing at an object nobody uses, and a needless churn at startup that
        //   grows with each policy added. Counting is the only way to see it; the resulting CONFIG looks
        //   identical either way, which is exactly why this went unnoticed while two registrars each wrote.
        //
        // Given a registry that counts additions for the alias
        val additions = AtomicInteger()
        val registry =
            CircuitBreakerRegistry.of(
                CircuitBreakerConfig.ofDefaults(),
                object : RegistryEventConsumer<CircuitBreaker> {
                    override fun onEntryAddedEvent(event: EntryAddedEvent<CircuitBreaker>) {
                        if (event.addedEntry.name == "backend") additions.incrementAndGet()
                    }

                    override fun onEntryRemovedEvent(event: EntryRemovedEvent<CircuitBreaker>) = Unit

                    override fun onEntryReplacedEvent(event: EntryReplacedEvent<CircuitBreaker>) = Unit
                },
            )

        // When two contributors amend the same alias
        ResilienceAmendmentApplier.applyOnce(
            registry,
            TimeLimiterRegistry.ofDefaults(),
            { plan -> plan.circuitBreaker("backend") { slowCallDurationThreshold(Duration.ofMillis(700)) } },
            { plan -> plan.circuitBreaker("backend") { failureRateThreshold(90f) } },
        )

        // Then the instance was created exactly once, carrying both changes
        assertThat(additions).hasValue(1)
        val config = registry.circuitBreaker("backend").circuitBreakerConfig
        assertThat(config.slowCallDurationThreshold).isEqualTo(Duration.ofMillis(700))
        assertThat(config.failureRateThreshold).isEqualTo(90f)
    }

    @Test
    fun `should reach both registries and leave untouched aliases alone`() {
        // Given contributions to a circuit breaker and to a DIFFERENT alias' time limiter
        val circuitBreakers = CircuitBreakerRegistry.ofDefaults()
        val timeLimiters = TimeLimiterRegistry.ofDefaults()
        timeLimiters.timeLimiter("untouched", TimeLimiterConfig.custom().timeoutDuration(Duration.ofSeconds(9)).build())

        // When
        ResilienceAmendmentApplier.applyOnce(
            circuitBreakers,
            timeLimiters,
            ResilienceAmendmentContributor { plan ->
                plan.circuitBreaker("backend") { failureRateThreshold(42f) }
                plan.timeLimiter("backend") { timeoutDuration(Duration.ofMillis(150)) }
            },
        )

        // Then both registries carry the change, and an alias nobody contributed to is untouched
        assertThat(circuitBreakers.circuitBreaker("backend").circuitBreakerConfig.failureRateThreshold).isEqualTo(42f)
        assertThat(timeLimiters.timeLimiter("backend").timeLimiterConfig.timeoutDuration).isEqualTo(Duration.ofMillis(150))
        assertThat(timeLimiters.timeLimiter("untouched").timeLimiterConfig.timeoutDuration).isEqualTo(Duration.ofSeconds(9))
    }

    @Test
    fun `should produce the same result whichever contributor is asked first`() {
        // What is tested: the property the two production contributors rely on, at the level where it now lives.
        //   Spring hands the applier its contributors in bean-definition order, which nothing promises.
        // How success is judged: the same two contributions, applied in both orders, both yield a config that
        //   carries the threshold AND the added ignore predicate.
        // Why it matters: if it did not hold, one of the two would be silently lost in whichever order the
        //   context happened to produce - and the tests of each contributor in isolation would still be green.
        //
        // Given a threshold-setting and a predicate-adding contribution
        val threshold =
            ResilienceAmendmentContributor { plan ->
                plan.circuitBreaker("backend") { slowCallDurationThreshold(Duration.ofMillis(700)) }
            }
        val ignore =
            ResilienceAmendmentContributor { plan ->
                plan.circuitBreaker("backend") { base ->
                    ignoreException { e -> e is ArithmeticException || base.ignoreExceptionPredicate.test(e) }
                }
            }

        // When applied in both orders
        val thresholdFirst = CircuitBreakerRegistry.ofDefaults()
        ResilienceAmendmentApplier.applyOnce(thresholdFirst, TimeLimiterRegistry.ofDefaults(), threshold, ignore)
        val ignoreFirst = CircuitBreakerRegistry.ofDefaults()
        ResilienceAmendmentApplier.applyOnce(ignoreFirst, TimeLimiterRegistry.ofDefaults(), ignore, threshold)

        // Then both configs carry both contributions
        listOf(thresholdFirst, ignoreFirst).forEach { registry ->
            val config = registry.circuitBreaker("backend").circuitBreakerConfig
            assertThat(config.slowCallDurationThreshold).isEqualTo(Duration.ofMillis(700))
            assertThat(config.ignoreExceptionPredicate.test(ArithmeticException())).isTrue()
        }
    }

    /** Runs [block] with this applier's log captured, and hands it the ERROR events it wrote. */
    private fun withErrors(block: (() -> List<ILoggingEvent>) -> Unit) {
        val logger = LoggerFactory.getLogger(ResilienceAmendmentApplier::class.java) as Logger
        val appender = ListAppender<ILoggingEvent>().apply { start() }
        logger.addAppender(appender)
        try {
            block { appender.list.filter { it.level == Level.ERROR } }
        } finally {
            logger.detachAppender(appender)
        }
    }

    private fun ILoggingEvent.fields(): Map<String, String> = keyValuePairs.associate { it.key to it.value.toString() }

    @Test
    fun `should report an amendment aimed at a name this deployment does not have`() {
        // What is tested: the failure that CANNOT be seen after the fact. Registering a config under a free
        //   name CREATES the instance, so amending an alias that does not exist succeeds - onto a phantom
        //   nobody calls. Every after-the-fact check then says the amendment worked.
        // How judged: an ERROR carrying the shared resilience_* fields and the alias, so it lands in the same
        //   query as everything else about this layer.
        // Why it matters: Resilience4j's Spring Boot configuration materialises an instance for every
        //   `instances.<name>` entry when the registry bean is built, so by the time this applier runs every
        //   configured alias exists. A missing one therefore means a misspelling, or an endpoint that is not
        //   configured in this environment - and its policy is then absent exactly where it was meant to apply.
        withErrors { errors ->
            // Given a registry that has never heard of the alias / When
            ResilienceAmendmentApplier.applyOnce(
                CircuitBreakerRegistry.ofDefaults(),
                TimeLimiterRegistry.ofDefaults(),
                ResilienceAmendmentContributor { plan -> plan.circuitBreaker("apis-example-com-POST-typo") { failureRateThreshold(42f) } },
            )

            // Then
            assertThat(errors()).hasSize(1)
            assertThat(errors().single().fields())
                .containsEntry("resilience_event", "amendment_unknown_alias")
                .containsEntry("resilience_layer", "circuitbreaker")
                .containsEntry("resilience_instance", "apis-example-com-POST-typo")
        }
    }

    @Test
    fun `should say nothing when the amendment landed on the instance the registry serves`() {
        // What/why: the quiet path. A check that also fires when everything is fine is one that gets ignored,
        //   and the mesh amendments run on every boot.
        // Given: the alias exists, as it does in production once the instances.<name> entries are materialised
        val registry = CircuitBreakerRegistry.ofDefaults()
        registry.circuitBreaker("apis-example-com-POST-lookup")

        withErrors { errors ->
            // When
            ResilienceAmendmentApplier.applyOnce(
                registry,
                TimeLimiterRegistry.ofDefaults(),
                ResilienceAmendmentContributor { plan -> plan.circuitBreaker("apis-example-com-POST-lookup") { failureRateThreshold(42f) } },
            )

            // Then
            assertThat(errors()).isEmpty()
            assertThat(registry.circuitBreaker("apis-example-com-POST-lookup").circuitBreakerConfig.failureRateThreshold).isEqualTo(42f)
        }
    }

    @Test
    fun `should report an amendment that did not replace the instance`() {
        // What is tested: "Trap 2" from RegistryAmendments - a Resilience4j config is immutable and
        //   `circuitBreaker(name, config)` RETURNS the existing instance rather than replacing it, so
        //   overriding one is only ever a remove + recreate. If the remove does not take effect, the
        //   registration silently keeps the old config.
        // How judged: a registry whose `remove` is a no-op reproduces exactly that, and the applier must catch
        //   it by comparing the config it just built against the one the instance actually holds - by
        //   IDENTITY, which is what makes the check exact and free.
        // Why it matters: without it, dropping or breaking the remove in a future refactor of `amendAll` would
        //   turn every amendment into a no-op, and nothing anywhere would say so.
        val real = CircuitBreakerRegistry.ofDefaults()
        real.circuitBreaker("apis-example-com-POST-lookup")
        // Given: everything real except the removal, which quietly does not happen
        val stubborn =
            object : CircuitBreakerRegistry by real {
                override fun remove(name: String): Optional<CircuitBreaker> = Optional.empty()
            }

        withErrors { errors ->
            // When
            ResilienceAmendmentApplier.applyOnce(
                stubborn,
                TimeLimiterRegistry.ofDefaults(),
                ResilienceAmendmentContributor { plan -> plan.circuitBreaker("apis-example-com-POST-lookup") { failureRateThreshold(42f) } },
            )

            // Then: caught, and the instance indeed kept its old threshold
            assertThat(errors()).hasSize(1)
            assertThat(errors().single().fields()).containsEntry("resilience_event", "amendment_not_applied")
            assertThat(real.circuitBreaker("apis-example-com-POST-lookup").circuitBreakerConfig.failureRateThreshold).isNotEqualTo(42f)
        }
    }

    @Test
    fun `should do nothing when no contributor asks for anything`() {
        // Why this case is important: a context without mesh endpoints is legal, and the applier must then be a
        // no-op rather than materialising instances that nothing configured - creating an instance is itself a
        // decision, and an empty plan is not one.
        //
        // Given / When
        val circuitBreakers = CircuitBreakerRegistry.ofDefaults()
        ResilienceAmendmentApplier.applyOnce(circuitBreakers, TimeLimiterRegistry.ofDefaults())

        // Then
        assertThat(circuitBreakers.allCircuitBreakers).isEmpty()
    }
}
