package eu.inqudium.veredium.core.resilience

import io.github.resilience4j.springboot.bulkhead.autoconfigure.BulkheadAutoConfiguration
import io.github.resilience4j.springboot.circuitbreaker.autoconfigure.CircuitBreakerAutoConfiguration
import io.github.resilience4j.springboot.timelimiter.autoconfigure.TimeLimiterAutoConfiguration
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.core.env.MapPropertySource

/**
 * The ownership rule of ADR-0003 ("each mesh endpoint is owned by one client stack"), cell by cell, in real contexts
 * with one or two stand-in stacks ([TestStackConfiguration], [OtherStackConfiguration]): who serves an endpoint
 * depending on its `stack` and on how many stacks the context runs, and that [MeshOwnershipCheck] fails the start
 * where the answer is "two stacks" or "none".
 *
 * | `stack` of the endpoint | one stack in the context | both stacks in the context |
 * |---|---|---|
 * | not set | owned by that stack | the start fails |
 * | set to a stack in the context | owned by it | owned by it |
 * | set to a stack not in the context | the start fails | the start fails |
 */
class MeshOwnershipCheckTest {
    @Nested
    inner class `One stack in the context` {
        @Test
        fun `should give an endpoint without stack to the one stack`() {
            // What is tested: the cell "not set / one stack".
            // Success criteria: the context starts; the stack declared the endpoint's customizer and its guard.
            // Why it matters: a host with one client module - the common case - never writes a `stack` key, and its
            //   configuration must keep working exactly as before.
            //
            // Given / When
            context(stackOfAccounts = null, TestStackConfiguration::class.java).run { context ->
                // Then
                assertThat(context).hasNotFailed()
                assertThat(context).hasBean(MeshResilienceRegistrar.circuitBreakerBeanName(TestMeshRegistrar.STACK, ACCOUNTS))
                assertThat(context).hasBean(MeshResilienceRegistrar.startupGuardBeanName(TestMeshRegistrar.STACK))
            }
        }

        @Test
        fun `should give an endpoint naming the stack to it`() {
            // What is tested: the cell "set to a stack in the context / one stack".
            // Success criteria: the context starts, the endpoint's customizer is the stack's.
            // Why it matters: a host that already names the stacks - preparing for a second module - must not be
            //   refused while it runs one.
            //
            // Given / When
            context(stackOfAccounts = TestMeshRegistrar.STACK, TestStackConfiguration::class.java).run { context ->
                // Then
                assertThat(context).hasNotFailed()
                assertThat(context).hasBean(MeshResilienceRegistrar.circuitBreakerBeanName(TestMeshRegistrar.STACK, ACCOUNTS))
            }
        }

        @Test
        fun `should fail the start for an endpoint naming a stack that is not in the context`() {
            // What is tested: the cell "set to a stack not in the context / one stack".
            // Success criteria: the start fails with the ownership check's message: the key, the stack it names and
            //   the stacks present.
            // Why it matters: an endpoint nobody serves would otherwise surface only at its first call, as "no mesh
            //   endpoint configured" from a factory that never heard of it.
            //
            // Given / When
            context(stackOfAccounts = "third", TestStackConfiguration::class.java).run { context ->
                // Then
                assertThat(context).hasFailed()
                assertThat(rootCauseOf(context.startupFailure))
                    .isInstanceOf(IllegalStateException::class.java)
                    .hasMessageContainingAll("'$ACCOUNTS'", "served by no stack", "app.mesh.endpoints[0].stack is 'third'", "[test]")
            }
        }
    }

    @Nested
    inner class `Both stacks in the context` {
        @Test
        fun `should fail the start for an endpoint without stack, naming the key to set`() {
            // What is tested: the cell "not set / both stacks".
            // Success criteria: the start fails with the ownership check's message naming the endpoint, both stacks
            //   and the key `app.mesh.endpoints[0].stack`.
            // Why it matters: both stacks claim the endpoint; its breaker would get two customizers with two stacks'
            //   verdicts. The check turns that into one message that says what to write, before any registry or pool
            //   exists.
            //
            // Given / When
            context(stackOfAccounts = null, TestStackConfiguration::class.java, OtherStackConfiguration::class.java).run { context ->
                // Then
                assertThat(context).hasFailed()
                assertThat(rootCauseOf(context.startupFailure))
                    .isInstanceOf(IllegalStateException::class.java)
                    .hasMessageContainingAll("'$ACCOUNTS'", "claimed by the stacks [other, test]", "app.mesh.endpoints[0].stack must name")
            }
        }

        @Test
        fun `should give each endpoint to the stack it names, and to no other`() {
            // What is tested: the cell "set to a stack in the context / both stacks", one endpoint per stack.
            // Success criteria: the context starts; each stack declared the customizer of its own endpoint only, and
            //   each has its own guard.
            // Why it matters: this is what ADR-0003 is for - both client modules in one host, each backend on the stack
            //   that fits it, each breaker on the verdicts of the stack that calls it.
            //
            // Given / When
            context(stackOfAccounts = TestMeshRegistrar.STACK, TestStackConfiguration::class.java, OtherStackConfiguration::class.java, stackOfPayments = OtherMeshRegistrar.STACK)
                .run { context ->
                    // Then
                    assertThat(context).hasNotFailed()
                    assertThat(context).hasBean(MeshResilienceRegistrar.circuitBreakerBeanName(TestMeshRegistrar.STACK, ACCOUNTS))
                    assertThat(context).hasBean(MeshResilienceRegistrar.circuitBreakerBeanName(OtherMeshRegistrar.STACK, PAYMENTS))
                    assertThat(context).doesNotHaveBean(MeshResilienceRegistrar.circuitBreakerBeanName(OtherMeshRegistrar.STACK, ACCOUNTS))
                    assertThat(context).doesNotHaveBean(MeshResilienceRegistrar.circuitBreakerBeanName(TestMeshRegistrar.STACK, PAYMENTS))
                    assertThat(context).hasBean(MeshResilienceRegistrar.startupGuardBeanName(TestMeshRegistrar.STACK))
                    assertThat(context).hasBean(MeshResilienceRegistrar.startupGuardBeanName(OtherMeshRegistrar.STACK))
                }
        }

        @Test
        fun `should fail the start for an endpoint naming a stack that is not in the context`() {
            // What is tested: the cell "set to a stack not in the context / both stacks".
            // Success criteria: the start fails, naming the endpoint, the key with its value and both stacks present.
            // Why it matters: a typo in `stack` must not leave a backend unserved in a host that runs both modules.
            //
            // Given / When
            context(stackOfAccounts = "restclinet", TestStackConfiguration::class.java, OtherStackConfiguration::class.java, stackOfPayments = OtherMeshRegistrar.STACK)
                .run { context ->
                    // Then
                    assertThat(context).hasFailed()
                    assertThat(rootCauseOf(context.startupFailure))
                        .isInstanceOf(IllegalStateException::class.java)
                        .hasMessageContainingAll("'$ACCOUNTS'", "app.mesh.endpoints[0].stack is 'restclinet'", "[other, test]")
                        .hasMessageNotContaining("'$PAYMENTS'")
                }
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /**
     * A context with Resilience4j's three auto-configurations, the given stand-in stacks and two endpoints, `accounts`
     * with [stackOfAccounts] and `payments` with [stackOfPayments] - each declared under all three layers, so the
     * guards are satisfied wherever the ownership is.
     */
    private fun context(
        stackOfAccounts: String?,
        vararg stacks: Class<*>,
        stackOfPayments: String? = stackOfAccounts,
    ): ApplicationContextRunner {
        val properties =
            buildMap<String, Any> {
                put("app.mesh.endpoints[0].triplet", "/endpoint/api.example.com/GET/accounts/{accountId}")
                put("app.mesh.endpoints[0].alias", ACCOUNTS)
                stackOfAccounts?.let { put("app.mesh.endpoints[0].stack", it) }
                put("app.mesh.endpoints[1].triplet", "/endpoint/api.example.com/POST/payments")
                put("app.mesh.endpoints[1].alias", PAYMENTS)
                stackOfPayments?.let { put("app.mesh.endpoints[1].stack", it) }
                listOf(ACCOUNTS, PAYMENTS).forEach { alias ->
                    put("resilience4j.circuitbreaker.instances.$alias.sliding-window-size", 10)
                    put("resilience4j.timelimiter.instances.$alias.cancel-running-future", true)
                    put("resilience4j.bulkhead.instances.$alias.max-concurrent-calls", 10)
                }
            }
        return ApplicationContextRunner()
            .withInitializer { context -> context.environment.propertySources.addFirst(MapPropertySource("ownership", properties)) }
            .withConfiguration(
                AutoConfigurations.of(
                    TimeLimiterAutoConfiguration::class.java,
                    CircuitBreakerAutoConfiguration::class.java,
                    BulkheadAutoConfiguration::class.java,
                ),
            ).withUserConfiguration(*stacks)
    }

    /** The innermost cause - the check's own exception, however Spring wrapped it on the way out of refresh. */
    private fun rootCauseOf(failure: Throwable?): Throwable = generateSequence(failure) { it.cause.takeIf { cause -> cause !== it } }.last()

    private companion object {
        const val ACCOUNTS = "accounts"
        const val PAYMENTS = "payments"
    }
}
