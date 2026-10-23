# ADR-0003: Each mesh endpoint is owned by one client stack, so both stacks can share a context

**Status:** Proposed  
**Date:** 2026-10-23  
**Deciders:** Dirk Haase (maintainer)  
**Related:** ADR-0001 (the shared mesh wiring both stacks import, which
this ADR makes stack-aware)

## Context

ADR-0001 gave both client stacks one mesh wiring in `veredium-core`. It was
built on an assumption it never stated: one client stack per Spring
context. A host with both client modules on its classpath - each brings
its auto-configuration - does not start unless it excludes one of them.

Until the verdicts became an object (PR #17), the first failure was the
`breakerVerdicts` bean both stacks declared. On the current code the
start fails one step later:

```text
The bean 'meshCircuitBreakerCustomizer.customers' could not be registered.
A bean with that name has already been defined and overriding is disabled.
```

The name clash is only the symptom. Underneath it, every part of the
mesh wiring serves **every** endpoint of `app.mesh.endpoints`:

- **Customizers.** Each stack's subclass of `MeshResilienceRegistrar`
  registers one circuit-breaker customizer per alias, with its own
  stack's verdicts. A Resilience4j instance can take one customizer only,
  and the verdicts differ between the stacks: which exception carries a
  status (`RestClientResponseException` or `WebClientResponseException`),
  and which failures the client raises itself (the blocking stack adds
  the caller's `InterruptedException`).
- **The guard.** Each stack registers a `MeshResilienceStartupGuard` that
  checks all aliases with its own stack's probe. It also compares the
  bulkheads of all aliases with the pools of all factories.
- **The factories.** `ResilientRestClientFactory` and
  `ResilientWebClientFactory` both build their transports, engine checks,
  validation and startup lines from `meshProperties.resolveAll()`.
  Together they would hold two pools for every backend.

Bean overriding (`spring.main.allow-bean-definition-overriding=true`) is
no way out: the last customizer for an alias wins, so one stack's breakers
would run on the other stack's verdicts.

Most hosts use one stack, and they are not affected. A host that does
need both - a reactive service with one blocking SDK-style backend, or a
migration from one stack to the other one backend at a time - today has
to run two contexts or give one of the backends up.

The options:

1. **Keep one stack per context and document it.** Add a check that
   fails the start with a clear message when both stacks are present.
2. **Choose the verdicts per alias through a list of stack beans.** Each
   stack declares its verdicts as a bean again, and the core resolves
   them per alias. This undoes PR #17: the verdicts would once more be
   replaceable through the context.
3. **Make each endpoint owned by exactly one stack.** Each stack's
   wiring serves only the endpoints it owns, and the core checks the
   ownership before any registry exists.

## Decision

We adopt option 3: **every mesh endpoint is owned by exactly one client
stack.** The stack's registrar, guard and factory serve the endpoints
that stack owns, and nothing else.

### Configuration

`app.mesh.endpoints[]` gains an optional `stack`, `restclient` or
`webclient`:

```yaml
app:
  mesh:
    endpoints:
      - triplet: "/endpoint/customers.example.com/GET/v1/customers/{id}"
        alias: customers
        stack: restclient
      - triplet: "/endpoint/orders.example.com/POST/v1/orders/search"
        alias: order-search
        stack: webclient
```

The ownership rule, implemented once in the core and used by every part
of the wiring:

| `stack` of the endpoint | One stack in the context | Both stacks in the context |
|---|---|---|
| not set | owned by that stack | the start fails: `stack` is required |
| set to a stack in the context | owned by it | owned by it |
| set to a stack not in the context | the start fails | the start fails |

A host with one stack and no `stack` keys changes nothing. A `stack` that
names the wrong stack fails the start instead of leaving the backend
unserved.

```kotlin
// veredium-core, eu.inqudium.veredium.core.mesh
fun MeshProperties.endpointsOwnedBy(stack: String): Map<String, ResolvedTimeouts> =
    resolveAll().values.filter { it.stack == null || it.stack == stack }.associateBy { it.alias }
```

`ResolvedTimeouts` carries the `stack` of its `EndpointConfig`. The stack
names are the stacks' own constants (`RestClientMeshRegistrar.STACK`,
`WebClientMeshRegistrar.STACK`); the core treats them as opaque strings.

### Registrar: the stack is part of every bean name

`MeshResilienceRegistrar` takes the stack name next to the verdicts and
registers only what its stack owns, under names that carry the stack:

```kotlin
abstract class MeshResilienceRegistrar(
    private val stack: String,
    private val verdicts: BreakerVerdicts,
) : BeanRegistrar

// meshCircuitBreakerCustomizer.<stack>.<alias>
// meshTimeLimiterCustomizer.<stack>.<alias>
// meshResilienceStartupGuard.<stack>
```

A Spring registrar cannot see what another registrar registers, so a
single registrar cannot decide on its own whether it is the only stack in
the context. Each one therefore claims the endpoints with no `stack` as
well. With two stacks, such an endpoint is claimed twice, and the check
below rejects it.

### The ownership check runs before any registry exists

`MeshResilienceConfiguration` declares a static `BeanFactoryPostProcessor`,
the **ownership check**. It runs after every bean definition is
registered and before any singleton is created, so it fails the start
before Resilience4j builds a registry or a factory builds a pool.

It reads only bean definition **names**:

- the stacks present come from the `meshResilienceStartupGuard.<stack>`
  definitions;
- the claims come from the `meshCircuitBreakerCustomizer.<stack>.<alias>`
  definitions, one per stack and owned alias.

It fails the start, with all problems collected in one message, when:

- an alias is claimed by more than one stack - the message names the key
  to set, `app.mesh.endpoints[<alias>].stack`, and the stacks present;
- an endpoint is claimed by no stack - its `stack` names a stack that is
  not in the context.

Nothing is instantiated for the check, and the verdicts are never read
through the context. That keeps what PR #17 achieved: no bean of a host
can replace a stack's verdicts.

### Guard and factories

- **Guard.** Each stack's `MeshResilienceStartupGuard` checks the aliases
  its stack owns, with its own probe. It compares the bulkheads only with
  the pools whose aliases it owns: `MeshConnectionPool` already carries
  its aliases.
- **Factories.** Both factories take their endpoints from
  `endpointsOwnedBy(STACK)` instead of `resolveAll()`. Transports, engine
  checks, validation, pools and startup lines then cover the stack's own
  endpoints only.
  - `create(alias)` for an alias of the other stack fails with an
    `IllegalArgumentException` that names the owning stack and its
    factory.
  - An engine override such as `app.restclient.engine.endpoints.<alias>`
    for an alias of the other stack fails the start, as an unknown alias
    does today; the message names the owning stack.

### What stays as it is

- `MeshProperties` already rejects a repeated triplet. One backend
  therefore cannot be served by both stacks, and this ADR adds no rule for
  that case.
- The decorator beans, the metric tags and the thread names are distinct
  per stack already (`restclient-call-`, `restclient-apache`,
  `webclient-reactor-netty`, ...).
- `app.mesh`, the Resilience4j registries and legatium's exchange log
  are shared by both stacks, as before.

## Consequences

**Positive:**

- A host can run both stacks in one context, each backend on the stack
  that fits it, and can migrate one backend at a time by changing its
  `stack`.
- The breakers of each alias run on the verdicts of the stack that calls
  it, which bean overriding could never guarantee.
- A configuration that names a stack the context does not run fails the
  start instead of leaving a backend unserved until its first call.
- Hosts with one stack are not affected: `stack` is optional for them.

**Negative:**

- The customizer and guard bean names change
  (`meshCircuitBreakerCustomizer.<alias>` becomes
  `meshCircuitBreakerCustomizer.<stack>.<alias>`). A host or test that
  looks up these beans by name has to follow. Recorded in the CHANGELOG.
- The ownership check depends on the bean-name scheme. Renaming a prefix
  without changing the check breaks it. A test that runs both stacks in
  one context has to pin the scheme.
- Each stack holds its own pools to the sidecar.
  `app.mesh.defaults.transport.max-connections` applies per stack, so
  the sidecar can see up to twice as many connections from one host.
  The guides have to say so.
- One more concept for operators: who owns an endpoint. It is invisible
  until a second stack arrives, and then mandatory for every endpoint.

**Neutral:**

- `MeshResilienceRegistrar` gets a second constructor argument. A host
  that uses the core without a client (ADR-0001, "Neutral") picks a
  stack name of its own.
- The stack name is a string, not an enum in the core. The core does not
  know which stacks exist; a third stack would bring its own name.
- Option 1 (one stack per context, documented) stays the fallback if the
  implementation turns out more invasive than this record expects. Its
  check would reuse the ownership check: fail when more than one stack is
  present.

## Implementation status

Nothing is built yet. The work, in order:

1. Core:
   - `EndpointConfig.stack` and `ResolvedTimeouts.stack`;
   - `endpointsOwnedBy`;
   - `MeshResilienceRegistrar(stack, verdicts)` with the new bean names;
   - the ownership check.
2. Guard: filter its aliases and pools by owner.
3. Both factories: `endpointsOwnedBy(STACK)`, and the foreign-alias
   message in `create` and in the engine checks.
4. Tests:
   - core: the ownership table, cell by cell, against the ownership
     check;
   - a context with both stacks, one alias each: a benign 404 is left
     unrecorded on both breakers, and each factory refuses the other's
     alias;
   - the factories' foreign-alias messages;
   - an example application with both stacks in `veredium-examples`.
5. Docs:
   - the core guide and `mesh-timeout.md` (the ownership rule, the
     doubled sidecar connections);
   - both client guides;
   - the CHANGELOG (the new key, the renamed beans);
   - this record set to `Accepted`.
