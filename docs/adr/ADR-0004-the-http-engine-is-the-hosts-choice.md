# ADR-0004: The HTTP engine is the host's choice, on both stacks

**Status:** Accepted  
**Date:** 2026-10-23  
**Deciders:** Dirk Haase (maintainer)  
**Related:** ADR-0001 (the core module, where what both stacks share
about their engines belongs)

## Context

Since commit `1b72625` (PR #8), each client stack lets a host choose
the HTTP engine underneath its clients. The choice is made for all
backends at once and can be overridden per mesh alias:

| Stack | Property | Engines | Default |
|---|---|---|---|
| `veredium-restclient` | `app.restclient.engine` | `apache`, `jdk`, `jetty`, `reactor-netty` | `apache` |
| `veredium-webclient` | `app.webclient.engine` | `reactor-netty`, `jdk`, `jetty`, `apache` | `reactor-netty` |

Each engine has its own transport class per stack, eight in all. Each
class maps the mesh ladder and the pool settings onto its engine and
drains the engine on shutdown. Each one also closes the gaps Spring's
adapters leave around cancellation. What every engine does by itself is
recorded in [HTTP engine behaviour](../engine-behaviour.md).

The feature was added without a record of why it exists. The
architecture review of 2026-10-23
(`docs/assessment/ARCHITECTURE_REVIEW-2026-10-23T22-22-51.md`,
finding 1, High) measured what it costs:

- about a third of the production code;
- about 4 200 lines of tests;
- 13 of 30 commits since the feature was added;
- 34 of 43 findings in the two most recent defect analyses.

Because no record named a need, the review classed the engines as
speculative generality. It proposed either cutting each stack down to
its default engine or recording the need in an ADR.

## Decision

We keep the engine choice. It is **wanted functionality**: the host
decides which HTTP engine carries its mesh calls, not the library. All
four engines stay on both stacks, with the defaults as they are.

The feature has the following shape, and the shape is part of this
decision:

- **The choice belongs to this module.** Boot's
  `spring.http.clients.*` settings are neither read nor written, and no
  engine bean is registered. The engine is set on this module's clone of
  Boot's builder. A host's other clients keep the engine Boot gave them.
  `RestClientEngineIsolationTest` and `WebClientEngineIsolationTest`
  pin this.
- **Only the default engine is a hard dependency.** The other engine
  libraries are optional dependencies. A host adds the one it selects.
- **A misconfiguration fails the start.** The start fails, naming the
  key, when:
  - an override key is not a mesh alias;
  - the selected engine's library is missing;
  - the JDK client would replace the mesh `Host` header.
- **Each engine says what it does not apply.** One startup line per
  engine lists the settings it applies and the ones it cannot express.
- **Every engine abandons a cancelled exchange.** It frees its
  connection, whichever engine it is.

We accept the cost the review measured as the price of the feature. We
reduce that cost by sharing more code, never by cutting engines. What
is the same for both stacks moves to `veredium-core` (`core/engine`)
under ADR-0001's rule. Each stack keeps only its engine-specific
factories. These parts move:

- the lifecycle of the engine transports (creation, closed state,
  draining shutdown);
- the start rules, including the JDK `Host`-header check;
- the startup lines.

## Consequences

**Positive:**

- A host can run the engine it already operates, or one whose behaviour
  suits a particular backend, without forking the client.
- Future reviews have a recorded justifying force. Finding 1 of the
  review is closed as "not changed, deliberate".

**Negative:**

- Eight transport implementations stay. Every update of Spring, Jetty,
  Reactor Netty or HttpClient 5 means re-checking the `Assumption:`
  comments in the transports (`grep -rn "Assumption:" */src/main`) and
  running the engine-matrix tests.
- Each cross-cutting concern (draining, cancellation, meters) has to be
  solved per engine and per stack.
- The two stacks have already drifted apart. Their engine layers have
  different shapes, and they check the JDK `Host` header differently
  (the review's finding 3). Until that layer moves to the core, the
  same configuration can start on one stack and fail on the other.

**Neutral:**

- Adding a fifth engine is a new decision. It amends this record, it
  does not follow from it.
- Dropping an engine later, after a release, removes a value of a
  public configuration key and needs a deprecation cycle.

## Considered and rejected

- **One engine per stack, the current default** (the review's
  recommendation). This would delete the largest and most
  defect-prone part of the code. It was rejected because the engine
  choice is a product feature, not an accident of the port.
