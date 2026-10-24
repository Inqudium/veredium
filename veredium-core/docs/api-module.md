# Module veredium-core

The stack-neutral core of Veredium: the mesh timeout ladder (`app.mesh`), the breaker verdicts and the
Resilience4j customizer wiring that both client modules share. The long-form guide lives in
[docs/GUIDE.md](https://github.com/Inqudium/veredium/blob/main/veredium-core/docs/GUIDE.md).

# Package eu.inqudium.veredium.core

The library's identity (name, version, `User-Agent` token), `BaseAdapter`, the two budgets an adapter over
either client publishes, and `CallerMdc`, which carries the caller's MDC onto a thread the caller never ran on.

# Package eu.inqudium.veredium.core.mesh

The `app.mesh` tree: one entry per endpoint, the timeout ladder derived from one measured anchor, the binding
configuration, the startup line that prints the derived ladder, and `MeshConnectionPools`, the connection pools a
client factory reports for the startup guard's bulkhead check.

# Package eu.inqudium.veredium.core.resilience

The breaker verdicts (benign 4xx is a success, a client-side failure is ignored), the registrar each stack
subclasses to install them together with the derived rungs as per-alias Resilience4j customizers, the startup guard,
the ownership check that gives every mesh endpoint exactly one client stack (`MeshOwnershipCheck`), and the blocking
resilience decorator.

# Package eu.inqudium.veredium.core.engine

What both stacks share about selecting and closing an HTTP engine: the selection checks, the lifecycle of a
factory's engines (`EngineSet`: created together, used under a lock, released against one deadline) and the bounded
drain.

# Package eu.inqudium.veredium.core.client

What both client factories derive from `app.mesh` and their `validation` block (`MeshClientPlan`), the per-call
transport lines, the size sample and the validation step both clients run, and the exception for a 2xx without a
body.

# Package eu.inqudium.veredium.core.validation

The `validation.*` block of both properties trees and the Bean Validation `Validator` derived from the host's factory.

# Package eu.inqudium.veredium.core.observation

The startup report on what silences the `http.client.requests` observation.
