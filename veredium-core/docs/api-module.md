# Module veredium-core

The stack-neutral core of Veredium: the mesh timeout ladder (`app.mesh`), the breaker verdicts and the
Resilience4j customizer wiring that both client modules share. The long-form guide lives in
[docs/GUIDE.md](https://github.com/Inqudium/veredium/blob/main/veredium-core/docs/GUIDE.md).

# Package eu.inqudium.veredium.core

The library's identity (name, version, `User-Agent` token) and `BaseAdapter`, the two budgets an adapter over
either client publishes.

# Package eu.inqudium.veredium.core.mesh

The `app.mesh` tree: one entry per endpoint, the timeout ladder derived from one measured anchor, the binding
configuration and the startup line that prints the derived ladder.

# Package eu.inqudium.veredium.core.resilience

The breaker verdicts (benign 4xx is a success, a client-side failure is ignored), the registrar each stack
subclasses to install them together with the derived rungs as per-alias Resilience4j customizers, the startup guard,
and the blocking resilience decorator.

# Package eu.inqudium.veredium.core.engine

What both stacks share about selecting and closing an HTTP engine: the selection checks and the bounded drain.

# Package eu.inqudium.veredium.core.client

The per-call transport lines, the size sample and the validation step both clients run, and the exception for a
2xx without a body.

# Package eu.inqudium.veredium.core.validation

The `validation.*` block of both properties trees and the Bean Validation `Validator` derived from the host's factory.

# Package eu.inqudium.veredium.core.observation

The startup report on what silences the `http.client.requests` observation.
