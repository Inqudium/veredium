# Module veredium-core

The stack-neutral core of Veredium: the mesh timeout ladder (`app.mesh`), the breaker verdicts and the
Resilience4j customizer wiring that both client modules share. The long-form guide lives in
[docs/GUIDE.md](https://github.com/Inqudium/veredium/blob/main/veredium-core/docs/GUIDE.md).

# Package eu.inqudium.veredium.core.mesh

The `app.mesh` tree: one entry per endpoint, the timeout ladder derived from one measured anchor, and the
configuration that hands the derived rungs to Resilience4j.

# Package eu.inqudium.veredium.core.resilience

The breaker verdicts (benign 4xx is a success, a client-side failure is ignored), the per-alias Resilience4j
customizers that install them together with the derived rungs, and the startup guard.
