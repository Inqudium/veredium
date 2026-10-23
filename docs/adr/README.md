# Architecture Decision Records

This directory holds the Architecture Decision Records for
**veredium**. Every record follows the shape in
[ADR-FORMAT.md](ADR-FORMAT.md). ADRs are numbered
`ADR-NNNN-short-kebab-slug.md`; the number is never reused or changed,
because it is cited from code comments, the POMs, the CHANGELOG, and
review reports. The **code is authoritative**: where an ADR and the
code disagree, the code wins and the ADR is corrected.

## Index by topic

### Module structure

| ADR | Title | Status |
|-----|-------|--------|
| [ADR-0001](ADR-0001-one-stack-neutral-core-module.md) | One stack-neutral core module instead of twin copies | Accepted |
| [ADR-0002](ADR-0002-blocking-resilience-decorator-as-a-core-component.md) | The blocking resilience decorator is a core component, usable without a client | Accepted |
| [ADR-0003](ADR-0003-each-mesh-endpoint-is-owned-by-one-client-stack.md) | Each mesh endpoint is owned by one client stack, so both stacks can share a context | Accepted |

### HTTP engines

| ADR | Title | Status |
|-----|-------|--------|
| [ADR-0004](ADR-0004-the-http-engine-is-the-hosts-choice.md) | The HTTP engine is the host's choice, on both stacks | Accepted |

## Superseded

None yet. A fully superseded record keeps its number, gets the status
`Superseded by ADR-NNNN (YYYY-MM-DD)`, and moves to this table; it
must not be treated as current guidance.

## Notes

- **Format & conventions:** [ADR-FORMAT.md](ADR-FORMAT.md).
- **Evidence:** the review reports that ADRs cite live in
  `../assessment/` (created with the first report); they are dated
  snapshots and are not updated when an ADR changes.
- **Siblings:** veredium follows the conventions of tabellarium,
  limesium and legatium; a decision adopted from one of them says so,
  but each project keeps its own record.
