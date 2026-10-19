# ADR Format Guide

This guide describes the format every Architecture Decision Record
(ADR) in `docs/adr/` follows, so new records stay consistent with the
established convention. When in doubt, open a recent, well-structured
ADR and mirror its shape; the convention is the one of the sibling
projects (tabellarium, limesium, legatium), whose records are the
worked examples until this project has its own.

---

## File naming

- One file per decision: `ADR-NNNN-short-kebab-slug.md`.
- `NNNN` is a zero-padded, four-digit sequence number that never gets
  reused (e.g. `0001`, `0004`, `0017`). The `ADR-` prefix and the
  four-digit width are this repository's convention, established by
  the sibling projects' ADR-0001: the
  number is the immutable reference target cited from code comments,
  error messages, the CHANGELOG, and review reports, so it is never
  renumbered.
- The slug is a short, lowercase, hyphen-separated summary of the topic
  (`trace-id-is-the-request-id`, `headers-masked-by-default`).
- All filenames, content, headings, and comments are in **English**;
  ADRs are part of the open-source artifact.

## Document skeleton

Every ADR has this top-level shape:

```markdown
# ADR-NNNN: Short title

**Status:** <status>  
**Date:** YYYY-MM-DD  
**Deciders:** <who>

## Context

## Decision

## Consequences
```

The title line, the metadata block, and the three H2 sections
(`Context`, `Decision`, `Consequences`) are **mandatory**. Everything
else is optional and added only when it earns its place.

## Title

- A single H1: `# ADR-NNNN: Title`.
- `NNNN` matches the filename number.
- The title is a concise noun phrase or a one-line statement naming
  the decision (`The default correlation id is a counting id, not a
  UUID`, `Logged header values are masked by default; plaintext is an
  explicit allowlist`), not a paragraph.

## Metadata block

Immediately under the title, one field per line. Each line ends with
**two trailing spaces** so Markdown renders the block as separate
lines rather than one wrapped paragraph. There is no blank line
between fields; one blank line separates the block from the first
`## Context` heading.

Required fields, in this order:

| Field           | Notes                                                    |
|-----------------|----------------------------------------------------------|
| `**Status:**`   | One of the status values below.                          |
| `**Date:**`     | Original decision date in ISO `YYYY-MM-DD` format.       |
| `**Deciders:**` | Who made the call; currently `Dirk Haase (maintainer)`.  |

Optional fields, added when relevant (placed after `Date:`, before or
after `Deciders:` as appropriate; keep `Related:` last):

| Field               | When to use                                          |
|---------------------|------------------------------------------------------|
| `**Last updated:**` | Add when the ADR has been revised after its `Date`.  |
| `**Supersedes:**`   | This ADR replaces one or more earlier ADRs.          |
| `**Related:**`      | Cross-references to other ADRs (see format below).   |

### Status values

- `Accepted`: adopted and authoritative for the codebase.
- `Proposed`: under active design; the direction is committed but
  details may still shift before acceptance.
- `Superseded by ADR-NNNN (YYYY-MM-DD)`: retained for historical
  context; the named successor carries the current decision. May
  reference more than one successor.

### `Related:` format

A comma-separated list of `ADR-NNNN (short parenthetical description)`
entries, optionally wrapping across lines. The parenthetical states
*why* the ADR is related or what role it plays:

```markdown
**Related:** ADR-0002 (the retry policy this ADR's timeout feeds
into)
```

Supersession relationships are spelled out inline where useful, e.g.
`ADR-0003 (superseded by this ADR)`.

## The three mandatory sections

### `## Context`

The forces at play: the problem, constraints, and why a decision is
needed. State the situation neutrally; describe the tension that the
decision resolves, not the solution. Cite the review report or issue
that raised the question by path (`docs/assessment/...`, finding N)
so the evidence stays traceable. Where there are competing approaches,
it is common to enumerate them here before the Decision picks one.

### `## Decision`

What was decided, stated affirmatively ("We adopt…", "We use…"). This
is usually the longest section. Use H3 subsections (`###`) to break a
complex decision into parts, and include concrete artifacts that make
the decision unambiguous:

- fenced code blocks with a language tag (```` ```kotlin ````,
  ```` ```xml ````) for API signatures, configuration, etc.;
- tables for enumerations, mappings, and ordering;
- inline emphasis (`**bold**`) to name the key mechanism.

### `## Consequences`

The outcomes of the decision, grouped under three bolded labels, each
followed by a bullet list:

```markdown
**Positive:**

- …

**Negative:**

- …

**Neutral:**

- …
```

`Positive` and `Negative` are expected on almost every ADR; `Neutral`
is included whenever there are trade-offs that are neither clearly
good nor bad (e.g. "reversal is a conscious API addition"). Be honest
about the negatives: the value of an ADR is that it records the cost
of the decision, not just its benefits.

## Optional sections

Add these only when the decision genuinely needs them. They appear
after `Consequences` (or, for framing sections, before `Context`):

- `## Implementation status`: for `Proposed` ADRs, tracks what has
  actually been built versus what is still pending.
- `## Considered and rejected` / `## Considered options`: when the
  alternatives deserve fuller treatment than a list inside `Context`.
  Use `**Pros:**` / `**Cons:**` per option where helpful.
- `## Implementation notes`: concrete guidance for implementors.
- `## What this ADR does not decide` / `## Scope`: explicitly bound the
  decision to prevent scope creep.
- `## History`: chronology of revisions for long-lived ADRs.

Inline mini-fields (a bolded label followed by a colon and prose, e.g.
`**Rationale:**`, `**Enforced at:**`, `**Revisit when:**`,
`**Verification:**`) are used freely within sections to highlight a
specific point. Reach for them when a short labelled note is clearer
than a full subsection.

## Style conventions

- **English throughout**: prose, headings, code comments, identifiers.
- **Markdown line breaks** in the metadata block via two trailing
  spaces.
- **Fenced code blocks always carry a language tag.**
- **Wrap prose** at the repository's column (the existing ADRs and the
  README wrap around 72 characters); don't reflow an entire file just
  to change the width. Tables may exceed the column.
- **Cross-link** related ADRs with relative links
  (`[ADR-0002](ADR-0002-short-kebab-slug.md)`), and
  add the new ADR to the topic index in [`README.md`](README.md) under
  the appropriate cluster, with its status and any supersession noted
  inline.
- **Citing from code:** an in-code comment, a POM comment, or an
  error message may cite an ADR by number, but must still carry the
  local one-line rule so the reference is never bare; the ADR is the
  decision trail, not the normative statement.

## Minimal template

```markdown
# ADR-NNNN: Title

**Status:** Proposed  
**Date:** YYYY-MM-DD  
**Deciders:** Dirk Haase (maintainer)

## Context

Describe the problem and the forces that make a decision necessary.

## Decision

State what was decided, affirmatively. Use subsections, code blocks,
and tables as needed.

## Consequences

**Positive:**

- …

**Negative:**

- …

**Neutral:**

- …
```
