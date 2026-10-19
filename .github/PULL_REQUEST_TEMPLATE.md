## What

<!-- What does this PR change? -->

## Why

<!-- Why is this change needed? Link the issue if one exists: Fixes #... -->

## Checklist

- [ ] `mvn verify` passes locally (build, tests, ktlint)
- [ ] Behavior changes are covered by tests
- [ ] Documentation (README / GUIDE) updated where relevant
- [ ] Comments: for every rule this PR states or moves - **where does this rule
      already stand?** (one canonical place, the rest one sentence plus link);
      for every guide or ADR reference - **did I open the heading I cite?**
- [ ] Dependency bump: `grep -rn "Assumption:\|Workaround:" */src/main` reviewed
      against the new version
