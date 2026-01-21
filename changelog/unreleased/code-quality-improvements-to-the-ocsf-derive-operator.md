---
title: Code quality improvements to the ocsf derive operator
type: change
authors:
  - mavam
  - claude
pr: 5673
created: 2026-01-20T15:26:52.087067Z
---

The `ocsf::derive` operator now handles edge cases more gracefully with improved error handling.

This change refines the implementation that preserves original field order during OCSF enum derivation. The improvements include:

- Combined redundant schema iteration loops into a single pass for better efficiency
- Replaced assertion-based validation with graceful error handling that skips invalid schema fields instead of crashing
- Renamed internal variables for clarity (`reverse_sibling` to `sibling_to_enum`)
- Enhanced documentation explaining how non-schema fields are handled
- Added test coverage for derived enum insertion ordering

These changes improve code maintainability and robustness without altering the operator's behavior.
