---
title: Sigma diagnostics point at the rule
type: change
authors:
  - mavam
created: 2026-09-03T22:40:00Z
---

Diagnostics about one inline Sigma rule now underline that rule's `title` line
inside the `rules` string literal instead of the complete literal. This covers
rules the operator ignores, rules with duplicate identities, and rules skipped
for OCSF input, whether the rules come as one string or as a list of strings.
The rule's line is located for raw strings and for plain strings without
escapes; a plain string with escapes, or a value computed by an expression,
keeps the diagnostic on the complete literal rather than on a shifted line.
