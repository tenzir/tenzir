---
title: Full Sigma v2.1 detection support
type: feature
authors:
  - mavam
created: 2026-08-12T17:34:19.422368Z
---

The `sigma` operator now implements the Sigma v2.1.0 detection rule surface for the default `sigma` taxonomy, except placeholder expansion with `expand`. Rules with custom taxonomies or `expand` are rejected with an explicit diagnostic. Keyword selections match every string-valued leaf of an event, including strings nested in records and lists.

Modifier support now includes `exists`, `cased`, `neq`, `windash`, `fieldref`, the UTF-16 encoding modifiers, the `re` sub-modifiers `i`/`m`/`s`, and the time-part modifiers `minute` through `year`. Previously, several modifiers were silently misinterpreted; rules relying on them now match correctly.

Conditions can now be lists of OR-linked queries. Quantifiers such as `all of selection_*` combine matching selections without rewriting their internal logic, matching the behavior of pySigma.

Sigma wildcards now treat all regex metacharacters literally and match across newlines. For example, a value like `a+b` no longer accidentally matches `ab`. Field names containing dots deterministically prefer an exact top-level key before nested traversal.
