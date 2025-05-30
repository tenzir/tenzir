---
title: "PRs 3036-3039-3089"
type: feature
authors: dominiklohmann
pr: 3036
---

The `put` operator is the new companion to the existing `extend` and `replace`
operators. It specifies the output fields exactly, referring either to input
fields with an extractor, metadata with a selector, or a fixed value.

The `extend` and `replace` operators now support assigning extractors and
selectors in addition to just fixed values.
