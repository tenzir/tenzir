---
title: Cleared Sigma rules for empty files
type: bugfix
authors:
  - mavam
created: 2026-10-20T15:48:23.790171Z
---

The `sigma` operator now clears a file's rules and filters when the file becomes
empty or contains only whitespace and comments. Previously, it kept the last
valid rules active and reported an error. For example, emptying `rules.yaml`
now stops its findings on the next refresh of `sigma path="rules.yaml"`.
Malformed YAML still retains the source's last valid rules.
