---
title: "PRs 1721-1734"
type: change
authors: dominiklohmann
pr: 1721
---

VAST merges lists from configuration files. E.g., running VAST with
`--plugins=some-plugin` and `vast.plugins: [other-plugin]` in the
configuration now results in both `some-plugin` and `other-plugin` being
loaded (sorted by the usual precedence), instead of just `some-plugin`.
