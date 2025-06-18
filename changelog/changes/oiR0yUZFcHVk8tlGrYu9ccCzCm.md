---
title: "Upgrade exporter to use new pipelines"
type: feature
authors: jachris
pr: 3076
---

The `vast export` command now accepts the new pipelines as input. Furthermore,
`vast export <expr>` is now deprecated in favor of `vast export 'where <expr>'`.
