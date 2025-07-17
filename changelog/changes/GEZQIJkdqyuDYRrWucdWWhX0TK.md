---
title: "PRs 632-726"
type: feature
authors: mavam
pr: 632
---

When a record field has the `#index=hash` attribute, VAST will choose an
optimized index implementation. This new index type only supports (in)equality
queries and is therefore intended to be used with opaque types, such as unique
identifiers or random strings.
