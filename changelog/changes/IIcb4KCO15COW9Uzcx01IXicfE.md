---
title: "Introduce the `no-location-overrides` option"
type: change
authors: dominiklohmann
pr: 3978
---

We've replaced the `tenzir.allow-unsafe-pipelines` option with the
`tenzir.no-location-overrides` option with an inverted default. The new option
is a less confusing default for new users and more accurately describes what the
option does, namely preventing operator locations to be overriden.
