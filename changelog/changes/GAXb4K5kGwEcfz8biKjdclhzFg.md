---
title: "Add missing `-X` option for `kafka` saver"
type: bugfix
authors: dominiklohmann
pr: 4317
---

The `-X` option for overriding configuration options for `librdkafka` now works
the `kafka` saver as well. Previously, the option was only exposed for the
loader, unlike advertised in the documentation.
