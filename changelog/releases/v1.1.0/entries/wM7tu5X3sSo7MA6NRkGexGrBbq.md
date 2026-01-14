---
title: "Implement a generic aggregation transform step"
type: feature
author: dominiklohmann
created: 2022-02-15T12:10:32Z
pr: 2076
---

The new built-in `rename` transform step allows for renaming event types
during a transformation. This is useful when you want to ensure that a
repeatedly triggered transformation does not affect already transformed
events.

The new `aggregate` transform plugin allows for flexibly grouping and
aggregating events. We recommend using it alongside the [`compaction`
plugin](https://vast.io/docs/about/use-cases/data-aging), e.g., for rolling
up events into a more space-efficient representation after a certain amount of
time.
