---
title: "Fix exporter.selectivity for idle periods"
type: bugfix
authors: dominiklohmann
pr: 1574
---

The `exporter.selectivity` metric is now 1.0 instead of NaN for idle periods.

VAST no longer renders JSON numbers with non-finite numbers as `NaN`, `-NaN`,
`inf`, or `-inf`, resulting in invalid JSON output. Instead, such numbers are
now rendered as `null`.
