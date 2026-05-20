---
title: Preserve categorical order in `chart_bar`
type: change
author: mavam
created: 2026-05-12T00:00:00Z
---

The `chart_bar` and `chart_pie` operators now preserve the incoming row order
for categorical x-axis values such as strings, IP addresses, and subnets. This
allows users to control bar order with regular TQL operators such as `sort`
before charting.
