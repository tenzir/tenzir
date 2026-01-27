---
title: "Keep the y-axis order for `chart_*` as specified"
type: bugfix
author: dominiklohmann
created: 2025-04-22T09:02:33Z
pr: 5131
---

The `chart_*` operators no longer sort y-axes by their names. Instead, the
user-provided order is used. For example, in `metrics "pipeline" | chart_bar
x=timestamp, resolution=1d, y={"Ingress": ingress.bytes.sum(), "Egress":
egress.bytes.sum()}` the field order is now "Ingress" followed by "Egress"
as specified instead of the other way around.
