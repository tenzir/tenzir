---
title: "Prune expressions for the meta index lookup"
type: change
authors: tobim
pr: 1433
---

Query latency for expressions that contain concept names has improved
substantially. For DB sizes in the TB region, and with a large variety of event
types, queries with a high selectivity experience speedups of up to 5x.
