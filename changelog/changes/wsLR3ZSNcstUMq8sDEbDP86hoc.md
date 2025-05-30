---
title: "Add `events` field to output of `show partitions`"
type: feature
authors: dominiklohmann
pr: 3580
---

The output of `show partitions` includes a new `events` field that shows the
number of events kept in that partition. E.g., the pipeline `show partitions |
summarize events=sum(events) by schema` shows the number of events per schema
stored at the node.
