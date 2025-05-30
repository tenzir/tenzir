---
title: "Send correct message to index when dropping further results"
type: bugfix
authors: lava
pr: 1209
---

The index now correctly drops further results when queries finish early, thus
improving the performance of queries for a limited number of events.
