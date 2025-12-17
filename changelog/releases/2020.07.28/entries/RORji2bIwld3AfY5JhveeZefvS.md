---
title: "Use Flatbuffers for Persistent State of Segment Store and Meta Index"
type: change
author: lava
created: 2020-07-13T15:50:44Z
pr: 972
---

[FlatBuffers](https://google.github.io/flatbuffers/) is now a required
 dependency for VAST. The archive and the segment store use FlatBuffers to store
 and version their on-disk persistent state.
