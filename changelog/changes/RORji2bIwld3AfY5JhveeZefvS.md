---
title: "Use Flatbuffers for Persistent State of Segment Store and Meta Index"
type: change
authors: lava
pr: 972
---

[FlatBuffers](https://google.github.io/flatbuffers/) is now a required
 dependency for VAST. The archive and the segment store use FlatBuffers to store
 and version their on-disk persistent state.
