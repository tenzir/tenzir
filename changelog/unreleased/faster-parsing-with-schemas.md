---
title: Faster parsing with schemas
type: bugfix
authors:
  - jachris
created: 2026-11-03T15:48:21.054568Z
---

Parsing with a schema no longer slows down as batches grow. This affects
functions and operators that apply a schema to the events they build, such
as `parse_syslog` and `read_syslog`, which do so by default, and parsers that
read the same key more than once in every event. Previously, building each
event took time proportional to the number of events before it in the batch,
so large batches parsed many times slower than small ones.
