---
title: "Add support for type-level synopses and a string synopsis"
type: feature
authors: tobim
pr: 1214
---

Low-selectivity queries of string (in)equality queries now run up to 30x faster,
thanks to more intelligent selection of relevant index partitions.
