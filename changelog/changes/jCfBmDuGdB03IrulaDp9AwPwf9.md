---
title: "Enable configuration of the zstd compression level for feather store"
type: feature
authors: dispanser
pr: 2623
---

VAST has a new configuration setting, `vast.zstd-compression-level`, to control
the compression level of the Zstd algorithm used in both the Feather and
Parquet store backends. The default level is set by the Apache Arrow library,
and for Parquet is no longer explicitly defaulted to `9`.
