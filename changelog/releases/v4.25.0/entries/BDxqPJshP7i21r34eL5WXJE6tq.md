---
title: "Introduce a TQL2-only mode"
type: feature
author: dominiklohmann
created: 2024-12-12T15:52:08Z
pr: 4840
---

Start your Tenzir Node with `tenzir-node --tql2` or set the `TENZIR_TQL2=true`
environment variable to enable TQL2-only mode for your node. In this mode, all
pipelines will run as TQL2, with the old TQL1 pipelines only being available
through the `legacy` operator. In Q1 2025, this option will be enabled by
default, and later in 2025 the `legacy` operator and TQL1 support will be
removed entirely.
