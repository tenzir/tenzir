---
title: "Include in-process sources/sinks in status output"
type: bugfix
author: dominiklohmann
created: 2021-08-19T07:48:40Z
pr: 1852
---

The output of VAST status now includes status information for sources and sinks
spawned in the VAST node, i.e., via `vast spawn source|sink <format>` rather
than `vast import|export <format>`.
