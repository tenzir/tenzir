---
title: "Fix a potential crash in `enrich --replace`"
type: feature
author: tobim
created: 2024-06-12T16:43:52Z
pr: 4291
---

The `enrich` operator no longer crashes when it is used to replace a field value with a context value of a different type and the context is not able to provide a substitute for all inputs.
