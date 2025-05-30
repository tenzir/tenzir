---
title: "Fix a potential crash in `enrich --replace`"
type: feature
authors: tobim
pr: 4291
---

The `enrich` operator no longer crashes when it is used to replace a field value with a context value of a different type and the context is not able to provide a substitute for all inputs.
