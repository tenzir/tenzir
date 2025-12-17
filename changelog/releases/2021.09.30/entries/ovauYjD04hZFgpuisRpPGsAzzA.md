---
title: "Disallow unsupported field meta extractor predicates"
type: bugfix
author: tobim
created: 2021-09-23T08:09:20Z
pr: 1886
---

Expression predicates of the `#field` type now produce error messages instead of
empty result sets for operations that are not supported.
