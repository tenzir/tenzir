---
title: "Disallow unsupported field meta extractor predicates"
type: bugfix
authors: tobim
pr: 1886
---

Expression predicates of the `#field` type now produce error messages instead of
empty result sets for operations that are not supported.
