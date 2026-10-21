---
title: Failure-isolated Sigma rule reloads
type: bugfix
authors:
  - mavam
created: 2026-10-15T15:13:45.769545Z
---

The `sigma` operator now reloads multi-document rule files without blocking
valid sibling updates when one document is invalid. It retains the last valid
version of each affected document and continues applying valid changes from the
other documents. The operator matches documents by rule ID or name, using
positional matching only when neither version has an identity and the document
count is unchanged.
