---
title: from_file support for paths with spaces
type: bugfix
authors:
  - IyeOnline
  - claude
prs:
  - 6394
created: 2026-06-29T13:51:32.692597Z
---

The `from_file` operator now reads files whose path contains a space or other
characters that require URI encoding. Previously, a space anywhere in the path
(for example in a parent directory name) caused the pipeline to fail with a
`failed to parse path as URI` error.
