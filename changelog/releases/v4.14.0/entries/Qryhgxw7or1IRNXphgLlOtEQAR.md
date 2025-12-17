---
title: "Remove the superfluous path separators when using S3 or GS connectors"
type: bugfix
author: Dakostu
created: 2024-05-17T05:45:22Z
pr: 4222
---

Paths for `s3` and `gs` connectors are not broken anymore during
loading/saving.
