---
title: "Fix parsing of subnet columns in zeek-tsv"
type: bugfix
author: tobim
created: 2023-10-31T16:20:30Z
pr: 3606
---

The `zeek-tsv` parser is now able to handle fields of type `subnet` correctly.
