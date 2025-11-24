---
title: Fixed `write_parquet`
type: bugfix
authors: IyeOnline
pr: 5582
---

In the last release we introduced a bug that made the `write_parquet` operator
exit early without writing the file. This has now been fixed.
