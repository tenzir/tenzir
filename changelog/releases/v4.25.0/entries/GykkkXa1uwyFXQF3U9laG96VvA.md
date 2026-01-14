---
title: "Implement TQL2 `from` and `to`"
type: feature
author: IyeOnline
created: 2024-12-18T15:00:59Z
pr: 4805
---

We have added the `from` operator that allows you to easily onboard data from
most sources.
For example, you can now write `from "https://example.com/file.json.gz"`
to automatically deduce the load operator, compression, and format.

We have added the `to` operator that allows you to easily send data to most
destinations.
For example, you can now write `to "ftps://example.com/file.json.gz"`
to automatically deduce the save operator, compression, and format.

You can use the new `subnet(string)` function to parse strings as subnets.
