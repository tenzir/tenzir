---
title: "Amazon Security Lake"
type: change
author: [mavam,IyeOnline]
created: 2025-08-11T15:23:25Z
pr: 5412
---

We have made two convenience changes to the `to_amazon_security_lake` operator:

* The `role` parameter now defaults to the automatically generated role for the
  custom source in Security Lake. If you are using a different role, you can
  still specify it.
* The operator now uses UUIDv7 for the names of the files written into the
  Security Lake's blob storage. Since UUIDv7 is time ordered, inspecting the
  files in the lake becomes slightly easier.
