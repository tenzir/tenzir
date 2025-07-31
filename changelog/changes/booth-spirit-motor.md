---
title: "Roles in `save_s3` and `to_amazon_security_lake`"
type: feature
authors: [raxyte,IyeOnline]
pr: 5391
---

We have added new options to assume a role in `save_s3` and
`to_amazon_security_lake`. You can specify an AWS `role` and the operator(s)
will assume this role for authorization and optionally also an `external_id`
if you need to.
