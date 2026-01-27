---
title: "Roles in `save_s3` and `to_amazon_security_lake`"
type: feature
author: [raxyte,IyeOnline]
created: 2025-07-31T22:46:24Z
pr: 5391
---

We have added new options to assume a role to the `save_s3` and
`to_amazon_security_lake` operators. You can specify an AWS `role` and the
operator(s) will assume this role for authorization and optionally. Additionally
you can specify an `external_id` to use alongside the role.
