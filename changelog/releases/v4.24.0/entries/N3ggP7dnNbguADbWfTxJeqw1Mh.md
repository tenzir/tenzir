---
title: "PRs 4741-4746"
type: change
author: jachris
created: 2024-11-09T20:45:45Z
pr: 4741
---

The functions `ocsf_category_name`, `ocsf_category_uid`, `ocsf_class_name`, and
`ocsf_class_uid` are now called `ocsf::category_name`, `ocsf::category_uid`,
`ocsf::class_name`, and `ocsf::class_uid`, respectively. Similarly, the
`package_add`, `package_remove`, `packages`, and `show pipelines` operators are
now called `package::add`, `package::remove`, `package::list`, and
`pipeline::list`, respectively.
