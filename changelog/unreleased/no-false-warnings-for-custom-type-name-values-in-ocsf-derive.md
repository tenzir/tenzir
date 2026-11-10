---
title: No false warnings for custom type_name values in ocsf_derive
type: bugfix
authors:
  - zedoraps
created: 2026-10-28T10:01:28.3195Z
---

The `ocsf_derive` operator and function no longer warn about an invalid `type_name` when the `type_uid` denotes an "Other" activity, such as `300299` with `type_name: "Authentication: user_login"`. Like other "Other" values, the name carries a data source specific value.
