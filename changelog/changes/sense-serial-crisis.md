---
title: "Support for OCSF extensions"
type: feature
authors: jachris
pr: 5306
---

The `ocsf::apply` operator now supports OCSF extensions. This means that
`metadata.extensions` is now also taken into account for casting and validation.
At the moment, only the extensions versioned together with OCSF are supported.
This includes the `win` and `linux` extensions.
