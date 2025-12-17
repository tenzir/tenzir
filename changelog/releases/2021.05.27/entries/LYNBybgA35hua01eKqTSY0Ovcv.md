---
title: "Don't send dynamic type information to connecting sources"
type: change
author: lava
created: 2021-05-20T10:04:56Z
pr: 1656
---

Schemas are no longer implicitly shared between sources, i.e., an `import`
process importing data with a custom schema will no longer affect other
sources started at a later point in time. Schemas known to the VAST server
process are still available to all `import` processes. We do not expect this
change to have a real-world impact, but it could break setups where some
sources have been installed on hosts without their own schema files, the
VAST server did not have up-to-date schema files, and other sources were
(ab)used to provide the latest type information.
