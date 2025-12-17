---
title: "Rename the 'default' table slice type to 'caf"
type: change
author: dominiklohmann
created: 2020-06-24T11:53:32Z
pr: 948
---

The `default` table slice type has been renamed to `caf`. It has not been the
default when built with Apache Arrow support for a while now, and the new name
more accurately reflects what it is doing.
