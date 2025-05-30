---
title: "Rename the 'default' table slice type to 'caf'"
type: change
authors: dominiklohmann
pr: 948
---

The `default` table slice type has been renamed to `caf`. It has not been the
default when built with Apache Arrow support for a while now, and the new name
more accurately reflects what it is doing.
