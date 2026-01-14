---
title: "Deprecate the archive store-backend"
type: change
author: dominiklohmann
created: 2022-05-20T13:18:18Z
pr: 2290
---

The `vast.store-backend` configuration option no longer supports `archive`,
and instead always uses the superior `segment-store` instead. Events stored in
the archive will continue to be available in queries.
