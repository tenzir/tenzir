---
title: "API request isolation"
type: bugfix
author: jachris
created: 2025-09-25T23:16:57Z
pr: 5486
---

Requests to the `/pipeline` API are now properly isolated and sequentialized.
Before, it could happen that certain requests that should not be executed
concurrently were interleaved. This could lead to unpredictable results when
interacting with pipelines through the platform.
