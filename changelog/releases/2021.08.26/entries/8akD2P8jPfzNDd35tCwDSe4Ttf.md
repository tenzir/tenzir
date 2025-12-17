---
title: "Print memory counts in bytes instead of kB"
type: bugfix
author: tobim
created: 2021-08-25T09:33:22Z
pr: 1862
---

The memory counts in the output of `vast status` now represent bytes
consistently, as opposed to a mix of bytes and kilobytes.
