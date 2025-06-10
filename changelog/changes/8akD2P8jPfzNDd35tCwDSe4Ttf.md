---
title: "Print memory counts in bytes instead of kB"
type: bugfix
authors: tobim
pr: 1862
---

The memory counts in the output of `vast status` now represent bytes
consistently, as opposed to a mix of bytes and kilobytes.
