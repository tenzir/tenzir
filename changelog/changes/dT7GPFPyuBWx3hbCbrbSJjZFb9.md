---
title: "Correct the use of ::read()"
type: bugfix
authors: tobim
pr: 1025
---

Incomplete reads have not been handled properly, which manifested for files
larger than 2GB. On macOS, writing files larger than 2GB may have failed
previously. VAST now respects OS-specific constraints on the maximum block size.
