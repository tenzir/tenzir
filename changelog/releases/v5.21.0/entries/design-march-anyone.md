---
title: "Gracefully handle server errors in the web plugin"
type: bugfix
author: tobim
created: 2025-11-19T09:29:54Z
pr: 5577
---

The node now prints a proper error message in case the builtin web server can't start because the port is already in use.
