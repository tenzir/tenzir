---
title: "Close file descriptor by default in 'vast::file"
type: bugfix
author: lava
created: 2020-08-10T17:35:22Z
pr: 1018
---

Some file descriptors remained open when they weren't needed any more. This
descriptor leak has been fixed.
