---
title: "Close file descriptor by default in 'vast::file'"
type: bugfix
authors: lava
pr: 1018
---

Some file descriptors remained open when they weren't needed any more. This
descriptor leak has been fixed.
