---
title: "Error when initializing a plugin fails"
type: bugfix
author: dominiklohmann
created: 2021-04-30T19:16:32Z
pr: 1618
---

VAST now correctly refuses to run when loaded plugins fail their initialization,
i.e., are in a state that cannot be reasoned about.
