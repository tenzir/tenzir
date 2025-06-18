---
title: "Error when initializing a plugin fails"
type: bugfix
authors: dominiklohmann
pr: 1618
---

VAST now correctly refuses to run when loaded plugins fail their initialization,
i.e., are in a state that cannot be reasoned about.
