---
title: "Activate cloud plugins explicitely"
type: bugfix
authors: rdettai
pr: 2510
---

We changed the way `vast-cloud` is loading its cloud plugins to make it more
explicit. This avoids inconsitent defaults assigned to variables when using core
commands on specific plugins.
