---
title: "Fix configuration options for the kafka plugin"
type: bugfix
authors: tobim
pr: 4761
---

We fixed a bug in the kafka plugin so that it no longer wrongly splits config
options from the `yaml` files at the dot character.
