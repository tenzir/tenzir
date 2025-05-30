---
title: "Perform pass over build config defaults"
type: change
authors: dominiklohmann
pr: 695
---

Build configuration defaults have been adapated for a better user experience.
Installations are now relocatable by default, which can be reverted by
configuring with `--without-relocatable`. Additionally, new sets of defaults
named `--release` and `--debug` (renamed from `--dev-mode`) have been added.
