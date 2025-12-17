---
title: "Fix sorting of plugins by name"
type: bugfix
author: dominiklohmann
created: 2021-07-05T10:03:07Z
pr: 1756
---

A regression caused VAST's plugins to be loaded in random order, which
printed a warning about mismatching plugins between client and server. The
order is now deterministic.
