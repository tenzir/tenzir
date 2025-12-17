---
title: "Various config and default setting fixes"
type: bugfix
author: tobim
created: 2020-05-12T11:26:36Z
pr: 866
---

Fixed a bug that caused `vast import` processes to produce `'default'` table
slices, despite having the `'arrow'` type as the default.

Fixed a bug where setting the `logger.file-verbosity` in the config file would
not have an effect.
