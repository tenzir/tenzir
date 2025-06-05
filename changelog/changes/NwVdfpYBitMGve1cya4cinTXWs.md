---
title: "Various config and default setting fixes"
type: bugfix
authors: tobim
pr: 866
---

Fixed a bug that caused `vast import` processes to produce `'default'` table
slices, despite having the `'arrow'` type as the default.

Fixed a bug where setting the `logger.file-verbosity` in the config file would
not have an effect.
