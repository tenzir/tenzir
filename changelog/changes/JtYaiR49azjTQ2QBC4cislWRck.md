---
title: "Deprecate TQL1"
type: change
authors: dominiklohmann
pr: 4954
---

TQL1 is now deprecated in favor of TQL2. Starting a TQL1 pipeline issues a
warning on startup nudging towards upgrading to TQL2, which will become the
default in the upcoming Tenzir v5.0 release. This warning cannot be turned off.
