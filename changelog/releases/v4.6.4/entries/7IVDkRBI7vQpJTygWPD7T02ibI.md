---
title: "Use systemd provided default paths if available"
type: change
author: tobim
created: 2023-12-06T14:24:26Z
pr: 3714
---

When selecting default paths, the `tenzir-node` will now respect
the systemd-provided variables `STATE_DIRECTORY`, `CACHE_DIRECTORY`
and `LOGS_DIRECTORY` before falling back to `$PWD/tenzir.db`.
