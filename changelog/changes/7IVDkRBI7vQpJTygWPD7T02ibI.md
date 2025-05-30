---
title: "Use systemd provided default paths if available"
type: change
authors: tobim
pr: 3714
---

When selecting default paths, the `tenzir-node` will now respect
the systemd-provided variables `STATE_DIRECTORY`, `CACHE_DIRECTORY`
and `LOGS_DIRECTORY` before falling back to `$PWD/tenzir.db`.
