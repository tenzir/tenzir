---
title: "Support /etc/vast/vast.conf as global config"
type: feature
authors: dominiklohmann
pr: 898
---

VAST now supports `/etc/vast/vast.conf` as an additional fallback for the
configuration file. The following file locations are looked at in order: Path
specified on the command line via `--config=path/to/vast.conf`, `vast.conf` in
current working directory, `${INSTALL_PREFIX}/etc/vast/vast.conf`, and
`/etc/vast/vast.conf`.
