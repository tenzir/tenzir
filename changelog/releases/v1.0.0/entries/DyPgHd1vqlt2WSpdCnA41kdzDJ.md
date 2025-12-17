---
title: "Create intermediate dirs for db-directory and respect schema-dirs in bare mode"
type: bugfix
author: dominiklohmann
created: 2022-01-25T15:07:45Z
pr: 2046
---

VAST no longer ignores the `--schema-dirs` option when using `--bare-mode`.

Starting VAST no longer fails if creating the database directory requires
creating intermediate directories.
