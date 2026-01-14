---
title: "Implement feather printer and parser"
type: change
author: balavinaithirthan
created: 2024-04-10T09:02:33Z
pr: 4089
---

The `feather` format now reads and writes Arrow IPC streams in addition to
Feather files, and no longer requires random access to a file to function,
making the format streamable with both `read feather` and `write feather`.
