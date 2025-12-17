---
title: "Rename files after reading them"
type: feature
author: dominiklohmann
created: 2025-06-17T17:33:58Z
pr: 5285
---

The `from_file` operator now supports moving files after reading them.

For example, `from_file "logs/*.log", rename=path => f"{path}.done"` reads all
`.log` files in the `logs` directory, and after reading them renames the files
to have the extension `.log.done`.
