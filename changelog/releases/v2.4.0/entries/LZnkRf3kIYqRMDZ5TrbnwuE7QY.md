---
title: "Remove PyVAST in favor of new Python bindings"
type: change
author: dominiklohmann
created: 2022-10-29T10:27:40Z
pr: 2674
---

We removed PyVAST from the code base in favor of the new Python bindings. PyVAST
continues to work as a thin wrapper around the VAST binary, but will no longer
be released alongside VAST.
