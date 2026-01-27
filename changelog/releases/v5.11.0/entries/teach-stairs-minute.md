---
title: "Dynamic `log_type` for `to_google_secops`"
type: feature
author: raxyte
created: 2025-07-29T17:30:31Z
pr: 5365
---

The `to_google_secops` operator now supports dynamic `log_type`s. You can set
the option to any expression evaluating to a string, e.g.:

```tql
from {type: "CUSTOM_DNS", text: "..."},
     {type: "BIND_DNS", text: "..."}
to_google_secops log_type=type, log_text=text, ...
```
