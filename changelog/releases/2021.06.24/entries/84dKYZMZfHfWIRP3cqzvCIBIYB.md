---
title: "Fix shutdown hang in sources on SIGTERM/SIGINT"
type: bugfix
author: dominiklohmann
created: 2021-06-15T14:47:47Z
pr: 1718
---

Import processes no longer hang on receiving SIGINT or SIGKILL. Instead, they
shut down properly after flushing yet to be processed data.
