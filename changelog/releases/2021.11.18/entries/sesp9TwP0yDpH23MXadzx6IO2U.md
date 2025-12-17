---
title: "Fix deletion of segments if CWD != dbdir"
type: bugfix
author: tobim
created: 2021-10-21T11:47:12Z
pr: 1912
---

Store files now get deleted correctly if the database directory differs from the
working directory.
