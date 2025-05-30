---
title: "Fix deletion of segments if CWD != dbdir"
type: bugfix
authors: tobim
pr: 1912
---

Store files now get deleted correctly if the database directory differs from the
working directory.
