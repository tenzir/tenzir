---
title: "Break back pressure in hidden subscribers"
type: bugfix
author: dominiklohmann
created: 2024-07-17T07:28:51Z
pr: 4399
---

The `subscribe` operator no longer propagates back pressure to its corresponding
`publish` operators when part of a pipeline that runs in the background, i.e.,
is not visible on the overview page on app.tenzir.com. An invisible subscriber
should never be able to slow down a publisher.
