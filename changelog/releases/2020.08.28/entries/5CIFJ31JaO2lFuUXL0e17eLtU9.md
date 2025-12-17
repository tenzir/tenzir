---
title: "Fix bug in decoding multi-object MsgPack types"
type: bugfix
author: mavam
created: 2020-08-05T15:19:13Z
pr: 984
---

MessagePack-encoded table slices now work correctly for nested container types.
