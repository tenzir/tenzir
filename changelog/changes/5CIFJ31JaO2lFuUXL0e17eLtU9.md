---
title: "Fix bug in decoding multi-object MsgPack types"
type: bugfix
authors: mavam
pr: 984
---

MessagePack-encoded table slices now work correctly for nested container types.
