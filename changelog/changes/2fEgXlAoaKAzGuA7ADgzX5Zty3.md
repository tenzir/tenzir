---
title: "Remove the get subcommand"
type: change
authors: tobim
pr: 2121
---

We removed the experimental `vast get` command. It relied on an internal unique
event ID that was only exposed to the user in debug messages. This removal is a
preparatory step towards a simplification of some of the internal workings of
VAST.
