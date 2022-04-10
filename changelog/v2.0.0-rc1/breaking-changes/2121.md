We removed the experimental `vast get` command. It relied on an internal unique
event ID that was never exposed to the user except for debug messages. This
removal is a preparatory step towards a simplification of some of the internal
workings of VAST.
