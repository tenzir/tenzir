---
title: "Switch the index to basic messaging"
type: bugfix
authors: tobim
pr: 4613
---

We fixed a bug that sometimes caused the `tenzir-node` process to hang on
shutdown. This was most likely to happen when the node shut down immediately
after starting up, e.g., because of an invalid configuration file.
