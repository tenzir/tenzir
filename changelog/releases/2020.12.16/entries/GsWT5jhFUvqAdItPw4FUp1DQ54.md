---
title: "Move schema definitions into subdirectory"
type: change
author: dominiklohmann
created: 2020-11-24T10:07:33Z
pr: 1194
---

Installed schema definitions now reside in `<datadir>/vast/schema/types`,
taxonomy definitions in `<datadir>/vast/schema/taxonomy`, and concept
definitions in `<datadir/vast/schema/concepts`, as opposed to them all being in
the schema directory directly. When overriding an existing installation, you
_may_ have to delete the old schema definitions by hand.
