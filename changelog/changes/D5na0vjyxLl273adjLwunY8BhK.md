---
title: "Require end-of-input to be reached for range-based parser invocations"
type: feature
authors: dominiklohmann
pr: 808
---

An under-the-hood change to our parser-combinator framework makes sure that we
do not discard possibly invalid input data up the the end of input. This
uncovered a bug in our MRT/bgpdump integrations, which have thus been disabled
(for now), and will be fixed at a later point in time.
