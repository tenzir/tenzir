---
title: "`where` on lists no longer slows down with batch size"
type: bugfix
authors:
  - aljazerzen
---

Filtering a list with `xs.where(x => …)` used a bitmap with linear-time random
access to select the matching elements, making the function quadratic in the
number of list elements per batch. Pipelines got dramatically slower as their
batches grew: filtering 262144 events with a five-element list at a batch size
of 65536 took 21 s and now takes 2.2 s.
