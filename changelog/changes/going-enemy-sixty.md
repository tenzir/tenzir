---
title: "`ocsf::finalize` operator"
type: feature
authors: raxyte
pr: 5502
---

The new `ocsf::finalize` operator handles transformations when you want to
switch to format that is not as rich as Tenzir's typesystem. For example, when
formatting a OCSF event, `timestamp` OCSF fields need to be converted to
integers. This can be done automatically by using `ocsf::finalize`.
