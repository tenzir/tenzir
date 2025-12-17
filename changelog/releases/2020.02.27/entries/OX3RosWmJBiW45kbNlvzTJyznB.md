---
title: "Continuous import of Zeek logs in VAST is broken"
type: bugfix
author: tobim
created: 2020-02-14T15:33:42Z
pr: 750
---

Continuously importing events from a Zeek process with a low rate of emitted
events resulted in a long delay until the data would be included in the result
set of queries. This is because the import process would buffer up to 10,000
events before sending them to the server as a batch. The algorithm has been
tuned to flush its buffers if no data is available for more than 500
milliseconds.
