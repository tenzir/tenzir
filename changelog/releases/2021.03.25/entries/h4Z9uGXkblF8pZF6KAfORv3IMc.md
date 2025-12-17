---
title: "Change the default batch size to 1024"
type: change
author: tobim
created: 2021-03-17T12:40:50Z
pr: 1396
---

The default size of table slices (event batches) that is created from `vast
import` processes has been changed from 1,000 to 1,024.
