---
title: "Remove the /export endpoint"
type: change
author: Dakostu
created: 2023-03-07T14:59:49Z
pr: 2990
---

The REST API does not contain the `/export` and `/export/with-schemas`
endpoints anymore. Any previous queries using those endpoints have to be sent
to the `/query` endpoint now.
