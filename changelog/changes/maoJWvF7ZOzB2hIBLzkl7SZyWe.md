---
title: "Remove the /export endpoint"
type: change
authors: Dakostu
pr: 2990
---

The REST API does not contain the `/export` and `/export/with-schemas`
endpoints anymore. Any previous queries using those endpoints have to be sent
to the `/query` endpoint now.
