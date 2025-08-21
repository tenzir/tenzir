---
title: "Timezone troubles from `parse_time()`"
type: bugfix
authors: raxyte
pr: 5435
---

We fixed assertion failures when using the `parse_time` function with the `%z`
or `%Z` specifiers.
