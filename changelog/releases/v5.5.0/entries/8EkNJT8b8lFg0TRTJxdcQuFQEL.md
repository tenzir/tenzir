---
title: "Entropy Calculation"
type: feature
author: dominiklohmann
created: 2025-06-17T13:07:26Z
pr: 4852
---

TQL now supports calculating the Shannon entropy of data using the new `entropy`
aggregation function. This function measures the amount of uncertainty or
randomness in your data, which is particularly useful for analyzing data
distributions and information content.

The entropy function calculates Shannon entropy using the formula `H(x) =
-sum(p(x[i]) \* log(p(x[i])))`, where `p(x[i])` is the probability of each
unique value. Higher entropy values indicate more randomness, while lower values
indicate more predictability in your data.

For example, if you have a dataset with different categories and want to measure
how evenly distributed they are:

```tql
from {category: "A"}, {category: "A"}, {category: "B"}, {category: "C"}
summarize entropy_value = category.entropy()
```

This will return an entropy value of approximately 1.04, indicating moderate
randomness in the distribution.

The function also supports normalization via an optional `normalize` parameter.
When set to `true`, the entropy is normalized between 0 and 1 by dividing by
the logarithm of the number of unique values:

```tql
from {category: "A"}, {category: "A"}, {category: "B"}, {category: "C"}
summarize normalized_entropy = category.entropy(normalize=true)
```

This returns a normalized entropy value of approximately 0.95, making it easier
to compare entropy across datasets with different numbers of unique values.
