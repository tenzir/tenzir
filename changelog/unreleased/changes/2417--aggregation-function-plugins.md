Aggregation functions in the `summarize` operator are now plugins, which makes
them easily extensible. The syntax of `summarize` now supports specification of
output field names, similar to SQL's `AS` in `SELECT f(x) AS name`.

The `summarize` operator supports two new aggregation functions: `sample` takes
the first value in every group; `distinct` filters out duplicate values.
