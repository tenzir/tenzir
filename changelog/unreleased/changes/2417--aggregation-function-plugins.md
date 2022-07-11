The `summarize` pipeline operator is now a builtin; the previously bundled
`summarize` plugin no longer exists. Aggregation functions in the `summarize`
operator are now plugins, which makes them easily extensible. The syntax of
`summarize` now supports specification of output field names, similar to SQL's
`AS` in `SELECT f(x) AS name`.

The `count` pipeline operator no longer exists. If you relied on the
undocumented operator's behavior, please let us know.
