Store metrics have been modified. Every store now emits the following metrics:

1. `active-store.lookup.runtime`
2. `active-store.lookup.hits`
3. `passive-store.lookup.runtime`
4. `passive-store.lookup.hits`

The store type, previously containing `active` or `passive` is now used to
distinguish between the various supported store types, e.g. `parquet`,
`feather`, and `segment-store`.

