Rebuilding parttitions now additionally rebatches the contained events to
`vast.import.batch-size` events per batch, which makes queries against
partitions that previously had undersized batches faster.
