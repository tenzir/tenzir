The `rebuilder.partitions.remaining` metric sometimes reported wrong values when
partitions for at least one schema did not need to be rebuilt. We aligned the
metrics with the actual functionality.
