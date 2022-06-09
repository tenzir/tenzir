The new `rebuild` command allows for rebuilding old partitions to take advantage
of improvements by newer VAST versions. The rebuilding takes place in the VAST
server in the background. This process merges partitions up to the configured
`max-partition-size`, turns VAST v1.x's heterogeneous into VAST v2.x's
homogenous partitions, migrates all data to the currently configured
`store-backend`, and upgrades to the most recent internal batch encoding and
indexes.
