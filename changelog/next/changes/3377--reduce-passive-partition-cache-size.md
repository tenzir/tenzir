The default value for `max-resident-partitions` has been reduced from 10 to 1.
We now rely on the OS Page Cache to provide fast data access to recently loaded
partitions.
