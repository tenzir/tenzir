Added new index metrics 'index.partitions-created', 'index.partitions-persisted'
and 'index.partitions-persisted-events' which allow monitoring the number of
partitions created and persisted by the index. The former is a pure counter,
and the latter two are reported separately per layout.
