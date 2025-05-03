The `tenzir` command-line utility now supports running multiple pipelines
sequentially, splitting the TQL program whenever a sink is followed by a source.
For example, `from {x: 1} | write_ndjson | save_stdout | from {y: 2} | write_csv
| save_stdout` will start two pipelines. This feature is primarily aimed at
making it easier to write tests.

The `pipeline::detach` operator offers a convenient way of running a pipeline in
the background at the node.
