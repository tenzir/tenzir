# Query Language

VAST query language (or VASTQL) allows for flexible extraction of events. It
is currently limited to the [expression language](query-language/expressions) to
filter a subset of data.

## Language Evolution

Moving forward, we plan to go beyond pure filter and add a pipeline-style
dataflow processing engine. This engine exists partially today to
[transform](/docs/use-vast/transform) data, but needs further work to be
available for user queries.

The basic idea is that a query consists of two connected pieces: a *dataset* to
represent a data source and a *pipeline* as a squence of operators to process
the data.

![Query Language](/img/query-language.light.png#gh-light-mode-only)
![Query Language](/img/query-language.dark.png#gh-dark-mode-only)

 To date, a VAST expression takes the role of a dataset and the pipeline is a
 transformation. VAST implements a few operators as custom commands, such as
 `pivot` and `explore`. Stay tuned.
