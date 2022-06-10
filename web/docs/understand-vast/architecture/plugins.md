# Plugins

VAST has a plugin system that makes it easy to hook into various places of
the data processing pipeline and add custom functionality in a safe and
sustainable way. A set of customization points allow anyone to add new
functionality that adds CLI commands, receives a copy of the input stream,
spawns queries, or implements integrations with third-party libraries.

There exist **dynamic plugins** that come in the form shared libraries, and
**static plugins** that are compiled into libvast or VAST itself:

![Plugins](/img/plugins.light.png#gh-light-mode-only)
![Plugins](/img/plugins.dark.png#gh-dark-mode-only)

Plugins do not only exist for extensions by third parties, but VAST also
implements core functionality through the plugin API. Such plugins compile as
static plugins. Because they are always built, we call them *native plugins*.

## Plugin types

VAST offers several customization points to exchange or enhance functionality
selectively. Here is a list of available plugin categories and plugin types:

![Plugin types](/img/plugin-types.light.png#gh-light-mode-only)
![Plugin types](/img/plugin-types.dark.png#gh-dark-mode-only)

### Command

A command plugin adds a new command to the `vast` executable, at a configurable
location in the command hierarchy.

The base class `vast::command_plugin` defines a factory function
`make_command()` that returns a new command. Concretely, the return value of
this function is a `std::pair<std::unique_ptr<command>, command::factory>`. The
first component is the command instance, and the second defines the mapping
from command name to command implementation.

### Component

A component plugin spawns a component inside the VAST server process.

The base class `vast::component_plugin` has a typed actor interface
`system::component_plugin_actor` that users must return by overriding the pure
virtual factory `make_component(...)`. The `component_plugin_actor` has the
following type:

```cpp
// See libvast/vast/system/actors.hpp
/// The interface of a COMPONENT PLUGIN actor.
using component_plugin_actor = caf::typed_actor<>
  // Conform to the protocol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>;
```

### Analyzer

The analyzer plugin hooks into the processing path of data by spawning a new
actor inside the server that receives the full stream of table slices. The
analyzer plugin is a refinement of the component plugin.

The base class `vast::analyzer_plugin` has a typed actor interface
`system::analyzer_plugin_actor` that users must return by overriding the pure
virtual factory `make_analyzer(...)`. The `analyzer_plugin_actor` has the
following type:

```cpp
// See libvast/vast/system/actors.hpp
/// The interface of an ANALYZER PLUGIN actor.
using analyzer_plugin_actor = caf::typed_actor<>
  // Conform to the protocol of the STREAM SINK actor for table slices.
  ::extend_with<stream_sink_actor<table_slice>>
  // Conform to the protocol of the COMPONENT PLUGIN actor.
  ::extend_with<component_plugin_actor>;
```

That is, an analyzer must use [CAF
streaming](https://actor-framework.readthedocs.io/en/latest/Streaming.html) to
implement its functionality.

<!--
### Import

The import plugin post-processes the stream of parsed data by injecting a new
actor into the input stream at the source, right before the data flows to the
importer.

### Export

The export plugin pre-processes query results by injecting a new actor into the
output stream, right before the data flows onto the sink.
-->

### Reader

The reader plugin adds a new format to parse input data.

The base class `vast::reader_plugin` has five pure virtual functions that reader
plugins must override.

Reader plugins automatically add the subcommands `vast import <format>` and
`vast spawn source <format>`.

### Writer

The writer plugin adds a new format to print query results.

The base class `vast::writer_plugin` has five pure virtual functions that writer
plugins must override.

Writer plugins automatically add the subcommands `vast export <format>` and
`vast spawn sink <format>`.

### Query Language

A query language plugin adds an alternative parser for a query expression. This
plugin allows for replacing the query *frontend* while using VAST as *backend*
execution engine. For example, you could write a SQL plugin that takes an
expression like `SELECT * FROM zeek.conn WHERE id.orig_h = "1.2.3.4"` and
executes it on historical data or runs it as live query.

The base class `vast::query_plugin` defines a function
`parse()` that returns a `vast::expression`. The function takes a
`std::string_view` that represents the user input to transpile into a VAST
expression.

<!--
### Carrier

The carrier plugin adds a new data transport mechanism to VAST. A carrier is
symmetric in that it sits in front of a reader and behind a writer. Carriers
are orthogonal to the format that they transport on top. Think of it like TCP
and HTTP: a stream of bytes (carrier) and an application layer protocol
(format). Both are conceptually independent.

VAST ships with the following carriers: TCP, UDP, and file (with STDIN/STDOUT
being a special case). However, these are hard-coded and not implemented
through the plugin API.

Good candidates for this type of plugins are message brokers like Kafka,
RabbitMQ, or ZeroMQ.

-->

### Transform

The transform plugin adds a new [transform
step](/docs/understand-vast/query-language/operators) that users can reference in
a [transform definition](/docs/understand-vast/query-language/pipelines).

The base class `transformation_plugin` base class implements the following
virtual function:

```cpp
class transform_plugin {
  virtual transform_step_ptr make_transform_step(const caf::settings&) const = 0;
};
```

The returned transform step must implement at least one of the following
interfaces:

```cpp
class generic_transform_step {
  virtual caf::expected<table_slice> operator()(table_slice&&) const = 0;
};

class arrow_transform_step {
  std::pair<record_type, std::shared_ptr<arrow::RecordBatch>>
  operator()(record_type, std::shared_ptr<arrow::RecordBatch>) const = 0;
};
```

For a full example, take a look at the [example transform
plugin][transform-example-plugin].

[transform-example-plugin]: https://github.com/tenzir/vast/blob/master/examples/plugins/transform/transform_plugin.cpp

### Store

The store plugin adds a new backend store where VAST stores the raw input data.

The base class `vast::store_plugin` has two pure virtual functions to
construct a new store and recover an existing store from persisted data
that users must implement.
