## [0.2] - 2019.10.30
[0.2]: https://github.com/tenzir/vast/releases/tag/0.2

### Changes

- The query language has been extended to support expression of the form `X == /pattern/`, where `X` is a compatible LHS extractor. Previously, patterns only supports the match operator `~`. The two operators have the same semantics when one operand is a pattern.

- CAF and Broker are no longer required to be installed prior to building VAST. These dependencies are now tracked as git submodules to ensure version compatibility. Specifying a custom build is still possible via the CMake variables `CAF_ROOT_DIR` and `BROKER_ROOT_DIR`.

- When exporting data in `pcap` format, it is no longer necessary to manually restrict the query by adding the predicate `#type == "pcap.packet"` to the expression. This now happens automatically because only this type contains the raw packet data.

- When defining schema attributes in key-value pair form, the value no longer requires double-quotes. For example, `#foo=x` is now the same as `#foo="x"`. The form without double-quotes consumes the input until the next space and does not support escaping. In case an attribute value contains whitespace, double-quotes must be provided, e.g., `#foo="x y z"`.

- The PCAP packet type gained the additional field `community_id` that contains the [Community ID](https://github.com/corelight/community-id-spec) flow hash. This identifier facilitates pivoting to a specific flow from data sources with connnection-level information, such Zeek or Suricata logs.

- Log files generally have some notion of timestamp for recorded events. To make the query language more intuitive, the syntax for querying time points thus changed from `#time` to `#timestamp`. For example, `#time > 2019-07-02+12:00:00` now reads `#timestamp > 2019-07-02+12:00:00`.

- Default schema definitions for certain `import` formats changed from hard-coded to runtime-evaluated. The default location of the schema definition files is `$(dirname vast-executable)`/../share/vast/schema. Currently this is used for the Suricata JSON log reader.

- The default directory name for persistent state changed from `vast` to `vast.db`. This makes it possible to run `./vast` in the current directory without having to specify a different state directory on the command line.

- Nested types are from now on accessed by the `.`-syntax. This means VAST now has a unified syntax to select nested types and fields. For example, what used to be `zeek::http` is now just `zeek.http`.

- The (internal) option `--node` for the `import` and `export` commands has been renamed from `-n` to `-N`, to allow usage of `-n` for `--max-events`.

- To make the export option to limit the number of events to be exported more idiomatic, it has been renamed from `--events,e` to `--max-events,n`. Now `vast export -n 42` generates at most 42 events.

### Features

- The default schema for Suricata has been updated to support the new `suricata.smtp` event type in Suricata 5.

- The `export null` command retrieves data, but never prints anything. Its main purpose is to make benchmarking VAST easier and faster.

- The new `pivot` command retrieves data of a related type. It inspects each event in a query result to find an event of the requested type. If a common field exists in the schema definition of the requested type, VAST will dynamically create a new query to fetch the contextual data according to the type relationship. For example, if two records `T` and `U` share the same field `x`, and the user requests to pivot via `T.x == 42`, then VAST will fetch all data for `U.x == 42`. An example use case would be to pivot from a Zeek or Suricata log entry to the corresponding PCAP packets. VAST uses the field `community_id` to pivot between the logs and the packets. Pivoting is currently implemented for Suricata, Zeek (with [community ID computation](https://github.com/corelight/bro-community-id) enabled), and PCAP.

- The new `infer` command performs schema inference of input data. The command can deduce the input format and creates a schema definition that is sutable to use with the supplied data. Supported input types include Zeek TSV and JSONLD.

- The newly added `count` comman allows counting hits for a query without exporting data.

- Commands now support a `--documentation` option, which returns Markdown-formatted documentation text.

- A new schema for Argus CSV output has been added. It parses the output of `ra(1)`, which produces CSV output when invoked with `-L 0 -c ,`.

- The schema language now supports comments. A double-slash (`//`) begins a comment. Comments last until the end of the line, i.e., until a newline character (`\n`).

- The `import` command now supports CSV formatted data. The type for each column is automatically derived by matching the column names from the CSV header in the input with the available types from the schema definitions.

- Configuring how much status information gets printed to STDERR previously required obscure config settings. From now on, users can simply use `--verbosity=<level>,-v <level>`, where `<level>` is one of `quiet`, `error`, `warn`, `info`, `debug`, or `trace`. However, `debug` and `trace` are only available for debug builds (otherwise they fall back to log level `info`).

- The query expression language now supports *data predicates*, which are a shorthand for a type extractor in combination with an equality operator. For example, the data predicate `6.6.6.6` is the same as `:addr == 6.6.6.6`.

- The `index` object in the output from `vast status` has a new field `statistics` for a high-level summary of the indexed data. Currently, there exists a nested `layouts` objects with per-layout statistics about the number of events indexed.

- The `accountant` object in the output from `vast status` has a new field `log-file` that points to the filesystem path of the accountant log file.

- Data extractors in the query language can now contain a type prefix. This enables an easier way to extract data from a specific type. For example, a query to look for Zeek conn log entries with responder IP address 1.2.3.4 had to be written with two terms, `#type == zeek.conn && id.resp_h == 1.2.3.4`, because the nested id record can occur in other types as well. Such queries can now written more tersely as `zeek.conn.id.resp_h == 1.2.3.4`.

- VAST gained support for importing Suricata JSON logs. The import command has a new suricata format that can ingest EVE JSON output.

- The data parser now supports `count` and `integer` values according to the *International System for Units (SI)*. For example, `1k` is equal to `1000` and `1Ki` equal to `1024`.

- VAST can now ingest JSON data. The `import` command gained the `json` format, which allows for parsing line-delimited JSON (LDJSON) according to a user-selected type with `--type`. The `--schema` or `--schema-file` options can be used in conjunction to supply custom types. The JSON objects in the input must match the selected type, that is, the keys of the JSON object must be equal to the record field names and the object values must be convertible to the record field types.

- For symmetry to the `export` command, the `import` command gained the `--max-events,n` option to limit the number of events that will be imported.

- The `import` command gained the `--listen,l` option to receive input from the network. Currently only UDP is supported. Previously, one had to use a clever netcat pipe with enough receive buffer to achieve the same effect, e.g., `nc -I 1500 -p 4200 | vast import pcap`. Now this pipe degenerates to `vast import pcap -l`.

- The new `--disable-accounting` option shuts off periodic gathering of system telemetry in the accountant actor. This also disables output in the `accounting.log`.

### Bug Fixes

- The user environments `LDFLAGS` were erroneously passed to `ar`. Instead, the user environments `ARFLAGS` are now used.

- Exporting data with `export -n <count>` crashed when `count` was a multiple of the table slice size. The command now works as expected.

- Queries of the form `#type ~ /pattern/` used to be rejected erroneously. The validation code has been corrected and such queries are now working as expected.

- When specifying `enum` types in the schema, ingestion failed because there did not exist an implementation for such types. It is now possible to use define enumerations in schema as expected and query them as strings.

- Queries with the less `<` or greater `>` operators produced off-by-one results for the `duration` when the query contained a finer resolution than the index. The operator now works as expected.

- Timestamps were always printed in millisecond resolution, which lead to loss of precision when the internal representation had a higher resolution. Timestamps are now rendered up to nanosecond resolution - the maximum resolution supported.

- All query expressions in the form `#type != X` were falsely evaluated as `#type == X` and consequently produced wrong results. These expressions now behave as expected.

- Parsers for reading log input that relied on recursive rules leaked memory by creating cycling references. All recursive parsers have been updated to break such cycles and thus no longer leak memory.

- The Zeek reader failed upon encountering logs with a `double` column, as it occurs in `capture_loss.log`. The Zeek parser generator has been fixed to handle such types correctly.

- Some queries returned duplicate events because the archive did not filter the result set properly. This no longer occurs after fixing the table slice filtering logic.

- The `map` data parser did not parse negative values correctly. It was not possible to parse strings of the form `"{-42 -> T}"` because the parser attempted to parse the token for the empty map `"{-}"` instead.

- The CSV printer of the `export` command used to insert 2 superfluous fields when formatting an event: The internal event ID and a deprecated internal timestamp value. Both fields have been removed from the output, bringing it into line with the other output formats.

- When a node terminates during an import, the client process remained unaffected and kept processing input. Now the client terminates when a remote node terminates.

- Evaluation of predicates with negations return incorrect results. For example, the expression `:addr !in 10.0.0.0/8` created a disjunction of all fields to which `:addr` resolved, without properly applying De-Morgan. The same bug also existed for key extractors. De-Morgan is now applied properly for the operations `!in` and `!~`.

## [0.1][0.1] - 2019.02.28
[0.1]: https://github.com/tenzir/vast/releases/tag/0.1

This is the first official release.
