# Changelog

This changelog documents all notable user-facing changes of VAST.

<!--
## Unreleased
### ⚡️ Breaking Changes
### ⚠️ Changes
### 🧬 Experimental Features
### 🎁 Features
### 🐞 Bug Fixes
-->

## Unreleased

- ⚡️ Plugins must define a separate entrypoint in their build scaffolding using
  the argument `ENTRYPOINT` to the CMake function `VASTRegisterPlugin`. If only
  a single value is given to the argument `SOURCES`, it is interpreted as the
  `ENTRYPOINT` automatically.
  [#1549](https://github.com/tenzir/vast/pull/1549)

- 🐞 Plugin unit tests now correctly load and initialize their respective
  plugins.
  [#1549](https://github.com/tenzir/vast/pull/1549)

- 🎁 *Reader Plugins* and *Writer Plugins* are a new family of plugins that add
  import/export formats. The previously optional PCAP format moved into a
  dedicated plugin. Configure with `--with-pcap-plugin` and add `pcap` to
  `vast.plugins` to enable the PCAP plugin.
  [#1549](https://github.com/tenzir/vast/pull/1549)

- 🎁 *Component Plugins* are a new category of plugins that execute code within
  the VAST server process. *Analyzer Plugins* are now a specialization of
  *Component Plugins*, and their API remains unchanged.
  [#1544](https://github.com/tenzir/vast/pull/1544)
  [#1547](https://github.com/tenzir/vast/pull/1547)

- ⚠️ The status output of *Analyzer Plugins* moved from the `importer.analyzers`
  key into the top-level record.
  [#1544](https://github.com/tenzir/vast/pull/1544)

- 🐞 A bug in the parsing of ISO8601 formatted dates that incorrectly adjusted
  the time to the UTC timezone has been fixed.
  [#1537](https://github.com/tenzir/vast/pull/1537)

- 🎁 The `VAST_PLUGIN_DIRS` and `VAST_SCHEMA_DIRS` environment variables allow
  for setting additional plugin and schema directories separated with `:` with
  higher precedence than other plugin and schema directories.
  [#1532](https://github.com/tenzir/vast/pull/1532)
  [#1541](https://github.com/tenzir/vast/pull/1541)

- 🎁 It is now possible to build plugins against an installed VAST. This
  requires a slight adaptation to every plugin's build scaffolding. The example
  plugin was updated accordingly.
  [#1532](https://github.com/tenzir/vast/pull/1532)

- 🐞 Linking against an installed VAST via CMake now correctly resolves VAST's
  dependencies.
  [#1532](https://github.com/tenzir/vast/pull/1532)

- 🐞 The command-line parser no longer crashes when encountering a flag
  with missing value in the last position of a command invocation.
  [#1536](https://github.com/tenzir/vast/pull/1536)

- 🐞 The CSV reader no longer crashes when encountering nested type aliases.
  [#1534](https://github.com/tenzir/vast/pull/1534)

- 🐞 VAST no longer erroneously tries to load explicitly specified plugins
  dynamically that are linked statically.
  [#1528](https://github.com/tenzir/vast/pull/1528)

- 🐞 VAST no longer refuses to start when any of the configuration file
  directories is unreadable, e.g., because VAST is running in a sandbox.
  [#1533](https://github.com/tenzir/vast/pull/1533)

- ⚠️ The Suricata `dns` schema type now defines the `dns.grouped.A` field
  containing a list of all returned addresses.
  [#1531](https://github.com/tenzir/vast/pull/1531)

- 🐞 Custom commands from plugins ending in `start` no longer try to write to
  the server instead of the client log file.
  [#1530](https://github.com/tenzir/vast/pull/1530)

- ⚡️ The previously deprecated usage
  ([#1354](https://github.com/tenzir/vast/pull/1354)) of format-independent
  options after the format in commands is now no longer possible. This affects
  the options `listen`, `read`, `schema`, `schema-file`, `type`, and `uds` for
  import commands and the `write` and `uds` options for export commands.
  [#1529](https://github.com/tenzir/vast/pull/1529)

- ⚡️ Plugins configured via `vast.plugins` in the configuration file can now be
  specified using either the plugin name or the full path to the shared plugin
  library. We no longer allow omitting the extension from specified plugin
  files, and recommend using the plugin name as a more portable solution, e.g.,
  `example` over `libexample` and `/path/to/libexample.so` over
  `/path/to/libexample`.
  [#1527](https://github.com/tenzir/vast/1527)

- ⚠️ We upstreamed the Debian patches provided by
  [@satta](https://github.com/satta). VAST now prefers an installed
  `tsl-robin-map>=0.6.2` to the bundled one unless configured with
  `--with-bundled-robin-map`, and we provide a manpage for `lsvast` if
  `pandoc` is installed.
  [#1515](https://github.com/tenzir/vast/pull/1515)

- ⚡️ The previously deprecated
  ([#1409](https://github.com/tenzir/vast/pull/1409)) option
  `vast.no-default-schema` no longer exists.
  [#1507](https://github.com/tenzir/vast/pull/1409)

- 🎁 The disk monitor gained a new `vast.start.disk-budget-check-binary` option
  that can be used to specify an external binary to determine the size of the
  database directory. This can be useful in cases where `stat()` does not give
  the correct answer, e.g. on compressed filesystems.
  [#1453](https://github.com/tenzir/vast/pull/1433)

- ⚠️ VAST now ships with a schema record type for Suricata's `rfb` event type.
  [#1499](https://github.com/tenzir/vast/pull/1499)
  [@satta](https://github.com/satta)

## [2021.03.25]

### ⚡️ Breaking Changes

- Plugins can now be linked statically against VAST. A new `VASTRegisterPlugin`
  CMake function enables easy setup of the build scaffolding required for
  plugins. Configure with `--with-static-plugins` or build a static binary to
  link all plugins built alongside VAST statically. All plugin build
  scaffoldings must be adapted, older plugins do no longer work.
  [#1445](https://github.com/tenzir/vast/pull/1445)
  [#1452](https://github.com/tenzir/vast/pull/1452)

- The previously deprecated `#timestamp` extractor has been removed from the
  query language entirely. Use `:timestamp` instead.
  [#1399](https://github.com/tenzir/vast/pull/1399)

### ⚠️ Changes

- The default size of table slices (event batches) that is created from
  `vast import` processes has been changed from 1,000 to 1,024.
  [#1396](https://github.com/tenzir/vast/pull/1396)

- The type extractor in the expression language now works with type aliases.
  For example, given the type definition for port from the base schema
  `type port = count`, a search for `:count` will also consider fields of type
  `port`.
  [#1446](https://github.com/tenzir/vast/pull/1446)

- Query latency for expressions that contain concept names has improved
  substantially. For DB sizes in the TB region, and with a large variety of
  event types, queries with a high selectivity experience speedups of up to 5x.
  [#1433](https://github.com/tenzir/vast/pull/1433)

- The zeek-to-vast utility was moved to the
  [tenzir/zeek-vast](https://github.com/tenzir/zeek-vast) repository. All
  options related to zeek-to-vast and the bundled Broker submodule were removed.
  [#1435](https://github.com/tenzir/vast/1435)

- The option `vast.no-default-schema` is deprecated, as it is no longer needed
  to override types from bundled schemas.
  [#1409](https://github.com/tenzir/vast/pull/1409)

- VAST now ships with schema record types for Suricata's `mqtt` and `anomaly`
  event types. [#1408](https://github.com/tenzir/vast/pull/1408)
  [@satta](https://github.com/satta)

### 🎁 Features

- VAST now supports nested records in Arrow table slices and in the JSON import,
  e.g., data of type `list<record<name: string, age: count>`. While nested
  record fields are not yet queryable, ingesting such data will no longer cause
  VAST to crash. MessagePack table slices don't support records in lists yet.
  [#1429](https://github.com/tenzir/vast/pull/1429)

- The schema language now supports 4 operations on record types: `+` combines
  the fields of 2 records into a new record. `<+` and `+>` are variations of `+`
  that give precedence to the left and right operand respectively. `-` creates a
  record with the field specified as its right operand removed.
  [#1407](https://github.com/tenzir/vast/pull/1407)
  [#1487](https://github.com/tenzir/vast/pull/1487)
  [#1490](https://github.com/tenzir/vast/pull/1490)


### 🐞 Bug Fixes

- Enabling the disk budget feature no longer prevents the server process from
  exiting after it was stopped.
  [#1495](https://github.com/tenzir/vast/pull/1495)

- A race condition during server shutdown could lead to an invariant violation,
  resulting in a firing assertion. Streamlining the shutdown logic resolved the
  issue.
  [#1473](https://github.com/tenzir/vast/pull/1473)
  [#1485](https://github.com/tenzir/vast/pull/1485)

- Insufficient permissions for one of the paths in the `schema-dirs` option
  would lead to a crash in `vast start`.
  [#1472](https://github.com/tenzir/vast/pull/1472)

- A query for a field or field name suffix that matches multiple fields of
  different types would erroneously return no results.
  [#1447](https://github.com/tenzir/vast/pull/1447)

- The disk monitor now correctly erases partition synopses from the meta index.
  [#1450](https://github.com/tenzir/vast/pull/1450)

- The archive, index, source, and sink components now report metrics when idle
  instead of omitting them entirely. This allows for distinguishing between idle
  and not running components from the metrics.
  [#1451](https://github.com/tenzir/vast/pull/1451)

- VAST no longer crashes when the disk monitor tries to calculate the size of
  the database while files are being deleted. Instead, it will retry after the
  configured scan interval. [#1458](https://github.com/tenzir/vast/1458)

- The JSON parser now accepts data with numerical or boolean values in fields
  that expect strings according to the schema. VAST converts these values into
  string representations.
  [#1439](https://github.com/tenzir/vast/pull/1439)

- Data that was ingested before the deprecation of the `#timestamp` attribute
  wasn't exported correctly with newer versions. This is now corrected.
  [#1432](https://github.com/tenzir/vast/pull/1432)

- Some non-null pointers were incorrectly rendered as `*nullptr` in log
  messages. [#1430](https://github.com/tenzir/vast/pull/1430)

## [2021.02.24]

### ⚡️ Breaking Changes

- The previously deprecated options `vast.spawn.importer.ids` and
  `vast.schema-paths` no longer work. Furthermore, queries spread over multiple
  arguments are now disallowed instead of triggering a deprecation warning.
  [#1374](https://github.com/tenzir/vast/pull/1374)

- The special meaning of the `#timestamp` attribute has been removed from
  the schema language. Timestamps can from now on be marked as such by using
  the `timestamp` type instead. Queries of the form `#timestamp <op> value`
  remain operational but are deprecated in favor of `:timestamp`. Note that
  this change also affects `:time` queries, which aren't supersets of
  `#timestamp` queries any longer.
  [#1388](https://github.com/tenzir/vast/pull/1388)

- All options in `vast.metrics.*` had underscores in their names replaced
  with dashes to align with other options. For example, `vast.metrics.file_sink`
  is now `vast.metrics.file-sink`. The old options no longer work.
  [#1368](https://github.com/tenzir/vast/pull/1368)

- User-supplied schema files are now picked up from
  `<SYSCONFDIR>/vast/schema` and `<XDG_CONFIG_HOME>/vast/schema` instead of
  `<XDG_DATA_HOME>/vast/schema`.
  [#1372](https://github.com/tenzir/vast/pull/1372)

- VAST now requires [{fmt} >= 5.2.1](https://fmt.dev) to be installed.
  [#1330](https://github.com/tenzir/vast/pull/1330)

- VAST switched to [spdlog >= 1.5.0](https://github.com/gabime/spdlog) for
  logging. For users, this means: The `vast.console-format` and
  `vast.file-format` now must be specified using the spdlog pattern syntax as
  described
  [here](https://github.com/gabime/spdlog/wiki/3.-Custom-formatting#pattern-flags).
  All settings under `caf.logger.*` are now ignored by VAST, and only the
  `vast.*` counterparts are used for logger configuration.
  [#1223](https://github.com/tenzir/vast/pull/1223)
  [#1328](https://github.com/tenzir/vast/pull/1328)
  [#1334](https://github.com/tenzir/vast/pull/1334)
  [#1390](https://github.com/tenzir/vast/pull/1390)
  [@a4z](https://github.com/a4z)

### ⚠️ Changes

- The output of `vast help` and `vast documentation` now goes to *stdout*
  instead of to stderr. Erroneous invocations of `vast` also print the helptext,
  but in this case the output still goes to stderr to avoid interference with
  downstream tooling. [#1385](https://github.com/tenzir/vast/pull/1385)

- The query normalizer interprets value predicates of type `subnet` more
  broadly: given a subnet `S`, the parser expands this to the expression
  `:subnet == S || :addr in S`. This change makes it easier to search for IP
  addresses belonging to a specific subnet.
  [#1373](https://github.com/tenzir/vast/pull/1373)

- The `infer` command has an improved heuristic for the number types `int`,
  `count`, and `real`. [#1343](https://github.com/tenzir/vast/pull/1343)
  [#1356](https://github.com/tenzir/vast/pull/1356)
  [@ngrodzitski](https://github.com/ngrodzitski)

- The options `listen`, `read`, `schema`, `schema-file`, `type`, and `uds` can
  from now on be supplied to the `import` command directly. Similarly, the
  options `write` and `uds` can be supplied to the `export` command. All options
  can still be used after the format subcommand, but that usage is deprecated.
  [#1354](https://github.com/tenzir/vast/pull/1354)

- Schema parsing now uses a 2-pass loading phase so that type aliases can
  reference other types that are later defined in the same directory.
  Additionally, type definitions from already parsed schema dirs can be
  referenced from schema types that are parsed later. Types can also be
  redefined in later directories, but a type can not be defined twice in the
  same directory. [#1331](https://github.com/tenzir/vast/pull/1331)

### 🧬 Experimental Features

- [Sigma](https://github.com/Neo23x0/sigma) rules are now a valid format to
  represent query expression. VAST parses the `detection` attribute of a rule
  and translates it into a native query expression. To run a query using a Sigma
  rule, pass it on standard input, e.g., `vast export json < rule.yaml`.
  [#1379](https://github.com/tenzir/vast/pull/1379)

### 🎁 Features

- The type extractor in the expression language now works with user defined
  types. For example the type `port` is defined as `type port = count` in the
  base schema. This type can now be queried with an expression like `:port ==
  80`. [#1382](https://github.com/tenzir/vast/pull/1382)

- The new options `vast.metrics.file-sink.real-time` and
  `vast.metrics.uds-sink.real-time` enable real-time metrics reporting for the
  file sink and UDS sink respectively.
  [#1368](https://github.com/tenzir/vast/pull/1368)

- The meta index now stores partition synopses in separate files. This will
  decrease restart times for systems with large databases, slow disks and
  aggressive `readahead` settings. A new config setting `vast.meta-index-dir`
  allows storing the meta index information in a separate directory.
  [#1330](https://github.com/tenzir/vast/pull/1330)
  [#1376](https://github.com/tenzir/vast/pull/1376)

- The JSON import now always relies upon [simdjson](https://simdjson.org). The
  previously experimental `--simdjson` option to the `vast import
  json|suricata|zeek-json` commands no longer exist as the feature is considered
  stable. [#1343](https://github.com/tenzir/vast/pull/1343)
  [#1356](https://github.com/tenzir/vast/pull/1356)
  [@ngrodzitski](https://github.com/ngrodzitski)

- VAST rotates server logs by default. The new config options
  `vast.disable-log-rotation` and `vast.log-rotation-threshold` can be used to
  control this behaviour. [#1223](https://github.com/tenzir/vast/pull/1223)
  [#1362](https://github.com/tenzir/vast/pull/1362)

### 🐞 Bug Fixes

- A bug in the new simdjson based JSON reader introduced in
  [#1356](https://github.com/tenzir/vast/pull/1356) could trigger an assertion
  in the `vast import` process if an input field could not be converted to the
  field type in the target layout. This is no longer the case.
  [#1386](https://github.com/tenzir/vast/pull/1386)

- An ordering issue introduced in
  [#1295](https://github.com/tenzir/vast/pull/1295) that could lead to a
  segfault with long-running queries was reverted.
  [#1381](https://github.com/tenzir/vast/pull/1381)

## [2021.01.28]

### ⚡️ Breaking Changes

- The GitHub CI changed to Debian Buster and produces Debian artifacts instead
  of Ubuntu artifacts. Similarly, the Docker images we provide on
  [dockerhub](https://hub.docker.com/r/tenzir/vast) use Debian Buster as base
  image. To build Docker images locally, users must set `DOCKER_BUILDKIT=1` in
  the build environment.  [#1294](https://github.com/tenzir/vast/pull/1294)

- The new short options `-v`, `-vv`, `-vvv`, `-q`, `-qq`, and `-qqq` map onto
  the existing verbosity levels. The existing short syntax, e.g., `-v debug`, no
  longer works. [#1244](https://github.com/tenzir/vast/pull/1244)

### ⚠️ Changes

- The option `vast.schema-paths` is renamed to `vast.schema-dirs`. The old
  option is deprecated and will be removed in a future release.
  [#1287](https://github.com/tenzir/vast/pull/1287)

- VAST preserves nested JSON objects in events instead of formatting them in a
  flattened form when exporting data with `vast export json`. The old behavior
  can be enabled with `vast export json --flatten`.
  [#1257](https://github.com/tenzir/vast/pull/1257)
  [#1289](https://github.com/tenzir/vast/pull/1289)

- `vast start` prints the endpoint it is listening on when providing the option
  `--print-endpoint`.
  [#1271](https://github.com/tenzir/vast/pull/1271)

### 🧬 Experimental Features

- VAST relies on [simdjson](https://github.com/simdjson/simdjson) for JSON
  parsing. The substantial gains in throughput shift the bottleneck of the
  ingest path from parsing input to indexing at the node. To use the (yet
  experimental) feature, use `vast import json|suricata|zeek-json --simdjson`.
  [#1230](https://github.com/tenzir/vast/pull/1230)
  [#1246](https://github.com/tenzir/vast/pull/1246)
  [#1281](https://github.com/tenzir/vast/pull/1281)
  [#1314](https://github.com/tenzir/vast/pull/1314)
  [#1315](https://github.com/tenzir/vast/pull/1315)
  [@ngrodzitski](https://github.com/ngrodzitski)

- VAST features a new plugin framework to support efficient customization points
  at various places of the data processing pipeline. There exist several base
  classes that define an interface, e.g., for adding new commands or spawning a
  new actor that processes the incoming stream of data. The directory
  `examples/plugins/example` contains an example plugin.
  [#1208](https://github.com/tenzir/vast/pull/1208)
  [#1264](https://github.com/tenzir/vast/pull/1264)
  [#1275](https://github.com/tenzir/vast/pull/1275)
  [#1282](https://github.com/tenzir/vast/pull/1282)
  [#1285](https://github.com/tenzir/vast/pull/1285)
  [#1287](https://github.com/tenzir/vast/pull/1287)
  [#1302](https://github.com/tenzir/vast/pull/1302)
  [#1307](https://github.com/tenzir/vast/pull/1307)
  [#1316](https://github.com/tenzir/vast/pull/1316)

### 🎁 Features

- The output of `vast status` contains detailed memory usage information about
  active and cached partitions.
  [#1297](https://github.com/tenzir/vast/pull/1297)

- VAST installations bundle a LICENSE.3rdparty file alongside the regular
  LICENSE file that lists all embedded code that is under a separate license.
  [#1306](https://github.com/tenzir/vast/pull/1306)

- VAST queries also accept `nanoseconds`, `microseconds`, `milliseconds`
  `seconds` and `minutes` as units for a duration.
  [#1265](https://github.com/tenzir/vast/pull/1265)

- The new `import zeek-json` command allows for importing line-delimited Zeek
  JSON logs as produced by the
  [json-streaming-logs](https://github.com/corelight/json-streaming-logs)
  package. Unlike stock Zeek JSON logs, where one file contains exactly one log
  type, the streaming format contains different log event types in a single
  stream and uses an additional `_path` field to disambiguate the log type. For
  stock Zeek JSON logs, use the existing `import json` with the `-t` flag to
  specify the log type. [#1259](https://github.com/tenzir/vast/1259)

### 🐞 Bug Fixes

- A potential race condition that could lead to a hanging export if a partition
  was persisted just as it was scanned no longer exists.
  [#1295](https://github.com/tenzir/vast/pull/1295)

- Disk monitor quota settings not ending in a 'B' are no longer silently
  discarded. [#1278](https://github.com/tenzir/vast/pull/1278)

- Line based imports correctly handle read timeouts that occur in the middle of
  a line. [#1276](https://github.com/tenzir/vast/pull/1276)

- For relocatable installations, the list of schema loading paths does not
  include a build-time configured path any more.
  [#1249](https://github.com/tenzir/vast/pull/1249)

- Values in JSON fields that can't be converted to the type that is specified in
  the schema won't cause the containing event to be dropped any longer.
  [#1250](https://github.com/tenzir/vast/pull/1250)

- Invalid Arrow table slices read from disk no longer trigger a segmentation
  fault. Instead, the invalid on-disk state is ignored.
  [#1247](https://github.com/tenzir/vast/pull/1247)

- Manually specified configuration files may reside in the default location
  directories. Configuration files can be symlinked.
  [#1248](https://github.com/tenzir/vast/pull/1248)

## [2020.12.16]

### ⚡️ Breaking Changes

- The build configuration of VAST received a major overhaul. Inclusion of
  libvast in other procects via `add_subdirectory(path/to/vast)` is now easily
  possible. The names of all build options were aligned, and the new build
  summary shows all available options.
  [#1175](https://github.com/tenzir/vast/pull/1175)

- The `port` type is no longer a first-class type. The new way to represent
  transport-layer ports relies on `count` instead. In the schema, VAST ships
  with a new alias `type port = count` to keep existing schema definitions in
  tact.  However, this is a breaking change because the on-disk format and Arrow
  data representation changed. Queries with `:port` type extractors no longer
  work.  Similarly, the syntax `53/udp` no longer exists; use `count` syntax
  `53` instead. Since most `port` occurrences do not carry a known
  transport-layer type, and the type information exists typically in a separate
  field, removing `port` as native type streamlines the data model.
  [#1187](https://github.com/tenzir/vast/pull/1187)

- Archive segments no longer include an additional, unnecessary version
  identifier. We took the opportunity to clean this up bundled with the other
  recent breaking changes. [#1168](https://github.com/tenzir/vast/pull/1168)

- The on-disk format for table slices now supports versioning of table slice
  encodings. This breaking change makes it so that adding further encodings or
  adding new versions of existing encodings is possible without breaking again
  in the future. [#1143](https://github.com/tenzir/vast/pull/1143)
  [#1157](https://github.com/tenzir/vast/pull/1157)
  [#1160](https://github.com/tenzir/vast/pull/1160)
  [#1165](https://github.com/tenzir/vast/pull/1165)

- CAF-encoded table slices no longer exist. As such, the option
  `vast.import.batch-encoding` now only supports `arrow` and `msgpack` as
  arguments. [#1142](https://github.com/tenzir/vast/pull/1142)

- The `splunk-to-vast` script has a new name: `taxonomize`. The script now also
  generates taxonomy declarations for Azure Sentinel.
  [#1134](https://github.com/tenzir/vast/pull/1134)

### ⚠️ Changes

- The `zeek` export format now strips off the prefix `zeek.` to ensure full
  compatibility with regular Zeek output. For all non-Zeek types, the prefix
  remains intact. [#1205](https://github.com/tenzir/vast/pull/1205)

- Installed schema definitions now reside in `<datadir>/vast/schema/types`,
  taxonomy definitions in `<datadir>/vast/schema/taxonomy`, and concept
  definitions in `<datadir/vast/schema/concepts`, as opposed to them all being
  in the schema directory directly. When overriding an existing installation,
  you _may_ have to delete the old schema definitions by hand.
  [#1194](https://github.com/tenzir/vast/pull/1194)

- The Suricata schemas received an overhaul: there now exist `vlan` and
  `in_iface` fields in all types. In addition, VAST ships with new types for
  `ikev2`, `nfs`, `snmp`, `tftp`, `rdp`, `sip` and `dcerpc`. The `tls` type gets
  support for the additional `sni` and `session_resumed` fields.
  [#1237](https://github.com/tenzir/vast/pull/1237)
  [#1176](https://github.com/tenzir/vast/pull/1176)
  [#1180](https://github.com/tenzir/vast/pull/1180)
  [#1186](https://github.com/tenzir/vast/pull/1186)
  [@satta](https://github.com/satta)

- VAST now listens on port 42000 instead of letting the operating system choose
  the port if the option `vast.endpoint` specifies an endpoint without a port.
  To restore the old behavior, set the port to 0 explicitly.
  [#1170](https://github.com/tenzir/vast/pull/1170)

- The default segment size in the archive is now 1 GiB. This reduces
  fragmentation of the archive meta data and speeds up VAST startup time.
  [#1166](https://github.com/tenzir/vast/pull/1166)

- VAST now processes the schema directory recursively, as opposed to stopping at
  nested directories. [#1154](https://github.com/tenzir/vast/pull/1154)

- VAST does not produce metrics by default any more. The option
  `--disable-metrics` has been renamed to `--enable-metrics` accordingly.
  [#1137](https://github.com/tenzir/vast/pull/1137)

- VAST no longer requires you to manually remove a stale PID file from a
  no-longer running `vast` process. Instead, VAST prints a warning and
  overwrites the old PID file.
  [#1128](https://github.com/tenzir/vast/pull/1128)

### 🧬 Experimental Features

- The expression language gained support for the `#field` meta extractor. It is
  the complement for `#type` and uses suffix matching for field names at the
  layout level. [#1228](https://github.com/tenzir/vast/pull/1228)

- The query language now supports models. Models combine a list of concepts into
  a semantic unit that can be fulfiled by an event. If the type of an event
  contains a field for every concept in a model. Turn to [the
  documentation](https://docs.tenzir.com/vast/data-model/taxonomies/#models) for
  more information. [#1185](https://github.com/tenzir/vast/pull/1185)
  [#1228](https://github.com/tenzir/vast/pull/1228)

- VAST now ships with its own taxonomy and basic concept definitions for
  Suricata, Zeek, and Sysmon. [#1135](https://github.com/tenzir/vast/pull/1135)
  [#1150](https://github.com/tenzir/vast/pull/1150)

### 🎁 Features

- Low-selectivity queries of string (in)equality queries now run up to 30x
  faster, thanks to more intelligent selection of relevant index partitions.
  [#1214](https://github.com/tenzir/vast/pull/1214)

- On Linux, VAST now contains a set of built-in USDT tracepoints that can be
  used by tools like `perf` or `bpftrace` when debugging. Initially, we provide
  the two tracepoints `chunk_make` and `chunk_destroy`, which trigger every time
  a `vast::chunk` is created or destroyed.
  [#1206](https://github.com/tenzir/vast/pull/1206)

- The new `dump` command prints configuration and schema-related information.
  The implementation allows for printing all registered concepts and models, via
  `vast dump concepts` and `vast dump models`. The flag to `--yaml` to `dump`
  switches from JSON to YAML output, such that it confirms to the taxonomy
  configuration syntax. [#1196](https://github.com/tenzir/vast/pull/1196)
  [#1233](https://github.com/tenzir/vast/pull/1233)

- A new key 'meta-index-bytes' appears in the status output generated by `vast
  status --detailed`. [#1193](https://github.com/tenzir/vast/pull/1193)

- The storage required for index IP addresses has been optimized. This should
  result in significantly reduced memory usage over time, as well as faster
  restart times and reduced disk space requirements.
  [#1172](https://github.com/tenzir/vast/pull/1172)
  [#1200](https://github.com/tenzir/vast/pull/1200)
  [#1216](https://github.com/tenzir/vast/pull/1216)

- The new option `--print-bytesizes` of `lsvast` prints information about the
  size of certain fields of the flatbuffers inside a VAST database directory.
  [#1149](https://github.com/tenzir/vast/pull/1149)

- The new option `vast.client-log-file` enables client-side logging. By default,
  VAST only writes log files for the server process.
  [#1132](https://github.com/tenzir/vast/pull/1132)

### 🐞 Bug Fixes

- Concepts that reference other concepts are now loaded correctly from their
  definition. [#1236](https://github.com/tenzir/vast/pull/1236)

- The `vast status` command does not collect status information from sources and
  sinks any longer. They were often too busy to respond, leading to a long delay
  before the command completed.
  [#1234](https://github.com/tenzir/vast/pull/1234)

- The summary log message of `vast export` now contains the correct number of
  candidate events. [#1228](https://github.com/tenzir/vast/pull/1228)

- The index no longer causes exporters to deadlock when the meta index produces
  false positives. [#1225](https://github.com/tenzir/vast/pull/1225)

- The index now correctly drops further results when queries finish early, thus
  improving the performance of queries for a limited number of events.
  [#1209](https://github.com/tenzir/vast/pull/1209)

- The index no longer crashes when too many parallel queries are running.
  [#1210](https://github.com/tenzir/vast/pull/1210)

- The type registry now detects and handles breaking changes in schemas, e.g.,
  when a field type changes or a field is dropped from record.
  [#1195](https://github.com/tenzir/vast/pull/1195)

- VAST no longer blocks when an invalid query operation is issued.
  [#1189](https://github.com/tenzir/vast/pull/1189)

- `vast import` no longer stalls when it doesn't receive any data for more than
  10 seconds. [#1136](https://github.com/tenzir/vast/pull/1136)

- The output of `vast status --detailed` now contains informations about
  runnings sinks, e.g., `vast export <format> <query>` processes.
  [#1155](https://github.com/tenzir/vast/pull/1155)

- The `vast.yaml.example` contained syntax errors. The example config file now
  works again. [#1145](https://github.com/tenzir/vast/pull/1145)

- VAST no longer starts if the specified config file does not exist.
  [#1147](https://github.com/tenzir/vast/pull/1147)

## [2020.10.29]

<!-- ### ⚡️ Breaking Changes -->

### ⚠️ Changes

- The default database directory moved to `/var/lib/vast` for Linux deployments.
  [#1116](https://github.com/tenzir/vast/pull/1116)

- Log files are now less verbose because class and function names are not
  printed on every line. [#1107](https://github.com/tenzir/vast/pull/1107)

- The new option `import.read-timeout` allows for setting an input timeout for
  low volume sources. Reaching the timeout causes the current batch to be
  forwarded immediately. This behavior was previously controlled by
  `import.batch-timeout`, which now only controls the maximum buffer time before
  the source forwards batches to the server.
  [#1096](https://github.com/tenzir/vast/pull/1096)

- VAST will now warn if a client command connects to a server that runs on a
  different version of the vast binary.
  [#1098](https://github.com/tenzir/vast/pull/1098)

### 🧬 Experimental Features

- A new *disk monitor* component can now monitor the database size and delete
  data that exceeds a specified threshold. Once VAST reaches the maximum amount
  of disk space, the disk monitor deletes the oldest data. The command-line
  options `--disk-quota-high`, `--disk-quota-low`, and
  `--disk-quota-check-interval` control the rotation behavior.
  [#1103](https://github.com/tenzir/vast/pull/1103)

- The query language now comes with support for concepts, the first part of
  taxonomies. Concepts is a mechanism to unify the various naming schemes of
  different data formats into a single, coherent nomenclature.
  [#1102](https://github.com/tenzir/vast/pull/1102)

### 🎁 Features

- The expression language now accepts records without field names. For
  example,`id == <192.168.0.1, 41824, 143.51.53.13, 25, "tcp">` is now valid
  syntax and instantiates a record with 5 fields. Note: expressions with records
  currently do not execute. [#1129](https://github.com/tenzir/vast/pull/1129)

- The new options `vast.segments` and `vast.max-segment-size` control how the
  archive generates segments. [#1103](https://github.com/tenzir/vast/pull/1103)

- The new script `splunk-to-vast` converts a splunk CIM model file in JSON to a
  VAST taxonomy. For example, `splunk-to-vast < Network_Traffic.json` renders
  the concept definitions for the *Network Traffic* datamodel. The generated
  taxonomy does not include field definitions, which users should add separately
  according to their data formats.
  [#1121](https://github.com/tenzir/vast/pull/1121)

- When running VAST under systemd supervision, it is now possible to use the
  `Type=notify` directive in the unit file to let VAST notify the service
  manager when it becomes ready.
  [#1091](https://github.com/tenzir/vast/pull/1091)

### 🐞 Bug Fixes

- The `lsvast` tool failed to print FlatBuffers schemas correctly. The output
  now renders correctly. [#1123](https://github.com/tenzir/vast/pull/1123)

- VAST no longer opens a random public port, which used to be enabled in the
  experimental VAST cluster mode in order to transparently establish a full
  mesh. [#1110](https://github.com/tenzir/vast/pull/1110)

- The `vast status --detailed` command now correctly shows the status of all
  sources, i.e., `vast import` or `vast spawn source` commands.
  [#1109](https://github.com/tenzir/vast/pull/1109)

- Sources that receive no or very little input do not block `vast status` any
  longer. [#1096](https://github.com/tenzir/vast/pull/1096)

- The lookup for schema directories now happens in a fixed order.
  [#1086](https://github.com/tenzir/vast/pull/1086)

## [2020.09.30]

### ⚡️ Breaking Changes

- Data exported in the Apache Arrow format now contains the name of the payload
  record type in the metadata section of the schema.
  [#1072](https://github.com/tenzir/vast/pull/1072)

- The persistent storage format of the index now uses FlatBuffers.
  [#863](https://github.com/tenzir/vast/pull/863)

### ⚠️ Changes

- All configuration options are now grouped into `vast` and `caf` sections,
  depending on whether they affect VAST itself or are handed through to the
  underlying actor framework CAF directly. Take a look at the bundled
  `vast.yaml.example` file for an explanation of the new layout.
  [#1073](https://github.com/tenzir/vast/pull/1073)

- We refactored the index architecture to improve stability and responsiveness.
  This includes fixes for several shutdown issues.
  [#863](https://github.com/tenzir/vast/pull/863)

- The options that affect batches in the `import` command received new, more
  user-facing names: `import.table-slice-type`, `import.table-slice-size`, and
  `import.read-timeout` are now called `import.batch-encoding`,
  `import.batch-size`, and `import.read-timeout` respectively.
  [#1058](https://github.com/tenzir/vast/pull/1058)

- The prioprietary VAST configuration file has changed to the more ops-friendly
  industry standard YAML. This change introduced also a new dependency:
  [yaml-cpp](https://github.com/jbeder/yaml-cpp) version 0.6.2 or greater. The
  top-level `vast.yaml.example` illustrates how the new YAML config looks like.
  Please rename existing configuration files from `vast.conf` to `vast.yaml`.
  VAST still reads `vast.conf` but will soon only look for `vast.yaml` or
  `vast.yml` files in available configuration file paths.
  [#1045](https://github.com/tenzir/vast/pull/1045)
  [#1055](https://github.com/tenzir/vast/pull/1055)
  [#1059](https://github.com/tenzir/vast/pull/1059)
  [#1062](https://github.com/tenzir/vast/pull/1062)

- The global VAST configuration now always resides in
  `<sysconfdir>/vast/vast.conf`, and bundled schemas always in
  `<datadir>/vast/schema/`. VAST no longer supports reading a `vast.conf` file
  in the current working directory.
  [#1036](https://github.com/tenzir/vast/pull/1036)

- The JSON export format now renders `duration` and `port` fields using strings
  as opposed to numbers. This avoids a possible loss of information and enables
  users to re-use the output in follow-up queries directly.
  [#1034](https://github.com/tenzir/vast/pull/1034)

- The delay between the periodic log messages for reporting the current event
  rates has been increased to 10 seconds.
  [#1035](https://github.com/tenzir/vast/pull/1035)

### 🧬 Experimental Features

- The `vast get` command has been added. It retrieves events from the database
  directly by their ids. [#938](https://github.com/tenzir/vast/pull/938)

### 🎁 Features

- VAST now ships with a new tool `lsvast` to display information about the
  contents of a VAST database directory. See `lsvast --help` for usage
  instructions. [#863](https://github.com/tenzir/vast/pull/863)

- VAST now merges the contents of all used configuration files instead of using
  only the most user-specific file. The file specified using `--config` takes
  the highest precedence, followed by the user-specific path
  `${XDG_CONFIG_HOME:-${HOME}/.config}/vast/vast.conf`, and the compile-time
  path `<sysconfdir>/vast/vast.conf`.
  [#1040](https://github.com/tenzir/vast/pull/1040)

- The output of the `status` command was restructured with a strong focus on
  usability. The new flags `--detailed` and `--debug` add additional content to
  the output. [#995](https://github.com/tenzir/vast/pull/995)

- VAST now supports the XDG base directory specification: The `vast.conf` is now
  found at `${XDG_CONFIG_HOME:-${HOME}/.config}/vast/vast.conf`, and schema
  files at `${XDG_DATA_HOME:-${HOME}/.local/share}/vast/schema/`. The
  user-specific configuration file takes precedence over the global
  configuration file in `<sysconfdir>/vast/vast.conf`.
  [#1036](https://github.com/tenzir/vast/pull/1036)

### 🐞 Bug Fixes

- 🐞 Stalled sources that were unable to generate new events no longer stop
  import processes from shutting down under rare circumstances.
  [#1058](https://github.com/tenzir/vast/pull/1058)

## [2020.08.28]

### ⚡️ Breaking Changes

- We now bundle a patched version of CAF, with a changed ABI. This means that if
  you're linking against the bundled CAF library, you also need to distribute
  that library so that VAST can use it at runtime.  The versions are API
  compatible so linking against a system version of CAF is still possible and
  supported. [#1020](https://github.com/tenzir/vast/pull/1020)

### ⚠️ Changes

- The `vector` type has been renamed to `list`. In an effort to streamline the
  type system vocabulary, we favor `list` over `vector` because it's closer to
  existing terminology (e.g., Apache Arrow). This change requires updating
  existing schemas by changing `vector<T>` to `list<T>`.
  [#1016](https://github.com/tenzir/vast/pull/1016)

- The `set` type has been removed. Experience with the data model showed that
  there is no strong use case to separate sets from vectors in the core.  While
  this may be useful in programming languages, VAST deals with immutable data
  where set constraints have been enforced upstream. This change requires
  updating existing schemas by changing `set<T>` to `vector<T>`. In the query
  language, the new symbol for the empty `map` changed from `{-}` to `{}`, as it
  now unambiguously identifies `map` instances.
  [#1010](https://github.com/tenzir/vast/pull/1010)

- The expression field parser now allows the '-' character.
  [#999](https://github.com/tenzir/vast/pull/999)

<!-- ### 🧬 Experimental Features -->

### 🎁 Features

- The default schema for Suricata has been updated to support the `suricata.ftp`
  and `suricata.ftp_data` event types.
  [#1009](https://github.com/tenzir/vast/pull/1009)

- VAST now prints the location of the configuration file that is used.
  [#1009](https://github.com/tenzir/vast/pull/1009)

- VAST now writes a PID lock file on startup to prevent multiple server
  processes from accessing the same persistent state. The `pid.lock` file
  resides in the `vast.db` directory.
  [#1001](https://github.com/tenzir/vast/pull/1001)

### 🐞 Bug Fixes

- VAST did not terminate when a critical component failed during startup.  VAST
  now binds the lifetime of the node to all critical components.
  [#1028](https://github.com/tenzir/vast/pull/1028)

- VAST would overwrite existing on-disk state data when encountering a partial
  read during startup. This state-corrupting behavior no longer exists.
  [#1026](https://github.com/tenzir/vast/pull/1026)

- Incomplete reads have not been handled properly, which manifested for files
  larger than 2GB. On macOS, writing files larger than 2GB may have failed
  previously. VAST now respects OS-specific constraints on the maximum block
  size. [#1025](https://github.com/tenzir/vast/pull/1025)

- The shutdown process of the server process could potentially hang forever.
  VAST now uses a 2-step procedure that first attempts to terminate all
  components cleanly. If that fails, it will attempt a hard kill afterwards, and
  if that fails after another timeout, the process will call `abort(3)`.
  [#1005](https://github.com/tenzir/vast/pull/1005)

- When running VAST under heavy load, CAF stream slot ids could wrap around
  after a few days and deadlock the system. As a workaround, we extended the
  slot id bit width to make the time until this happens unrealistically large.
  [#1020](https://github.com/tenzir/vast/pull/1020)

- Some file descriptors remained open when they weren't needed any more.  This
  descriptor leak has been fixed.
  [#1018](https://github.com/tenzir/vast/pull/1018)

- Importing JSON no longer fails for JSON fields containing `null` when the
  corresponding VAST type in the schema is a non-trivial type like
  `vector<string>`. [#1009](https://github.com/tenzir/vast/pull/1009)

- The port encoding for Arrow-encoded table slices is now host-independent and
  always uses network-byte order.
  [#1007](https://github.com/tenzir/vast/pull/1007)

- When continuous query in a client process terminated, the node did not clean
  up the corresponding server-side state. This memory leak no longer exists.
  [#1006](https://github.com/tenzir/vast/pull/1006)

- A bug in the expression parser prevented the correct parsing of fields
  starting with either 'F' or 'T'.
  [#999](https://github.com/tenzir/vast/pull/999)

- MessagePack-encoded table slices now work correctly for nested container
  types. [#984](https://github.com/tenzir/vast/pull/984)

## [2020.07.28]

### ⚡️ Breaking Changes

- [FlatBuffers](https://google.github.io/flatbuffers/) is now a required
  dependency for VAST. The archive and the segment store use FlatBuffers to
  store and version their on-disk persistent state.
  [#972](https://github.com/tenzir/vast/pull/972)

### ⚠️ Changes

- VAST now recognizes `/etc/vast/schema` as an additional default directory
  for schema files. [#980](https://github.com/tenzir/vast/pull/980)

- The suricata schema file contains new type definitions for the stats, krb5,
  smb, and ssh events. [#954](https://github.com/tenzir/vast/pull/954)
  [#986](https://github.com/tenzir/vast/pull/986)

<!-- ### 🧬 Experimental Features -->

### 🎁 Features

- We open-sourced our [MessagePack](http://msgpack.org)-based table slice
  implementation, which provides a compact row-oriented encoding of data. This
  encoding works well for binary formats (e.g., PCAP) and access patterns that
  involve materializing entire rows. The MessagePack table slice is the new
  default when Apache Arrow is unavailable. To enable parsing into MessagePack,
  you can pass `--table-slice-type=msgpack` to the `import` command, or set the
  configuration option `import.table-slice-type` to `'msgpack'`.
  [#975](https://github.com/tenzir/vast/pull/975)

- Starting with this release, installing VAST on any Linux becomes significantly
  easier: A static binary will be provided with each release on the GitHub
  releases page. [#966](https://github.com/tenzir/vast/pull/966)

### 🐞 Bug Fixes

- The PCAP reader now correctly shows the amount of generated events.
  [#954](https://github.com/tenzir/vast/pull/954)

## [2020.06.25]

<!-- ### ⚡️ Breaking Changes -->

### ⚠️ Changes

- The options `system.table-slice-type` and `system.table-slice-size` have been
  removed, as they duplicated `import.table-slice-type` and
  `import.table-slice-size` respectively.
  [#908](https://github.com/tenzir/vast/pull/908)
  [#951](https://github.com/tenzir/vast/pull/951)

- The `default` table slice type has been renamed to `caf`. It has not been the
  default when built with Apache Arrow support for a while now, and the new name
  more accurately reflects what it is doing.
  [#948](https://github.com/tenzir/vast/pull/948)

- The JSON export format now renders timestamps using strings instead of numbers
  in order to avoid possible loss of precision.
  [#909](https://github.com/tenzir/vast/pull/909)

### 🧬 Experimental Features

- VAST now supports aging out existing data. This feature currently only
  concerns data in the archive. The options `system.aging-frequency` and
  `system.aging-query` configure a query that runs on a regular schedule to
  determine which events to delete. It is also possible to trigger an aging
  cycle manually. [#929](https://github.com/tenzir/vast/pull/929)

### 🎁 Features

- The output format for the `explore` and `pivot` commands can now be set using
  the `explore.format` and `pivot.format` options respectively. Both default to
  JSON. [#921](https://github.com/tenzir/vast/921)

- The meta index now uses Bloom filters for equality queries involving IP
  addresses. This especially accelerates queries where the user wants to know
  whether a certain IP address exists in the entire database.
  [#931](https://github.com/tenzir/vast/pull/931)

- The `import` command gained a new `--read-timeout` option that forces data to
  be forwarded to the importer regardless of the internal batching parameters
  and table slices being unfinished. This allows for reducing the latency
  between the `import` command and the node. The default timeout is 10 seconds.
  [#916](https://github.com/tenzir/vast/pull/916)

- VAST now has options to limit the amount of results produced by an invocation
  of `vast explore`. [#882](https://github.com/tenzir/vast/pull/882)

- The `import json` command's type restrictions are more relaxed now, and can
  additionally convert from JSON strings to VAST internal data types.
  [#891](https://github.com/tenzir/vast/pull/891)

- VAST now supports `/etc/vast/vast.conf` as an additional fallback for the
  configuration file. The following file locations are looked at in order: Path
  specified on the command line via `--config=path/to/vast.conf`, `vast.conf` in
  current working directory, `${INSTALL_PREFIX}/etc/vast/vast.conf`, and
  `/etc/vast/vast.conf`. [#898](https://github.com/tenzir/vast/pull/898)

### 🐞 Bug Fixes

- A bogus import process that assembled table slices with a greater number of
  events than expected by the node was able to lead to wrong query results.
  [#908](https://github.com/tenzir/vast/pull/908)

- A use after free bug would sometimes crash the node while it was shutting
  down. [#896](https://github.com/tenzir/vast/pull/896)

- The `export json` command now correctly unescapes its output.
  [#910](https://github.com/tenzir/vast/pull/910)

- VAST now correctly checks for control characters in inputs.
  [#910](https://github.com/tenzir/vast/pull/910)

## [2020.05.28]

<!-- ### ⚡️ Breaking Changes -->

### ⚠️ Changes

- Spreading a query over multiple command line arguments in commands like
  explore/export/pivot/etc. has been deprecated.
  [#878](https://github.com/tenzir/vast/pull/878)

- The command line flag for disabling the accountant has been renamed to
  `--disable-metrics` to more accurately reflect its intended purpose. The
  internal `vast.statistics` event has been renamed to `vast.metrics`.
  [#870](https://github.com/tenzir/vast/pull/870)

### 🧬 Experimental Features

- Added a new `explore` command to VAST that can be used to show data records
  within a certain time from the results of a query.
  [#873](https://github.com/tenzir/vast/pull/873)
  [#877](https://github.com/tenzir/vast/pull/877)

### 🎁 Features

- VAST now ships with a schema suitable for Sysmon import.
  [#886](https://github.com/tenzir/vast/pull/886)

- When importing events of a new or updated type, VAST now only requires the
  type to be specified once (e.g., in a schema file). For consecutive imports,
  the event type does not need to be specified again. A list of registered types
  can now be viewed using `vast status` under the key
  `node.type-registry.types`. [#875](https://github.com/tenzir/vast/pull/875)

- When importing JSON data without knowing the type of the imported events a
  priori, VAST now supports automatic event type deduction based on the JSON
  object keys in the data. VAST selects a type _iff_ the set of fields match a
  known type. The `--type` / `-t` option to the `import` command restricts the
  matching to the set of types that share the provided prefix. Omitting `-t`
  attempts to match JSON against all known types. If only a single variant of a
  type is matched, the import falls back to the old behavior and fills in `nil`
  for mismatched keys.  [#875](https://github.com/tenzir/vast/pull/875)

- VAST now prints a message when it is waiting for user input to read a query
  from a terminal.  [#878](https://github.com/tenzir/vast/pull/878)

- All input parsers now support mixed `\n` and `\r\n` line endings.
  [#865](https://github.com/tenzir/vast/pull/847)

### 🐞 Bug Fixes

- Fixed a bug that caused `vast import` processes to produce `'default'` table
  slices, despite having the `'arrow'` type as the default.
  [#866](https://github.com/tenzir/vast/pull/866)

- Fixed a bug where setting the `logger.file-verbosity` in the config file would
  not have an effect. [#866](https://github.com/tenzir/vast/pull/866)

- The parser for Zeek tsv data used to ignore attributes that were defined for
  the Zeek-specific types in the schema files. It has been modified to respect
  and prefer the specified attributes for the fields that are present in the
  input data. [#847](https://github.com/tenzir/vast/pull/847)

## [2020.04.29]

<!-- ### ⚡️ Breaking Changes -->

### ⚠️ Changes

- The option `--skip-candidate-checks` / `-s` for the `count` command was
  renamed to `--estimate` / `-e`.
  [#843](https://github.com/tenzir/vast/pull/843)

- The index specific options `max-partition-size`, `max-resident-partitions`,
  `max-taste-partitions`, and `max-queries` can now be specified on the command
  line when starting a node. [#728](https://github.com/tenzir/vast/pull/728)

- The default bind address has been changed from `::` to `localhost`.
  [#828](https://github.com/tenzir/vast/pull/828)

<!-- ### 🧬 Experimental Features -->

### 🎁 Features

- Bash autocompletion for `vast` is now available via the autocomplete script
  located at `scripts/vast-completions.bash` in the VAST source tree.
  [#833](https://github.com/tenzir/vast/pull/833)

- Packet drop and discard statistics are now reported to the accountant for PCAP
  import, and are available using the keys `pcap-reader.recv`,
  `pcap-reader.drop`, `pcap-reader.ifdrop`, `pcap-reader.discard`, and
  `pcap-reader.discard-rate ` in the `vast.statistics` event. If the number of
  dropped packets exceeds a configurable threshold, VAST additionally warns
  about packet drops on the command line.
  [#827](https://github.com/tenzir/pull/827)
  [#844](https://github.com/tenzir/pull/844)

### 🐞 Bug Fixes

- The `stop` command always returned immediately, regardless of whether it
  succeeded. It now blocks until the remote node shut down properly or returns
  an error exit code upon failure.
  [#849](https://github.com/tenzir/vast/pull/849)

- For some queries, the index evaluated only a subset of all relevant partitions
  in a non-deterministic manner. Fixing a violated evaluation invariant now
  guarantees deterministic execution.
  [#842](https://github.com/tenzir/vast/pull/842)

- Fixed a crash when importing data while a continuous export was running for
  unrelated events. [#830](https://github.com/tenzir/vast/pull/830)

- Fixed a bug that could cause stalled input streams not to forward events to
  the index and archive components for the JSON, CSV, and Syslog readers, when
  the input stopped arriving but no EOF was sent. This is a follow-up to
  [#750](https://github.com/tenzir/vast/pull/750). A timeout now ensures that
  that the readers continue when some events were already handled, but the input
  appears to be stalled. [#835](https://github.com/tenzir/vast/pull/835)

- Queries of the form `x != 80/tcp` were falsely evaluated as `x != 80/? && x !=
  ?/tcp`. (The syntax in the second predicate does not yet exist; it only
  illustrates the bug.) Port inequality queries now correctly evaluate `x !=
  80/? || x != ?/tcp`. E.g., the result now contains values like `80/udp` and
  `80/?`, but also `8080/tcp`. [#834](https://github.com/tenzir/vast/pull/834)

- Archive lookups are now interruptible. This change fixes an issue that caused
  consecutive exports to slow down the node, which improves the overall
  performance for larger databases considerably.
  [#825](https://github.com/tenzir/vast/pull/825)

## [2020.03.26]

<!-- ### ⚡️ Breaking Changes -->

### ⚠️ Changes

- The config option `system.log-directory` was deprecated and replaced by the
  new option `system.log-file`. All logs will now be written to a single file.
  [#806](https://github.com/tenzir/vast/pull/803)

- The log folder `vast.log/` in the current directory will not be created by
  default any more. Users must explicitly set the `system.file-verbosity` option
  if they wish to keep the old behavior.
  [#803](https://github.com/tenzir/vast/pull/803)

- The VERBOSE log level has been added between INFO and DEBUG. This level is
  enabled at build time for all build types, making it possible to get more
  detailed logging output from release builds.
  [#787](https://github.com/tenzir/pull/787)

- The command line options prefix for changing CAF options was changed from
  `--caf#` to `--caf.`. [#797](https://github.com/tenzir/pull/797)

- The internal statistics event type `vast.account` has been renamed to
  `vast.statistics` for clarity. [#789](https://github.com/tenzir/pull/789)

<!-- ### 🧬 Experimental Features -->

### 🎁 Features

- The new `vast import syslog` command allows importing Syslog messages as
  defined in [RFC5424](https://tools.ietf.org/html/rfc5424).
  [#770](https://github.com/tenzir/vast/pull/770)

- The hash index has been re-enabled after it was outfitted with a new
  [high-performance hash map](https://github.com/Tessil/robin-map/)
  implementation that increased performance to the point where it is on par with
  the regular index. [#796](https://github.com/tenzir/vast/796)

- The option `--disable-community-id` has been added to the `vast import pcap`
  command for disabling the automatic computation of Community IDs.
  [#777](https://github.com/tenzir/pull/777)

### 🐞 Bug Fixes

- An under-the-hood change to our parser-combinator framework makes sure that we
  do not discard possibly invalid input data up the the end of input. This
  uncovered a bug in our MRT/bgpdump integrations, which have thus been disabled
  (for now), and will be fixed at a later point in time.
  [#808](https://github.com/tenzir/vast/pull/808)

- Expressions must now be parsed to the end of input. This fixes a bug that
  caused malformed queries to be evaluated until the parser failed. For example,
  the query `#type == "suricata.http" && .dest_port == 80` was erroneously
  evaluated as `#type == "suricata.http"` instead.
  [#791](https://github.com/tenzir/pull/791)

- The short option `-c` for setting the configuration file has been removed.
  The long option `--config` must now be used instead. This fixed a bug that did
  not allow for `-c` to be used for continuous exports.
  [#781](https://github.com/tenzir/pull/781)

- Continuous export processes can now be stopped correctly. Before this change,
  the node showed an error message and the exporting process exited with a
  non-zero exit code. [#779](https://github.com/tenzir/pull/779)

## [2020.02.27]

<!-- ### ⚡️ Breaking Changes -->

### ⚠️ Changes

- Hash indices have been disabled again due to a performance regression.
  [#765](https://github.com/tenzir/vast/pull/765)

- The option `--directory` has been replaced by `--db-directory` and
  `log-directory`, which set directories for persistent state and log files
  respectively. The default log file path has changed from `vast.db/log` to
  `vast.log`. [#758](https://github.com/tenzir/vast/pull/758)

- VAST now supports (and requires) Apache Arrow >= 0.16.
  [#751](https://github.com/tenzir/vast/pull/751)

- The option `--historical` for export commands has been removed, as it was the
  default already. [#754](https://github.com/tenzir/vast/pull/754)

- The build system will from now on try use the CAF library from the system, if
  one is provided. If it is not found, the CAF submodule will be used as a
  fallback. [#740](https://github.com/tenzir/vast/pull/740)

<!-- ### 🧬 Experimental Features -->

### 🎁 Features

- For users of the [Nix](https://nixos.org/nix/) package manager, expressions
  have been added to generate reproducible development environments with
  `nix-shell`. [#740](https://github.com/tenzir/vast/pull/740)

### 🐞 Bug Fixes

- Continuously importing events from a Zeek process with a low rate of emitted
  events resulted in a long delay until the data would be included in the result
  set of queries. This is because the import process would buffer up to 10,000
  events before sending them to the server as a batch.  The algorithm has been
  tuned to flush its buffers if no data is available for more than 500
  milliseconds. [#750](https://github.com/tenzir/vast/pull/750)

## [2020.01.31]

<!-- ### ⚡️ Breaking Changes -->

### ⚠️ Changes

- VAST is switching to a calendar-based versioning scheme starting with this
  release. [#739](https://github.com/tenzir/vast/pull/739)

- Record field names can now be entered as quoted strings in the schema and
  expression languages. This lifts a restriction where JSON fields with
  whitespaces or special characters could not be ingested.
  [#685](https://github.com/tenzir/vast/pull/685)

- Two minor modifications were done in the parsing framework: (i) the parsers
  for enums and records now allow trailing separators, and (ii) the dash (`-`)
  was removed from the allowed characters of schema type names.
  [#706](https://github.com/tenzir/vast/pull/706)

- Build configuration defaults have been adapated for a better user experience.
  Installations are now relocatable by default, which can be reverted by
  configuring with `--without-relocatable`. Additionally, new sets of defaults
  named `--release` and `--debug` (renamed from `--dev-mode`) have been added.
  [#695](https://github.com/tenzir/vast/pull/695)

- The `import pcap` command no longer takes interface names via `--read,-r`, but
  instead from a separate option named `--interface,-i`. This change has been
  made for consistency with other tools.
  [#641](https://github.com/tenzir/vast/pull/641)

<!-- ### 🧬 Experimental Features -->

### 🎁 Features

- When a record field has the `#index=hash` attribute, VAST will choose an
  optimized index implementation. This new index type only supports (in)equality
  queries and is therefore intended to be used with opaque types, such as unique
  identifiers or random strings.
  [#632](https://github.com/tenzir/vast/pull/632),
  [#726](https://github.com/tenzir/vast/pull/726)

- An experimental new Python module enables querying VAST and processing results
  as [pyarrow](https://arrow.apache.org/docs/python/) tables.
  [#685](https://github.com/tenzir/vast/pull/685)

- On FreeBSD, a VAST installation now includes an rc.d script that simpliefies
  spinning up a VAST node. CMake installs the script at `PREFIX/etc/rc.d/vast`.
  [#693](https://github.com/tenzir/vast/pull/693)

- The long option `--config`, which sets an explicit path to the VAST
  configuration file, now also has the short option `-c`.
  [#689](https://github.com/tenzir/vast/pull/689)

- Added *Apache Arrow* as new export format. This allows users to export query
  results as Apache Arrow record batches for processing the results downstream,
  e.g., in Python or Spark. [#633](https://github.com/tenzir/vast/pull/633)

- The `import pcap` command now takes an optional snapshot length via
  `--snaplen`.  If the snapshot length is set to snaplen, and snaplen is less
  than the size of a packet that is captured, only the first snaplen bytes of
  that packet will be captured and provided as packet data.
  [#642](https://github.com/tenzir/vast/pull/642)

### 🐞 Bug Fixes

- A bug in the quoted string parser caused a parsing failure if an escape
  character occurred in the last position.
  [#685](https://github.com/tenzir/vast/pull/685)

- The example configuration file contained an invalid section `vast`.  This has
  been changed to the correct name `system`.
  [#705](https://github.com/tenzir/vast/pull/705)

- A race condition in the index logic was able to lead to incomplete or empty
  result sets for `vast export`. [#703](https://github.com/tenzir/vast/pull/703)

- The import process did not print statistics when importing events over UDP.
  Additionally, warnings about dropped UDP packets are no longer shown per
  packet, but rather periodically reported in a readable format.
  [#662](https://github.com/tenzir/vast/pull/662)

- Importing events over UDP with `vast import <format> --listen :<port>/udp`
  failed to register the accountant component. This caused an unexpected message
  warning to be printed on startup and resulted in losing import statistics.
  VAST now correctly registers the accountant.
  [#655](https://github.com/tenzir/vast/pull/655)

- PCAP ingestion failed for traces containing VLAN tags. VAST now strips [IEEE
  802.1Q](https://en.wikipedia.org/wiki/IEEE_802.1Q) headers instead of skipping
  VLAN-tagged packets.  [#650](https://github.com/tenzir/vast/pull/650)

- In some cases it was possible that a source would connect to a node before it
  was fully initialized, resulting in a hanging `vast import` process.
  [#647](https://github.com/tenzir/vast/pull/647)

## [0.2] - 2019.10.30

<!-- ### ⚡️ Breaking Changes -->

### ⚠️ Changes

- The query language has been extended to support expression of the form `X ==
  /pattern/`, where `X` is a compatible LHS extractor. Previously, patterns only
  supports the match operator `~`. The two operators have the same semantics
  when one operand is a pattern.

- CAF and Broker are no longer required to be installed prior to building VAST.
  These dependencies are now tracked as git submodules to ensure version
  compatibility. Specifying a custom build is still possible via the CMake
  variables `CAF_ROOT_DIR` and `BROKER_ROOT_DIR`.

- When exporting data in `pcap` format, it is no longer necessary to manually
  restrict the query by adding the predicate `#type == "pcap.packet"` to the
  expression. This now happens automatically because only this type contains the
  raw packet data.

- When defining schema attributes in key-value pair form, the value no longer
  requires double-quotes. For example, `#foo=x` is now the same as `#foo="x"`.
  The form without double-quotes consumes the input until the next space and
  does not support escaping. In case an attribute value contains whitespace,
  double-quotes must be provided, e.g., `#foo="x y z"`.

- The PCAP packet type gained the additional field `community_id` that contains
  the [Community ID](https://github.com/corelight/community-id-spec) flow hash.
  This identifier facilitates pivoting to a specific flow from data sources with
  connnection-level information, such Zeek or Suricata logs.

- Log files generally have some notion of timestamp for recorded events. To make
  the query language more intuitive, the syntax for querying time points thus
  changed from `#time` to `#timestamp`. For example, `#time >
  2019-07-02+12:00:00` now reads `#timestamp > 2019-07-02+12:00:00`.

- Default schema definitions for certain `import` formats changed from
  hard-coded to runtime-evaluated. The default location of the schema definition
  files is `$(dirname vast-executable)`/../share/vast/schema.  Currently this is
  used for the Suricata JSON log reader.

- The default directory name for persistent state changed from `vast` to
  `vast.db`. This makes it possible to run `./vast` in the current directory
  without having to specify a different state directory on the command line.

- Nested types are from now on accessed by the `.`-syntax. This means VAST now
  has a unified syntax to select nested types and fields.  For example, what
  used to be `zeek::http` is now just `zeek.http`.

- The (internal) option `--node` for the `import` and `export` commands has been
  renamed from `-n` to `-N`, to allow usage of `-n` for `--max-events`.
- To make the export option to limit the number of events to be exported more
  idiomatic, it has been renamed from `--events,e` to `--max-events,n`.  Now
  `vast export -n 42` generates at most 42 events.

<!-- ### 🧬 Experimental Features -->

### 🎁 Features

- The default schema for Suricata has been updated to support the new
  `suricata.smtp` event type in Suricata 5.

- The `export null` command retrieves data, but never prints anything. Its main
  purpose is to make benchmarking VAST easier and faster.

- The new `pivot` command retrieves data of a related type. It inspects each
  event in a query result to find an event of the requested type. If a common
  field exists in the schema definition of the requested type, VAST will
  dynamically create a new query to fetch the contextual data according to the
  type relationship. For example, if two records `T` and `U` share the same
  field `x`, and the user requests to pivot via `T.x == 42`, then VAST will
  fetch all data for `U.x == 42`. An example use case would be to pivot from a
  Zeek or Suricata log entry to the corresponding PCAP packets.  VAST uses the
  field `community_id` to pivot between the logs and the packets.  Pivoting is
  currently implemented for Suricata, Zeek (with [community ID
  computation](https://github.com/corelight/bro-community-id) enabled), and
  PCAP.

- The new `infer` command performs schema inference of input data. The command
  can deduce the input format and creates a schema definition that is sutable to
  use with the supplied data. Supported input types include Zeek TSV and JSONLD.

- The newly added `count` comman allows counting hits for a query without
  exporting data.

- Commands now support a `--documentation` option, which returns
  Markdown-formatted documentation text.

- A new schema for Argus CSV output has been added. It parses the output of
  `ra(1)`, which produces CSV output when invoked with `-L 0 -c ,`.

- The schema language now supports comments. A double-slash (`//`) begins a
  comment. Comments last until the end of the line, i.e., until a newline
  character (`\n`).

- The `import` command now supports CSV formatted data. The type for each column
  is automatically derived by matching the column names from the CSV header in
  the input with the available types from the schema definitions.

- Configuring how much status information gets printed to STDERR previously
  required obscure config settings. From now on, users can simply use
  `--verbosity=<level>,-v <level>`, where `<level>` is one of `quiet`, `error`,
  `warn`, `info`, `debug`, or `trace`. However, `debug` and `trace` are only
  available for debug builds (otherwise they fall back to log level `info`).

- The query expression language now supports *data predicates*, which are a
  shorthand for a type extractor in combination with an equality operator. For
  example, the data predicate `6.6.6.6` is the same as `:addr == 6.6.6.6`.

- The `index` object in the output from `vast status` has a new field
  `statistics` for a high-level summary of the indexed data. Currently, there
  exists a nested `layouts` objects with per-layout statistics about the number
  of events indexed.

- The `accountant` object in the output from `vast status` has a new field
  `log-file` that points to the filesystem path of the accountant log file.

- Data extractors in the query language can now contain a type prefix.  This
  enables an easier way to extract data from a specific type. For example, a
  query to look for Zeek conn log entries with responder IP address 1.2.3.4 had
  to be written with two terms, `#type == zeek.conn && id.resp_h == 1.2.3.4`,
  because the nested id record can occur in other types as well. Such queries
  can now written more tersely as `zeek.conn.id.resp_h == 1.2.3.4`.

- VAST gained support for importing Suricata JSON logs. The import command has a
  new suricata format that can ingest EVE JSON output.

- The data parser now supports `count` and `integer` values according to the
  *International System for Units (SI)*. For example, `1k` is equal to `1000`
  and `1Ki` equal to `1024`.

- VAST can now ingest JSON data. The `import` command gained the `json` format,
  which allows for parsing line-delimited JSON (LDJSON) according to a
  user-selected type with `--type`. The `--schema` or `--schema-file` options
  can be used in conjunction to supply custom types. The JSON objects in the
  input must match the selected type, that is, the keys of the JSON object must
  be equal to the record field names and the object values must be convertible
  to the record field types.

- For symmetry to the `export` command, the `import` command gained the
  `--max-events,n` option to limit the number of events that will be imported.

- The `import` command gained the `--listen,l` option to receive input from the
  network. Currently only UDP is supported. Previously, one had to use a clever
  netcat pipe with enough receive buffer to achieve the same effect, e.g., `nc
  -I 1500 -p 4200 | vast import pcap`. Now this pipe degenerates to `vast import
  pcap -l`.

- The new `--disable-accounting` option shuts off periodic gathering of system
  telemetry in the accountant actor. This also disables output in the
  `accounting.log`.

### 🐞 Bug Fixes

- The user environments `LDFLAGS` were erroneously passed to `ar`. Instead, the
  user environments `ARFLAGS` are now used.

- Exporting data with `export -n <count>` crashed when `count` was a multiple of
  the table slice size. The command now works as expected.

- Queries of the form `#type ~ /pattern/` used to be rejected erroneously.  The
  validation code has been corrected and such queries are now working as
  expected.

- When specifying `enum` types in the schema, ingestion failed because there did
  not exist an implementation for such types. It is now possible to use define
  enumerations in schema as expected and query them as strings.

- Queries with the less `<` or greater `>` operators produced off-by-one results
  for the `duration` when the query contained a finer resolution than the index.
  The operator now works as expected.

- Timestamps were always printed in millisecond resolution, which lead to loss
  of precision when the internal representation had a higher resolution.
  Timestamps are now rendered up to nanosecond resolution - the maximum
  resolution supported.

- All query expressions in the form `#type != X` were falsely evaluated as
  `#type == X` and consequently produced wrong results. These expressions now
  behave as expected.

- Parsers for reading log input that relied on recursive rules leaked memory by
  creating cycling references. All recursive parsers have been updated to break
  such cycles and thus no longer leak memory.

- The Zeek reader failed upon encountering logs with a `double` column, as it
  occurs in `capture_loss.log`. The Zeek parser generator has been fixed to
  handle such types correctly.

- Some queries returned duplicate events because the archive did not filter the
  result set properly. This no longer occurs after fixing the table slice
  filtering logic.

- The `map` data parser did not parse negative values correctly. It was not
  possible to parse strings of the form `"{-42 -> T}"` because the parser
  attempted to parse the token for the empty map `"{-}"` instead.

- The CSV printer of the `export` command used to insert 2 superfluous fields
  when formatting an event: The internal event ID and a deprecated internal
  timestamp value. Both fields have been removed from the output, bringing it
  into line with the other output formats.

- When a node terminates during an import, the client process remained
  unaffected and kept processing input. Now the client terminates when a remote
  node terminates.

- Evaluation of predicates with negations return incorrect results. For example,
  the expression `:addr !in 10.0.0.0/8` created a disjunction of all fields to
  which `:addr` resolved, without properly applying De-Morgan. The same bug also
  existed for key extractors. De-Morgan is now applied properly for the
  operations `!in` and `!~`.

## [0.1] - 2019.02.28

This is the first official release.

[0.1]: https://github.com/tenzir/vast/releases/tag/0.1
[0.2]: https://github.com/tenzir/vast/releases/tag/0.2
[2020.01.31]: https://github.com/tenzir/vast/releases/tag/2020.01.31
[2020.02.27]: https://github.com/tenzir/vast/releases/tag/2020.02.27
[2020.03.26]: https://github.com/tenzir/vast/releases/tag/2020.03.26
[2020.04.29]: https://github.com/tenzir/vast/releases/tag/2020.04.29
[2020.05.28]: https://github.com/tenzir/vast/releases/tag/2020.05.28
[2020.06.25]: https://github.com/tenzir/vast/releases/tag/2020.06.25
[2020.07.28]: https://github.com/tenzir/vast/releases/tag/2020.07.28
[2020.08.28]: https://github.com/tenzir/vast/releases/tag/2020.08.28
[2020.09.30]: https://github.com/tenzir/vast/releases/tag/2020.09.30
[2020.10.29]: https://github.com/tenzir/vast/releases/tag/2020.10.29
[2020.12.16]: https://github.com/tenzir/vast/releases/tag/2020.12.16
[2021.01.28]: https://github.com/tenzir/vast/releases/tag/2021.01.28
[2021.02.24]: https://github.com/tenzir/vast/releases/tag/2021.02.24
[2021.03.25]: https://github.com/tenzir/vast/releases/tag/2021.03.25
