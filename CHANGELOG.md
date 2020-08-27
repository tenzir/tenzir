# Changelog

This changelog documents all notable user-facing changes of VAST.

Every entry has a category for which we use the following visual abbreviations:

- 🎁 feature
- 🧬 experimental feature
- ⚠️ change
- 🐞 bugfix

<!-- ## Unreleased -->

## [2020.08.28]

- 🐞 VAST did not terminate when a critical component failed during startup.
  VAST now binds the lifetime of the node to all critical components.
  [#1028](https://github.com/tenzir/vast/pull/1028)

- 🐞 VAST would overwrite existing on-disk state data when encountering a partial
  read during startup. This state-corrupting behavior no longer exists.
  [#1026](https://github.com/tenzir/vast/pull/1026)

- 🐞 Incomplete reads have not been handled properly, which manifested for
  files larger than 2GB. On macOS, writing files larger than 2GB may have
  failed previously. VAST now respects OS-specific constraints on the maximum
  block size.
  [#1025](https://github.com/tenzir/vast/pull/1025)

- 🐞 The shutdown process of the server process could potentially hang forever.
  VAST now uses a 2-step procedure that first attempts to terminate all
  components cleanly. If that fails, it will attempt a hard kill afterwards,
  and if that fails after another timeout, the process will call `abort(3)`.
  [#1005](https://github.com/tenzir/vast/pull/1005)

- ⚠️ We now bundle a patched version of CAF, with a changed ABI. This means
  that if you're linking against the bundled CAF library, you also need to
  distribute that library so that VAST can use it at runtime.
  The versions are API compatible so linking against a system version of
  CAF is still possible and supported.
  [#1020](https://github.com/tenzir/vast/pull/1020)

- 🐞 When running VAST under heavy load, CAF stream slot ids could wrap around
  after a few days and deadlock the system. As a workaround, we extended the
  slot id bit width to make the time until this happens unrealistically large.
  [#1020](https://github.com/tenzir/vast/pull/1020)

- 🐞 Some file descriptors remained open when they weren't needed any more.
  This descriptor leak has been fixed.
  [#1018](https://github.com/tenzir/vast/pull/1018)

- ⚠️ The `vector` type has been renamed to `list`.In an effort to streamline
  the type system vocabulary, we favor `list` over `vector` because it's closer
  to existing terminology (e.g., Apache Arrow). This change requires updating
  existing schemas by changing `vector<T>` to `list<T>`.
  [#1016](https://github.com/tenzir/vast/pull/1016)

- ⚠️ The `set` type has been removed. Experience with the data model showed
  that there is no strong use case to separate sets from vectors in the core.
  While this may be useful in programming languages, VAST deals with immutable
  data where set constraints have been enforced upstream. This change requires
  updating existing schemas by changing `set<T>` to `vector<T>`. In the query
  language, the new symbol for the empty `map` changed from `{-}` to `{}`, as
  it now unambiguisly identifies `map` instances.
  [#1010](https://github.com/tenzir/vast/pull/1010)

- 🎁 The default schema for Suricata has been updated to support the
  `suricata.ftp` and `suricata.ftp_data` event types.
  [#1009](https://github.com/tenzir/vast/pull/1009)

- 🐞 Importing JSON no longer fails for JSON fields containing `null` when the
  corresponding VAST type in the schema is a non-trivial type like
  `vector<string>`. [#1009](https://github.com/tenzir/vast/pull/1009)

- 🎁 VAST now prints the location of the configuration file that is used.
  [#1009](https://github.com/tenzir/vast/pull/1009)

- 🐞 The port encoding for Arrow-encoded table slices is now host-independent
  and always uses network-byte order.
  [#1007](https://github.com/tenzir/vast/pull/1007)

- 🐞 When continuous query in a client process terminated, the node did not
  clean up the corresponding server-side state. This memory leak no longer
  exists. [#1006](https://github.com/tenzir/vast/pull/1006)

- ⚠️ The expression field parser now allows the '-' character.
  [#999](https://github.com/tenzir/vast/pull/999)

- 🐞 A bug in the expression parser prevented the correct parsing of fields
  starting with either 'F' or 'T'.
  [#999](https://github.com/tenzir/vast/pull/999)

- 🐞 MessagePack-encoded table slices now work correctly for nested container
  types. [#984](https://github.com/tenzir/vast/pull/984)

- 🎁 VAST now writes a PID lock file on startup to prevent multiple server
  processes from accessing the same persistent state. The `pid.lock` file
  resides in the `vast.db` directory.
  [#1001](https://github.com/tenzir/vast/pull/1001)

## [2020.07.28]

- ⚠️ VAST now recognizes `/etc/vast/schema` as an additional default directory
  for schema files. [#980](https://github.com/tenzir/vast/pull/980)

- ⚠️ [FlatBuffers](https://google.github.io/flatbuffers/) is now a required
  dependency for VAST. The archive and the segment store use FlatBuffers to
  store and version their on-disk persistent state.
  [#972](https://github.com/tenzir/vast/pull/972)

- 🎁 We open-sourced our [MessagePack](http://msgpack.org)-based table slice
  implementation, which provides a compact row-oriented encoding of data. This
  encoding works well for binary formats (e.g., PCAP) and access patterns that
  involve materializing entire rows. The MessagePack table slice is the new
  default when Apache Arrow is unavailable. To enable parsing into MessagePack,
  you can pass `--table-slice-type=msgpack` to the `import` command, or set the
  configuration option `import.table-slice-type` to `'msgpack'`.
  [#975](https://github.com/tenzir/vast/pull/975)

- 🎁 Starting with this release, installing VAST on any Linux becomes
  significantly easier: A static binary will be provided with each release on
  the GitHub releases page. [#966](https://github.com/tenzir/vast/pull/966)

- 🐞 The PCAP reader now correctly shows the amount of generated events.
  [#954](https://github.com/tenzir/vast/pull/954)

- ⚠️ The suricata schema file contains new type definitions for the stats, krb5,
  smb, and ssh events. [#954](https://github.com/tenzir/vast/pull/954)
  [#986](https://github.com/tenzir/vast/pull/986)

## [2020.06.25]

- ⚠️ The options `system.table-slice-type` and `system.table-slice-size` have
  been removed, as they duplicated `import.table-slice-type` and
  `import.table-slice-size` respectively.
  [#908](https://github.com/tenzir/vast/pull/908)
  [#951](https://github.com/tenzir/vast/pull/951)

- ⚠️ The `default` table slice type has been renamed to `caf`. It has not been
  the default when built with Apache Arrow support for a while now, and the new
  name more accurately reflects what it is doing.
  [#948](https://github.com/tenzir/vast/pull/948)

- 🎁 The output format for the `explore` and `pivot` commands can now be set
  using the `explore.format` and `pivot.format` options respectively. Both
  default to JSON. [#921](https://github.com/tenzir/vast/921)

- 🎁 The meta index now uses Bloom filters for equality queries involving IP
  addresses. This especially accelerates queries where the user wants to know
  whether a certain IP address exists in the entire database.
  [#931](https://github.com/tenzir/vast/pull/931)

- 🧬 VAST now supports aging out existing data. This feature currently only
  concerns data in the archive. The options `system.aging-frequency` and
  `system.aging-query` configure a query that runs on a regular schedule to
  determine which events to delete. It is also possible to trigger an aging
  cycle manually. [#929](https://github.com/tenzir/vast/pull/929)

- 🎁 The `import` command gained a new `--read-timeout` option that forces data
  to be forwarded to the importer regardless of the internal batching parameters
  and table slices being unfinished. This allows for reducing the latency
  between the `import` command and the node. The default timeout is 10 seconds.
  [#916](https://github.com/tenzir/vast/pull/916)

- 🐞 A bogus import process that assembled table slices with a greater number
  of events than expected by the node was able to lead to wrong query results.
  [#908](https://github.com/tenzir/vast/pull/908)

- ⚠️ The JSON export format now renders timestamps using strings instead of
  numbers in order to avoid possible loss of precision.
  [#909](https://github.com/tenzir/vast/pull/909)

- 🐞 A use after free bug would sometimes crash the node while it was shutting
  down. [#896](https://github.com/tenzir/vast/pull/896)

- 🐞 The `export json` command now correctly unescapes its output.
  [#910](https://github.com/tenzir/vast/pull/910)

- 🐞 VAST now correctly checks for control characters in inputs.
  [#910](https://github.com/tenzir/vast/pull/910)

- 🎁 VAST now has options to limit the amount of results produced by an
  invocation of `vast explore`. [#882](https://github.com/tenzir/vast/pull/882)

- 🎁 The `import json` command's type restrictions are more relaxed now, and can
  additionally convert from JSON strings to VAST internal data types.
  [#891](https://github.com/tenzir/vast/pull/891)

- 🎁 VAST now supports `/etc/vast/vast.conf` as an additional fallback for the
  configuration file. The following file locations are looked at in order: Path
  specified on the command line via `--config=path/to/vast.conf`, `vast.conf` in
  current working directory, `${INSTALL_PREFIX}/etc/vast/vast.conf`, and
  `/etc/vast/vast.conf`. [#898](https://github.com/tenzir/vast/pull/898)

## [2020.05.28]

- 🎁 VAST now ships with a schema suitable for Sysmon import.
  [#886](https://github.com/tenzir/vast/pull/886)

- 🎁 When importing events of a new or updated type, VAST now only requires the
  type to be specified once (e.g., in a schema file). For consecutive imports,
  the event type does not need to be specified again. A list of registered types
  can now be viewed using `vast status` under the key
  `node.type-registry.types`. [#875](https://github.com/tenzir/vast/pull/875)

- 🎁 When importing JSON data without knowing the type of the imported events a
  priori, VAST now supports automatic event type deduction based on the JSON
  object keys in the data. VAST selects a type _iff_ the set of fields match a
  known type. The `--type` / `-t` option to the `import` command restricts the
  matching to the set of types that share the provided prefix. Omitting `-t`
  attempts to match JSON against all known types. If only a single variant of a
  type is matched, the import falls back to the old behavior and fills in `nil`
  for mismatched keys.
  [#875](https://github.com/tenzir/vast/pull/875)

- 🎁 VAST now prints a message when it is waiting for user input to read
  a query from a terminal.
  [#878](https://github.com/tenzir/vast/pull/878)

- ⚠️ Spreading a query over multiple command line arguments in commands
  like explore/export/pivot/etc. has been deprecated.
  [#878](https://github.com/tenzir/vast/pull/878)

- 🧬 Added a new `explore` command to VAST that can be used to
  show data records within a certain time from the results of a query.
  [#873](https://github.com/tenzir/vast/pull/873)
  [#877](https://github.com/tenzir/vast/pull/877)

- ⚠️ The command line flag for disabling the accountant has been renamed to
  `--disable-metrics` to more accurately reflect its intended purpose. The
  internal `vast.statistics` event has been renamed to `vast.metrics`.
  [#870](https://github.com/tenzir/vast/pull/870)

- 🎁 All input parsers now support mixed `\n` and `\r\n` line endings.
  [#865](https://github.com/tenzir/vast/pull/847)

- 🐞 Fixed a bug that caused `vast import` processes to produce `'default'`
  table slices, despite having the `'arrow'` type as the default.
  [#866](https://github.com/tenzir/vast/pull/866)

- 🐞 Fixed a bug where setting the `logger.file-verbosity` in the config file
  would not have an effect. [#866](https://github.com/tenzir/vast/pull/866)

- 🐞 The parser for Zeek tsv data used to ignore attributes that were defined
  for the Zeek-specific types in the schema files. It has been modified to
  respect and prefer the specified attributes for the fields that are present
  in the input data. [#847](https://github.com/tenzir/vast/pull/847)

# [2020.04.29]

- 🐞 The `stop` command always returned immediately, regardless of whether it
  succeeded. It now blocks until the remote node shut down properly or returns
  an error exit code upon failure.
  [#849](https://github.com/tenzir/vast/pull/849)

- ⚠️ The option `--skip-candidate-checks` / `-s` for the `count` command
  was renamed to `--estimate` / `-e`.
  [#843](https://github.com/tenzir/vast/pull/843)

- 🐞 For some queries, the index evaluated only a subset of all relevant
  partitions in a non-deterministic manner. Fixing a violated evaluation
  invariant now guarantees deterministic execution.
  [#842](https://github.com/tenzir/vast/pull/842)

- 🐞 Fixed a crash when importing data while a continuous export was running for
  unrelated events. [#830](https://github.com/tenzir/vast/pull/830)

- 🐞 Fixed a bug that could cause stalled input streams not to forward events to
  the index and archive components for the JSON, CSV, and Syslog readers, when
  the input stopped arriving but no EOF was sent. This is a follow-up to
  [#750](https://github.com/tenzir/vast/pull/750). A timeout now ensures that
  that the readers continue when some events were already handled, but the input
  appears to be stalled. [#835](https://github.com/tenzir/vast/pull/835)

- 🐞 Queries of the form `x != 80/tcp` were falsely evaluated as
  `x != 80/? && x != ?/tcp`. (The syntax in the second predicate does not yet
  exist; it only illustrates the bug.) Port inequality queries now correctly
  evaluate `x != 80/? || x != ?/tcp`. E.g., the result now contains values like
  `80/udp` and `80/?`, but also `8080/tcp`.
  [#834](https://github.com/tenzir/vast/pull/834)

- 🎁 Bash autocompletion for `vast` is now available via the autocomplete
  script located at `scripts/vast-completions.bash` in the VAST source tree.
  [#833](https://github.com/tenzir/vast/pull/833)

- ⚠️ The index specific options `max-partition-size`, `max-resident-partitions`,
  `max-taste-partitions`, and `max-queries` can now be specified on the command
  line when starting a node.
  [#728](https://github.com/tenzir/vast/pull/728)

- 🎁 Packet drop and discard statistics are now reported to the accountant for
  PCAP import, and are available using the keys `pcap-reader.recv`,
  `pcap-reader.drop`, `pcap-reader.ifdrop`, `pcap-reader.discard`, and
  `pcap-reader.discard-rate ` in the `vast.statistics` event. If the number of
  dropped packets exceeds a configurable threshold, VAST additionally warns
  about packet drops on the command line.
  [#827](https://github.com/tenzir/pull/827)
  [#844](https://github.com/tenzir/pull/844)

- ⚠️ The default bind address has been changed from `::` to `localhost`.
  [#828](https://github.com/tenzir/vast/pull/828)

- 🐞 Archive lookups are now interruptible. This change fixes an issue that
  caused consecutive exports to slow down the node, which improves the overall
  performance for larger databases considerably.
  [#825](https://github.com/tenzir/vast/pull/825)

## [2020.03.26]

- 🐞 An under-the-hood change to our parser-combinator framework makes sure that
  we do not discard possibly invalid input data up the the end of input. This
  uncovered a bug in our MRT/bgpdump integrations, which have thus been disabled
  (for now), and will be fixed at a later point in time.
  [#808](https://github.com/tenzir/vast/pull/808)

- ⚠️ The config option `system.log-directory` was deprecated and replaced
  by the new option `system.log-file`. All logs will now be written to a
  single file.
  [#806](https://github.com/tenzir/vast/pull/803)

- ⚠️ The log folder `vast.log/` in the current directory will not be created
  by default any more. Users must explicitly set the `system.file-verbosity`
  option if they wish to keep the old behavior.
  [#803](https://github.com/tenzir/vast/pull/803)

- 🎁 The new `vast import syslog` command allows importing Syslog messages
  as defined in [RFC5424](https://tools.ietf.org/html/rfc5424).
  [#770](https://github.com/tenzir/vast/pull/770)

- 🎁 The hash index has been re-enabled after it was outfitted with a new
  [high-performance hash map](https://github.com/Tessil/robin-map/)
  implementation that increased performance to the point where it is on par with
  the regular index. [#796](https://github.com/tenzir/vast/796)

- ⚠️ The VERBOSE log level has been added between INFO and DEBUG. This level
  is enabled at build time for all build types, making it possible to get more
  detailed logging output from release builds.
  [#787](https://github.com/tenzir/pull/787)

- ⚠️ The command line options prefix for changing CAF options was changed from
  `--caf#` to `--caf.`. [#797](https://github.com/tenzir/pull/797)

- 🐞 Expressions must now be parsed to the end of input. This fixes a bug that
  caused malformed queries to be evaluated until the parser failed. For example,
  the query `#type == "suricata.http" && .dest_port == 80` was erroneously
  evaluated as `#type == "suricata.http"` instead.
  [#791](https://github.com/tenzir/pull/791)

- ⚠️ The internal statistics event type `vast.account` has been renamed to
  `vast.statistics` for clarity. [#789](https://github.com/tenzir/pull/789)

- 🐞 The short option `-c` for setting the configuration file has been removed.
  The long option `--config` must now be used instead. This fixed a bug that did
  not allow for `-c` to be used for continuous exports.
  [#781](https://github.com/tenzir/pull/781)

- 🐞 Continuous export processes can now be stopped correctly. Before this
  change, the node showed an error message and the exporting process exited with
  a non-zero exit code. [#779](https://github.com/tenzir/pull/779)

- 🎁 The option `--disable-community-id` has been added to the `vast import
  pcap` command for disabling the automatic computation of Community IDs.
  [#777](https://github.com/tenzir/pull/777)

## [2020.02.27]

- 🐞 Continuously importing events from a Zeek process with a low rate of
  emitted events resulted in a long delay until the data would be included
  in the result set of queries. This is because the import process would
  buffer up to 10,000 events before sending them to the server as a batch.
  The algorithm has been tuned to flush its buffers if no data is available
  for more than 500 milliseconds.
  [#750](https://github.com/tenzir/vast/pull/750)

- ⚠️ Hash indices have been disabled again due to a performance regression.
  [#765](https://github.com/tenzir/vast/pull/765)

- ⚠️ The option `--directory` has been replaced by `--db-directory` and
  `log-directory`, which set directories for persistent state and log files
  respectively. The default log file path has changed from `vast.db/log` to
  `vast.log`. [#758](https://github.com/tenzir/vast/pull/758)

- ⚠️ VAST now supports (and requires) Apache Arrow >= 0.16.
  [#751](https://github.com/tenzir/vast/pull/751)

- ⚠️ The option `--historical` for export commands has been removed, as it was
  the default already. [#754](https://github.com/tenzir/vast/pull/754)

- 🎁 For users of the [Nix](https://nixos.org/nix/) package manager, expressions
  have been added to generate reproducible development environments with
  `nix-shell`.
  [#740](https://github.com/tenzir/vast/pull/740)

- ⚠️ The build system will from now on try use the CAF library from the system,
  if one is provided. If it is not found, the CAF submodule will be used as a
  fallback.
  [#740](https://github.com/tenzir/vast/pull/740)

## [2020.01.31]

- ⚠️ VAST is switching to a calendar-based versioning scheme starting with this
  release.
  [#739](https://github.com/tenzir/vast/pull/739)

- 🎁 When a record field has the `#index=hash` attribute, VAST will choose an
  optimized index implementation. This new index type only supports
  (in)equality queries and is therefore intended to be used with opaque types,
  such as unique identifiers or random strings.
  [#632](https://github.com/tenzir/vast/pull/632),
  [#726](https://github.com/tenzir/vast/pull/726)

- 🎁 An experimental new Python module enables querying VAST and processing
  results as [pyarrow](https://arrow.apache.org/docs/python/) tables.
  [#685](https://github.com/tenzir/vast/pull/685)

- 🐞 A bug in the quoted string parser caused a parsing failure if an escape
  character occurred in the last position.
  [#685](https://github.com/tenzir/vast/pull/685)

- ⚠️ Record field names can now be entered as quoted strings in the schema
  and expression languages. This lifts a restriction where JSON fields
  with whitespaces or special characters could not be ingested.
  [#685](https://github.com/tenzir/vast/pull/685)

- ⚠️ Two minor modifications were done in the parsing framework: (i) the parsers
  for enums and records now allow trailing separators, and (ii) the dash (`-`)
  was removed from the allowed characters of schema type names.
  [#706](https://github.com/tenzir/vast/pull/706)

- 🐞 The example configuration file contained an invalid section `vast`.
   This has been changed to the correct name `system`.
  [#705](https://github.com/tenzir/vast/pull/705)

- 🐞 A race condition in the index logic was able to lead to incomplete or empty
  result sets for `vast export`. [#703](https://github.com/tenzir/vast/pull/703)

- ⚠️ Build configuration defaults have been adapated for a better user
  experience. Installations are now relocatable by default, which can be
  reverted by configuring with `--without-relocatable`. Additionally, new sets
  of defaults named `--release` and `--debug` (renamed from `--dev-mode`) have
  been added. [#695](https://github.com/tenzir/vast/pull/695)

- 🎁 On FreeBSD, a VAST installation now includes an rc.d script that
  simpliefies spinning up a VAST node. CMake installs the script at
  `PREFIX/etc/rc.d/vast`.
  [#693](https://github.com/tenzir/vast/pull/693)

- 🎁 The long option `--config`, which sets an explicit path to the VAST
  configuration file, now also has the short option `-c`.
  [#689](https://github.com/tenzir/vast/pull/689)

- 🎁 Added *Apache Arrow* as new export format. This allows users to export
  query results as Apache Arrow record batches for processing the results
  downstream, e.g., in Python or Spark.
  [#633](https://github.com/tenzir/vast/pull/633)

- 🐞 The import process did not print statistics when importing events over UDP.
  Additionally, warnings about dropped UDP packets are no longer shown per
  packet, but rather periodically reported in a readable format.
  [#662](https://github.com/tenzir/vast/pull/662)

- 🐞 Importing events over UDP with `vast import <format> --listen :<port>/udp`
  failed to register the accountant component. This caused an unexpected
  message warning to be printed on startup and resulted in losing import
  statistics. VAST now correctly registers the accountant.
  [#655](https://github.com/tenzir/vast/pull/655)

- 🐞 PCAP ingestion failed for traces containing VLAN tags. VAST now strips
  [IEEE 802.1Q](https://en.wikipedia.org/wiki/IEEE_802.1Q) headers instead of
  skipping VLAN-tagged packets.
  [#650](https://github.com/tenzir/vast/pull/650)

- 🐞 In some cases it was possible that a source would connect to a node before
  it was fully initialized, resulting in a hanging `vast import` process.
  [#647](https://github.com/tenzir/vast/pull/647)

- 🎁 The `import pcap` command now takes an optional snapshot length via
  `--snaplen`.  If the snapshot length is set to snaplen, and snaplen is less
  than the size of a packet that is captured, only the first snaplen bytes of
  that packet will be captured and provided as packet data.
  [#642](https://github.com/tenzir/vast/pull/642)

- ⚠️ The `import pcap` command no longer takes interface names via `--read,-r`,
  but instead from a separate option named `--interface,-i`. This change has
  been made for consistency with other tools.
  [#641](https://github.com/tenzir/vast/pull/641)

## [0.2] - 2019.10.30

- 🎁 The default schema for Suricata has been updated to support the new
  `suricata.smtp` event type in Suricata 5.

- 🎁 The `export null` command retrieves data, but never prints anything. Its
  main purpose is to make benchmarking VAST easier and faster.

- ⚠️ The query language has been extended to support expression of the form
  `X == /pattern/`, where `X` is a compatible LHS extractor. Previously,
  patterns only supports the match operator `~`. The two operators have the
  same semantics when one operand is a pattern.

- 🎁 The new `pivot` command retrieves data of a related type. It inspects each
  event in a query result to find an event of the requested type. If a common
  field exists in the schema definition of the requested type, VAST will
  dynamically create a new query to fetch the contextual data according to the
  type relationship. For example, if two records `T` and `U` share the same
  field `x`, and the user requests to pivot via `T.x == 42`, then VAST will
  fetch all data for `U.x == 42`. An example use case would be to pivot from a
  Zeek or Suricata log entry to the corresponding PCAP packets.
  VAST uses the field `community_id` to pivot between the logs and the packets.
  Pivoting is currently implemented for Suricata, Zeek (with [community ID
  computation](https://github.com/corelight/bro-community-id) enabled), and
  PCAP.

- 🎁 The new `infer` command performs schema inference of input data. The
  command can deduce the input format and creates a schema definition that is
  sutable to use with the supplied data. Supported input types include Zeek TSV
  and JSONLD.

- 🐞 The user environments `LDFLAGS` were erroneously passed to `ar`. Instead,
  the user environments `ARFLAGS` are now used.

- 🐞 Exporting data with `export -n <count>` crashed when `count` was a
  multiple of the table slice size. The command now works as expected.

- 🎁 The newly added `count` comman allows counting hits for a query without
  exporting data.

- 🎁 Commands now support a `--documentation` option, which returns
  Markdown-formatted documentation text.

- ⚠️ CAF and Broker are no longer required to be installed prior to building
  VAST. These dependencies are now tracked as git submodules to ensure version
  compatibility. Specifying a custom build is still possible via the CMake
  variables `CAF_ROOT_DIR` and `BROKER_ROOT_DIR`.

- ⚠️ When exporting data in `pcap` format, it is no longer necessary to
  manually restrict the query by adding the predicate `#type == "pcap.packet"`
  to the expression. This now happens automatically because only this type
  contains the raw packet data.

- 🐞 Queries of the form `#type ~ /pattern/` used to be rejected erroneously.
  The validation code has been corrected and such queries are now working
  as expected.

- 🐞 When specifying `enum` types in the schema, ingestion failed because there
  did not exist an implementation for such types. It is now possible to use
  define enumerations in schema as expected and query them as strings.

- 🐞 Queries with the less `<` or greater `>` operators produced off-by-one
  results for the `duration` when the query contained a finer resolution than
  the index. The operator now works as expected.

- 🎁 A new schema for Argus CSV output has been added. It parses the output of
  `ra(1)`, which produces CSV output when invoked with `-L 0 -c ,`.

- ⚠️ When defining schema attributes in key-value pair form, the value no
  longer requires double-quotes. For example, `#foo=x` is now the same as
  `#foo="x"`. The form without double-quotes consumes the input until the next
  space and does not support escaping. In case an attribute value contains
  whitespace, double-quotes must be provided, e.g., `#foo="x y z"`.

- 🎁 The schema language now supports comments. A double-slash (`//`) begins a
  comment. Comments last until the end of the line, i.e., until a newline
  character (`\n`).

- ⚠️ The PCAP packet type gained the additional field `community_id` that
  contains the [Community ID](https://github.com/corelight/community-id-spec)
  flow hash. This identifier facilitates pivoting to a specific flow from data
  sources with connnection-level information, such Zeek or Suricata logs.

- 🐞 Timestamps were always printed in millisecond resolution, which lead to
  loss of precision when the internal representation had a higher resolution.
  Timestamps are now rendered up to nanosecond resolution - the maximum
  resolution supported.

- 🎁 The `import` command now supports CSV formatted data. The type for each
  column is automatically derived by matching the column names from the CSV
  header in the input with the available types from the schema definitions.

- 🐞 All query expressions in the form `#type != X` were falsely evaluated as
  `#type == X` and consequently produced wrong results. These expressions now
  behave as expected.

- 🐞 Parsers for reading log input that relied on recursive rules leaked memory
  by creating cycling references. All recursive parsers have been updated to
  break such cycles and thus no longer leak memory.

- ⚠️ Log files generally have some notion of timestamp for recorded events. To
  make the query language more intuitive, the syntax for querying time points
  thus changed from `#time` to `#timestamp`. For example,
  `#time > 2019-07-02+12:00:00` now reads `#timestamp > 2019-07-02+12:00:00`.

- 🎁 Configuring how much status information gets printed to STDERR previously
  required obscure config settings. From now on, users can simply use
  `--verbosity=<level>,-v <level>`, where `<level>` is one of `quiet`, `error`,
  `warn`, `info`, `debug`, or `trace`. However, `debug` and `trace` are only
  available for debug builds (otherwise they fall back to log level `info`).

- 🎁 The query expression language now supports *data predicates*, which are a
  shorthand for a type extractor in combination with an equality operator. For
  example, the data predicate `6.6.6.6` is the same as `:addr == 6.6.6.6`.

- 🐞 The Zeek reader failed upon encountering logs with a `double` column, as
  it occurs in `capture_loss.log`. The Zeek parser generator has been fixed to
  handle such types correctly.

- 🐞 Some queries returned duplicate events because the archive did not filter
  the result set properly. This no longer occurs after fixing the table slice
  filtering logic.

- 🎁 The `index` object in the output from `vast status` has a new field
  `statistics` for a high-level summary of the indexed data. Currently, there
  exists a nested `layouts` objects with per-layout statistics about the number
  of events indexed.

- 🎁 The `accountant` object in the output from `vast status` has a new field
  `log-file` that points to the filesystem path of the accountant log file.

- ⚠️ Default schema definitions for certain `import` formats changed from
  hard-coded to runtime-evaluated. The default location of the schema
  definition files is `$(dirname vast-executable)`/../share/vast/schema.
  Currently this is used for the Suricata JSON log reader.

- ⚠️ The default directory name for persistent state changed from `vast` to
  `vast.db`. This makes it possible to run `./vast` in the current directory
  without having to specify a different state directory on the command line.

- ⚠️ Nested types are from now on accessed by the `.`-syntax. This means
  VAST now has a unified syntax to select nested types and fields.
  For example, what used to be `zeek::http` is now just `zeek.http`.

- 🎁 Data extractors in the query language can now contain a type prefix.
  This enables an easier way to extract data from a specific type. For example,
  a query to look for Zeek conn log entries with responder IP address 1.2.3.4
  had to be written with two terms, `#type == zeek.conn && id.resp_h == 1.2.3.4`,
  because the nested id record can occur in other types as well. Such queries
  can now written more tersely as `zeek.conn.id.resp_h == 1.2.3.4`.

- 🎁 VAST gained support for importing Suricata JSON logs. The import command
  has a new suricata format that can ingest EVE JSON output.

- 🎁 The data parser now supports `count` and `integer` values according to the
  *International System for Units (SI)*. For example, `1k` is equal to `1000`
  and `1Ki` equal to `1024`.

- 🐞 The `map` data parser did not parse negative values correctly. It was not
  possible to parse strings of the form `"{-42 -> T}"` because the parser
  attempted to parse the token for the empty map `"{-}"` instead.

- 🎁 VAST can now ingest JSON data. The `import` command gained the `json`
  format, which allows for parsing line-delimited JSON (LDJSON) according to a
  user-selected type with `--type`. The `--schema` or `--schema-file` options
  can be used in conjunction to supply custom types. The JSON objects in
  the input must match the selected type, that is, the keys of the JSON object
  must be equal to the record field names and the object values must be
  convertible to the record field types.

- 🐞 The CSV printer of the `export` command used to insert 2 superfluous
  fields when formatting an event: The internal event ID and a deprecated
  internal timestamp value. Both fields have been removed from the output,
  bringing it into line with the other output formats.

- ⚠️ The (internal) option `--node` for the `import` and `export` commands
  has been renamed from `-n` to `-N`, to allow usage of `-n` for
  `--max-events`.

- 🎁 For symmetry to the `export` command, the `import` command gained the
  `--max-events,n` option to limit the number of events that will be imported.

- ⚠️ To make the export option to limit the number of events to be exported
  more idiomatic, it has been renamed from `--events,e` to `--max-events,n`.
  Now `vast export -n 42` generates at most 42 events.

- 🐞 When a node terminates during an import, the client process remained
  unaffected and kept processing input. Now the client terminates when a
  remote node terminates.

- 🎁 The `import` command gained the `--listen,l` option to receive input
  from the network. Currently only UDP is supported. Previously, one had to use
  a clever netcat pipe with enough receive buffer to achieve the same effect,
  e.g., `nc -I 1500 -p 4200 | vast import pcap`. Now this pipe degenerates to
  `vast import pcap -l`.

- 🎁 The new `--disable-accounting` option shuts off periodic gathering of
  system telemetry in the accountant actor. This also disables output in the
  `accounting.log`.

- 🐞 Evaluation of predicates with negations return incorrect results. For
  example, the expression `:addr !in 10.0.0.0/8` created a disjunction of all
  fields to which `:addr` resolved, without properly applying De-Morgan. The
  same bug also existed for key extractors. De-Morgan is now applied properly
  for the operations `!in` and `!~`.


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
