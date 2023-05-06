//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/application.hpp"

#include "vast/command.hpp"
#include "vast/config.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/process.hpp"
#include "vast/error.hpp"
#include "vast/plugin.hpp"
#include "vast/system/configuration.hpp"
#include "vast/system/count_command.hpp"
#include "vast/system/import_command.hpp"
#include "vast/system/infer_command.hpp"
#include "vast/system/remote_command.hpp"
#include "vast/system/start_command.hpp"
#include "vast/system/stop_command.hpp"
#include "vast/system/version_command.hpp"
#include "vast/system/writer_command.hpp"

namespace vast::system {

namespace {

command::opts_builder add_index_opts(command::opts_builder ob) {
  return std::move(ob)
    .add<int64_t>("max-partition-size", "maximum number of events in a "
                                        "partition")
    .add<duration>("active-partition-timeout",
                   "timespan after which an active partition is "
                   "forcibly flushed")
    .add<int64_t>("max-resident-partitions", "maximum number of in-memory "
                                             "partitions")
    .add<int64_t>("max-taste-partitions", "maximum number of immediately "
                                          "scheduled partitions")
    .add<int64_t>("max-queries,q", "maximum number of "
                                   "concurrent queries");
}

auto make_count_command() {
  return std::make_unique<command>(
    "count", "count hits for a query without exporting data",
    opts("?vast.count")
      .add<bool>("disable-taxonomies", "don't substitute taxonomy identifiers")
      .add<bool>("estimate,e", "estimate an upper bound by "
                               "skipping candidate checks"));
}

auto make_export_command() {
  auto export_ = std::make_unique<command>(
    "export",
    "exports query results to STDOUT or file, expects a subcommand to select "
    "the format",
    opts("?vast.export")
      .add<bool>("continuous,c", "marks a query as continuous")
      .add<bool>("unified,u", "marks a query as unified")
      .add<bool>("disable-taxonomies", "don't substitute taxonomy identifiers")
      .add<bool>("low-priority", "respond to other queries first")
      .add<std::string>("timeout", "timeout to stop the export after")
      // We don't expose the `preserve-ids` option to the user because it
      // doesnt' affect the formatted output.
      //.add<bool>("preserve-ids", "don't substitute taxonomy identifiers")
      .add<int64_t>("max-events,n", "maximum number of results")
      .add<std::string>("read,r", "path for reading the query")
      .add<std::string>("write,w", "path to write events to")
      .add<bool>("uds,d", "treat -w as UNIX domain socket to connect to"));
  export_->add_subcommand("zeek", "exports query results in Zeek format",
                          opts("?vast.export.zeek")
                            .add<bool>("disable-timestamp-tags",
                                       "whether the output should contain "
                                       "#open/#close tags"));
  export_->add_subcommand("csv", "exports query results in CSV format",
                          opts("?vast.export.csv"));
  export_->add_subcommand("ascii", "exports query results in ASCII format",
                          opts("?vast.export.ascii"));
  export_->add_subcommand(
    "json", "exports query results in JSON format",
    opts("?vast.export.json")
      .add<bool>("flatten", "flatten nested objects into "
                            "the top-level")
      .add<bool>("numeric-durations", "render durations as numbers as opposed "
                                      "to human-readable strings with up to "
                                      "two decimal places")
      .add<bool>("omit-nulls", "omit null fields in JSON objects")
      .add<bool>("omit-empty-records", "omit empty records in JSON objects")
      .add<bool>("omit-empty-lists", "omit empty lists in JSON objects")
      .add<bool>("omit-empty-maps", "omit empty maps in JSON objects")
      .add<bool>("omit-empty", "omit all empty values and nulls in JSON "
                               "objects"));
  export_->add_subcommand("null",
                          "exports query without printing them (debug option)",
                          opts("?vast.export.null"));
  export_->add_subcommand("arrow",
                          "exports query results in Arrow format with separate "
                          "IPC streams for each schema, all concatenated "
                          "together",
                          opts("?vast.export.arrow"));

  for (const auto& plugin : plugins::get()) {
    if (const auto* writer = plugin.as<writer_plugin>()) {
      auto opts_category
        = fmt::format("?vast.export.{}", writer->writer_format());
      export_->add_subcommand(writer->writer_format(), writer->writer_help(),
                              writer->writer_options(opts(opts_category)));
    }
  }
  return export_;
}

auto make_infer_command() {
  return std::make_unique<command>(
    "infer", "infers the schema from data",
    opts("?vast.infer")
      .add<int64_t>("buffer,b", "maximum number of bytes to buffer")
      .add<std::string>("read,r", "path to the input data"));
}

auto make_kill_command() {
  return std::make_unique<command>("kill", "terminates a component",
                                   opts("?vast.kill"), false);
}

auto make_peer_command() {
  return std::make_unique<command>("peer", "peers with another node",
                                   opts("?vast.peer"), false);
}

auto make_send_command() {
  return std::make_unique<command>(
    "send", "sends a message to a registered actor", opts("?vast.send"), false);
}

auto make_spawn_source_command() {
  auto spawn_source = std::make_unique<command>(
    "source", "creates a new source inside the node",
    opts("?vast.spawn.source")
      .add<std::string>("batch-encoding", "encoding type of table slices")
      .add<int64_t>("batch-size", "upper bound for the size of a table slice")
      .add<std::string>("batch-timeout", "timeout after which batched "
                                         "table slices are forwarded")
      .add<std::string>("listen,l", "the endpoint to listen on "
                                    "([host]:port/type)")
      .add<int64_t>("max-events,n", "the maximum number of events to import")
      .add<std::string>("read,r", "path to input where to read events from")
      .add<std::string>("read-timeout", "timeout for waiting for incoming data")
      .add<std::string>("schema,S", "alternate schema as string")
      .add<std::string>("schema-file,s", "path to alternate schema")
      .add<std::string>("type,t", "filter event type based on prefix matching")
      .add<bool>("uds,d", "treat -r as listening UNIX domain socket"));
  spawn_source->add_subcommand("arrow",
                               "creates a new Arrow IPC source inside the node",
                               opts("?vast.spawn.source.arrow"));
  spawn_source->add_subcommand(
    "csv", "creates a new CSV source inside the node",
    opts("?vast.spawn.source.csv")
      .add<std::string>("separator", "the single-character separator (default: "
                                     "',')"));
  spawn_source->add_subcommand(
    "json", "creates a new JSON source inside the node",
    opts("?vast.spawn.source.json")
      .add<std::string>("selector", "read the event type from the given field "
                                    "(specify as '<field>[:<prefix>]')"));
  spawn_source->add_subcommand("suricata",
                               "creates a new Suricata source inside the node",
                               opts("?vast.spawn.source.suricata"));
  spawn_source->add_subcommand("syslog",
                               "creates a new Syslog source inside the node",
                               opts("?vast.spawn.source.syslog"));
  spawn_source->add_subcommand(
    "test", "creates a new test source inside the node",
    opts("?vast.spawn.source.test").add<int64_t>("seed", "the PRNG seed"));
  spawn_source->add_subcommand("zeek",
                               "creates a new Zeek source inside the node",
                               opts("?vast.spawn.source.zeek"));
  for (const auto& plugin : plugins::get()) {
    if (const auto* reader = plugin.as<reader_plugin>()) {
      auto opts_category
        = fmt::format("?vast.spawn.source.{}", reader->reader_format());
      spawn_source->add_subcommand(reader->reader_format(),
                                   reader->reader_help(),
                                   reader->reader_options(opts(opts_category)));
    }
  }
  return spawn_source;
}

auto make_spawn_sink_command() {
  auto spawn_sink = std::make_unique<command>(
    "sink", "creates a new sink",
    opts("?vast.spawn.sink")
      .add<std::string>("write,w", "path to write events to")
      .add<bool>("uds,d", "treat -w as UNIX domain socket"),
    false);
  spawn_sink->add_subcommand("zeek", "creates a new Zeek sink",
                             opts("?vast.spawn.sink.zeek"));
  spawn_sink->add_subcommand("ascii", "creates a new ASCII sink",
                             opts("?vast.spawn.sink.ascii"));
  spawn_sink->add_subcommand("csv", "creates a new CSV sink",
                             opts("?vast.spawn.sink.csv"));
  spawn_sink->add_subcommand("json", "creates a new JSON sink",
                             opts("?vast.spawn.sink.json"));
  for (const auto& plugin : plugins::get()) {
    if (const auto* writer = plugin.as<writer_plugin>()) {
      auto opts_category
        = fmt::format("?vast.spawn.sink.{}", writer->writer_format());
      spawn_sink->add_subcommand(writer->writer_format(), writer->writer_help(),
                                 writer->writer_options(opts(opts_category)));
    }
  }
  return spawn_sink;
}

auto make_spawn_command() {
  auto spawn = std::make_unique<command>("spawn", "creates a new component",
                                         opts("?vast.spawn"));
  spawn->add_subcommand("accountant", "spawns the accountant",
                        opts("?vast.spawn.accountant"), false);
  spawn->add_subcommand(
    "exporter", "creates a new exporter",
    opts("?vast.spawn.exporter")
      .add<bool>("continuous,c", "marks a query as continuous")
      .add<bool>("unified,u", "marks a query as unified")
      .add<int64_t>("events,e", "maximum number of results"),
    false);
  spawn->add_subcommand("importer", "creates a new importer",
                        opts("?vast.spawn.importer"), false);
  spawn->add_subcommand("index", "creates a new index",
                        add_index_opts(opts("?vast.spawn.index")), false);
  spawn->add_subcommand(make_spawn_source_command());
  spawn->add_subcommand(make_spawn_sink_command());
  return spawn;
}

auto make_status_command() {
  return std::make_unique<command>(
    "status",
    "shows properties of a server process by component; optional positional "
    "arguments allow for filtering by component name",
    opts("?vast.status")
      .add<bool>("detailed", "add more information to the output")
      .add<bool>("debug", "include extra debug information"));
}

auto make_start_command() {
  return std::make_unique<command>(
    "start", "starts a node",
    opts("?vast.start")
      .add<bool>("print-endpoint", "print the client endpoint on stdout")
      .add<caf::config_value::list>("commands", "an ordered list of commands "
                                                "to run inside the node after "
                                                "starting")
      .add<int64_t>("disk-budget-check-interval", "time between two disk size "
                                                  "scans")
      .add<std::string>("disk-budget-check-binary",
                        "binary to run to determine current disk usage")
      .add<std::string>("disk-budget-high", "high-water mark for disk budget")
      .add<std::string>("disk-budget-low", "low-water mark for disk budget")
      .add<int64_t>("disk-budget-step-size", "number of partitions to erase "
                                             "before re-checking size"));
}

auto make_stop_command() {
  return std::make_unique<command>("stop", "stops a node", opts("?vast.stop"));
}

auto make_version_command() {
  return std::make_unique<command>("version", "prints the software version",
                                   opts("?vast.version"));
}

auto make_command_factory() {
  // When updating this list, remember to update its counterpart in node.cpp as
  // well iff necessary
  // clang-format off
  auto result = command::factory{
    {"count", count_command},
    {"export ascii", make_writer_command("ascii")},
    {"export csv", make_writer_command("csv")},
    {"export json", make_writer_command("json")},
    {"export null", make_writer_command("null")},
    {"export arrow", make_writer_command("arrow")},
    {"export zeek", make_writer_command("zeek")},
    {"infer", infer_command},
    {"import csv", import_command},
    {"import json", import_command},
    {"import suricata", import_command},
    {"import syslog", import_command},
    {"import test", import_command},
    {"import zeek", import_command},
    {"import zeek-json", import_command},
    {"import arrow", import_command},
    {"kill", remote_command},
    {"peer", remote_command},
    {"send", remote_command},
    {"spawn accountant", remote_command},
    {"spawn eraser", remote_command},
    {"spawn exporter", remote_command},
    {"spawn importer", remote_command},
    {"spawn index", remote_command},
    {"spawn sink ascii", remote_command},
    {"spawn sink csv", remote_command},
    {"spawn sink json", remote_command},
    {"spawn sink zeek", remote_command},
    {"spawn source arrow", remote_command},
    {"spawn source csv", remote_command},
    {"spawn source json", remote_command},
    {"spawn source suricata", remote_command},
    {"spawn source syslog", remote_command},
    {"spawn source test", remote_command},
    {"spawn source zeek", remote_command},
    {"spawn source zeek-json", remote_command},
    {"start", start_command},
    {"status", remote_command},
    {"stop", stop_command},
    {"version", version_command},
  };
  // clang-format on
  for (auto& plugin : plugins::get()) {
    if (auto* reader = plugin.as<reader_plugin>()) {
      result.emplace(fmt::format("import {}", reader->reader_format()),
                     import_command);
      result.emplace(fmt::format("spawn source {}", reader->reader_format()),
                     remote_command);
    }
    if (auto* writer = plugin.as<writer_plugin>()) {
      result.emplace(fmt::format("export {}", writer->writer_format()),
                     make_writer_command(writer->writer_format()));
      result.emplace(fmt::format("spawn sink {}", writer->writer_format()),
                     remote_command);
    }
  }
  return result;
} // namespace

auto make_root_command(std::string_view path) {
  using namespace std::string_literals;
  // We're only interested in the application name, not in its path. For
  // example, argv[0] might contain "./build/release/bin/vast" and we are only
  // interested in "vast".
  path.remove_prefix(std::min(path.find_last_of('/') + 1, path.size()));
  const auto binary = detail::objectpath();
  auto module_desc
    = "list of directories to look for schema files ([/etc/vast/schema"s;
  if (binary) {
    const auto relative_module_dir
      = binary->parent_path().parent_path() / "share" / "vast" / "schema";
    module_desc += ", " + relative_module_dir.string();
  }
  module_desc += "])";
  auto ob
    = opts("?vast")
        .add<std::string>("config", "path to a configuration file")
        .add<bool>("bare-mode",
                   "disable user and system configuration, schema and plugin "
                   "directories lookup and static and dynamic plugin "
                   "autoloading (this may only be used on the command line)")
        .add<bool>("detach-components", "create dedicated threads for some "
                                        "components")
        .add<bool>("allow-unsafe-pipelines",
                   "allow unsafe location overrides for pipelines with the "
                   "'local' and 'remote' keywords, e.g., remotely reading from "
                   "a file")
        .add<std::string>("console-verbosity", "output verbosity level on the "
                                               "console")
        .add<std::string>("console-format", "format string for logging to the "
                                            "console")
        .add<caf::config_value::list>("schema-dirs", module_desc.c_str())
        .add<std::string>("db-directory,d", "directory for persistent state")
        .add<std::string>("log-file", "log filename")
        .add<std::string>("client-log-file", "client log file (default: "
                                             "disabled)")
        .add<int64_t>("log-queue-size", "the queue size for the logger")
        .add<std::string>("endpoint,e", "node endpoint")
        .add<std::string>("node-id,i", "the unique ID of this node")
        .add<bool>("node,N", "spawn a node instead of connecting to one")
        .add<bool>("enable-metrics", "keep track of performance metrics")
        .add<caf::config_value::list>("plugin-dirs", "additional directories "
                                                     "to load plugins from")
        .add<caf::config_value::list>(
          "plugins", "plugins to load at startup; the special values 'bundled' "
                     "and 'all' enable autoloading of bundled and all plugins "
                     "respectively.")
        .add<std::string>("aging-frequency", "interval between two aging "
                                             "cycles")
        .add<std::string>("aging-query", "query for aging out obsolete data")
        .add<std::string>("store-backend", "store plugin to use for imported "
                                           "data")
        .add<std::string>("connection-timeout", "the timeout for connecting to "
                                                "a VAST server (default: 5m)")
        .add<std::string>("connection-retry-delay",
                          "the delay between vast node connection attempts"
                          "a VAST server (default: 3s)");
  ob = add_index_opts(std::move(ob));
  auto root = std::make_unique<command>(path, "", std::move(ob));
  root->add_subcommand(make_count_command());
  root->add_subcommand(make_export_command());
  root->add_subcommand(make_import_command());
  root->add_subcommand(make_infer_command());
  root->add_subcommand(make_kill_command());
  root->add_subcommand(make_peer_command());
  root->add_subcommand(make_send_command());
  root->add_subcommand(make_spawn_command());
  root->add_subcommand(make_start_command());
  root->add_subcommand(make_status_command());
  root->add_subcommand(make_stop_command());
  root->add_subcommand(make_version_command());
  return root;
}

} // namespace

std::unique_ptr<command> make_import_command() {
  auto import_ = std::make_unique<command>(
    "import", "imports data from STDIN or file",
    opts("?vast.import")
      .add<std::string>("batch-encoding", "encoding type of table slices")
      .add<int64_t>("batch-size", "upper bound for the size of a table slice")
      .add<std::string>("batch-timeout", "timeout after which batched "
                                         "table slices are forwarded")
      .add<bool>("blocking,b", "block until the IMPORTER forwarded all data")
      .add<std::string>("listen,l", "the endpoint to listen on "
                                    "([host]:port/type)")
      .add<int64_t>("max-events,n", "the maximum number of events to import")
      .add<std::string>("read,r", "path to input where to read events from")
      .add<std::string>("read-timeout", "timeout for waiting for incoming data")
      .add<std::string>("schema,S", "alternate schema as string")
      .add<std::string>("schema-file,s", "path to alternate schema")
      .add<std::string>("type,t", "filter event type based on prefix matching")
      .add<bool>("uds,d", "treat -r as listening UNIX domain socket"));
  import_->add_subcommand("zeek", "imports Zeek TSV logs from STDIN or file",
                          opts("?vast.import.zeek"));
  import_->add_subcommand("zeek-json",
                          "imports Zeek JSON logs from STDIN or file",
                          opts("?vast.import.zeek-json"));
  import_->add_subcommand(
    "csv", "imports CSV logs from STDIN or file",
    opts("?vast.import.csv")
      .add<std::string>("separator", "the single-character separator (default: "
                                     "',')"));
  import_->add_subcommand(
    "json", "imports JSON with schema",
    opts("?vast.import.json")
      .add<std::string>("selector", "read the event type from the given field "
                                    "(specify as '<field>[:<prefix>]')"));
  import_->add_subcommand("suricata", "imports suricata EVE JSON",
                          opts("?vast.import.suricata"));
  import_->add_subcommand("syslog", "imports syslog messages",
                          opts("?vast.import.syslog"));
  import_->add_subcommand("arrow", "import from an Arrow IPC stream",
                          opts("?vast.import.arrow"));
  import_->add_subcommand(
    "test", "imports random data for testing or benchmarking",
    opts("?vast.import.test").add<int64_t>("seed", "the PRNG seed"));
  for (const auto& plugin : plugins::get()) {
    if (const auto* reader = plugin.as<reader_plugin>()) {
      auto opts_category
        = fmt::format("?vast.import.{}", reader->reader_format());
      import_->add_subcommand(reader->reader_format(), reader->reader_help(),
                              reader->reader_options(opts(opts_category)));
    }
  }
  return import_;
}

std::pair<std::unique_ptr<command>, command::factory>
make_application(std::string_view path) {
  auto root = make_root_command(path);
  auto root_factory = make_command_factory();
  // Add additional commands from plugins.
  for (auto& plugin : plugins::get()) {
    if (auto* cp = plugin.as<command_plugin>()) {
      auto&& [cmd, cmd_factory] = cp->make_command();
      if (!cmd || cmd_factory.empty())
        continue;
      root->add_subcommand(std::move(cmd));
      root_factory.insert(std::make_move_iterator(cmd_factory.begin()),
                          std::make_move_iterator(cmd_factory.end()));
    }
  }
  return {std::move(root), std::move(root_factory)};
}

void render_error(const command& root, const caf::error& err,
                  std::ostream& os) {
  if (!err)
    // The user most likely killed the process via CTRL+C, print nothing.
    return;
  os << render(err) << '\n';
  if (err.category() == caf::type_id_v<vast::ec>) {
    auto x = static_cast<vast::ec>(err.code());
    switch (x) {
      default:
        break;
      case ec::invalid_subcommand:
      case ec::missing_subcommand:
      case ec::unrecognized_option: {
        auto ctx = err.context();
        if (ctx.match_element<std::string>(1)) {
          auto name = ctx.get_as<std::string>(1);
          if (auto cmd = resolve(root, name))
            helptext(*cmd, os);
        } else {
          VAST_ASSERT(!"User visible error contexts must consist of strings!");
        }
        break;
      }
    }
  }
}

command::opts_builder opts(std::string_view category) {
  return command::opts(category);
}

} // namespace vast::system
