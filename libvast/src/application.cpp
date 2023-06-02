//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/application.hpp"

#include "vast/command.hpp"
#include "vast/config.hpp"
#include "vast/configuration.hpp"
#include "vast/count_command.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/process.hpp"
#include "vast/error.hpp"
#include "vast/import_command.hpp"
#include "vast/plugin.hpp"
#include "vast/remote_command.hpp"
#include "vast/start_command.hpp"
#include "vast/writer_command.hpp"

#include <fmt/color.h>

namespace vast {

namespace {

void add_root_opts(command& cmd) {
  const auto binary = detail::objectpath();
  auto schema_paths = std::vector<std::filesystem::path>{"/etc/vast/schema"};
  if (binary) {
    schema_paths.push_back(binary->parent_path().parent_path() / "share"
                           / "vast" / "schema");
  }
  const auto module_desc
    = fmt::format("list of directories to look for schema files ([{}])",
                  fmt::join(schema_paths, ", "));
  cmd.options.add<std::string>("?vast", "config",
                               "path to a configuration file");
  cmd.options.add<bool>(
    "?vast", "bare-mode",
    "disable user and system configuration, schema and plugin "
    "directories lookup and static and dynamic plugin "
    "autoloading (this may only be used on the command line)");
  cmd.options.add<bool>("?vast", "detach-components",
                        "create dedicated threads for some "
                        "components");
  cmd.options.add<bool>(
    "?vast", "allow-unsafe-pipelines",
    "allow unsafe location overrides for pipelines with the "
    "'local' and 'remote' keywords, e.g., remotely reading from "
    "a file");
  cmd.options.add<std::string>("?vast", "console-verbosity",
                               "output verbosity level on the "
                               "console");
  cmd.options.add<std::string>("?vast", "console-format",
                               "format string for logging to the "
                               "console");
  cmd.options.add<caf::config_value::list>("?vast", "schema-dirs", module_desc);
  cmd.options.add<std::string>("?vast", "db-directory,d",
                               "directory for persistent state");
  cmd.options.add<std::string>("?vast", "log-file", "log filename");
  cmd.options.add<std::string>("?vast", "client-log-file",
                               "client log file (default: "
                               "disabled)");
  cmd.options.add<int64_t>("?vast", "log-queue-size",
                           "the queue size for the logger");
  cmd.options.add<std::string>("?vast", "endpoint,e", "node endpoint");
  cmd.options.add<std::string>("?vast", "node-id,i",
                               "the unique ID of this node");
  cmd.options.add<bool>("?vast", "node,N",
                        "spawn a node instead of connecting to one");
  cmd.options.add<bool>("?vast", "enable-metrics",
                        "keep track of performance metrics");
  cmd.options.add<caf::config_value::list>("?vast", "plugin-dirs",
                                           "additional directories "
                                           "to load plugins from");
  cmd.options.add<caf::config_value::list>(
    "?vast", "plugins",
    "plugins to load at startup; the special values 'bundled' "
    "and 'all' enable autoloading of bundled and all plugins "
    "respectively.");
  cmd.options.add<std::string>("?vast", "aging-frequency",
                               "interval between two aging "
                               "cycles");
  cmd.options.add<std::string>("?vast", "aging-query",
                               "query for aging out obsolete data");
  cmd.options.add<std::string>("?vast", "store-backend",
                               "store plugin to use for imported "
                               "data");
  cmd.options.add<std::string>("?vast", "connection-timeout",
                               "the timeout for connecting to "
                               "a VAST server (default: 5m)");
  cmd.options.add<std::string>("?vast", "connection-retry-delay",
                               "the delay between vast node connection attempts"
                               "a VAST server (default: 3s)");
  cmd.options.add<int64_t>("?vast", "max-partition-size",
                           "maximum number of events in a "
                           "partition");
  cmd.options.add<duration>("?vast", "active-partition-timeout",
                            "timespan after which an active partition is "
                            "forcibly flushed");
  cmd.options.add<int64_t>("?vast", "max-resident-partitions",
                           "maximum number of in-memory "
                           "partitions");
  cmd.options.add<int64_t>("?vast", "max-taste-partitions",
                           "maximum number of immediately "
                           "scheduled partitions");
  cmd.options.add<int64_t>("?vast", "max-queries,q",
                           "maximum number of "
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

auto make_status_command() {
  return std::make_unique<command>(
    "status",
    "shows properties of a server process by component; optional positional "
    "arguments allow for filtering by component name",
    opts("?vast.status")
      .add<std::string>("timeout", "how long to wait for components to report")
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
    {"import csv", import_command},
    {"import json", import_command},
    {"import suricata", import_command},
    {"import syslog", import_command},
    {"import test", import_command},
    {"import zeek", import_command},
    {"import zeek-json", import_command},
    {"import arrow", import_command},
    {"start", start_command},
    {"status", remote_command},
  };
  // clang-format on
  for (auto& plugin : plugins::get()) {
    if (auto* reader = plugin.as<reader_plugin>()) {
      result.emplace(fmt::format("import {}", reader->reader_format()),
                     import_command);
    }
    if (auto* writer = plugin.as<writer_plugin>()) {
      result.emplace(fmt::format("export {}", writer->writer_format()),
                     make_writer_command(writer->writer_format()));
    }
  }
  return result;
} // namespace

auto make_root_command(std::string_view name) {
  using namespace std::string_literals;
  auto ob = opts("?vast");
  auto root = std::make_unique<command>(name, "", std::move(ob));
  add_root_opts(*root);
  root->add_subcommand(make_count_command());
  root->add_subcommand(make_export_command());
  root->add_subcommand(make_import_command());
  root->add_subcommand(make_start_command());
  root->add_subcommand(make_status_command());
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
  // We're only interested in the application name, not in its path. For
  // example, argv[0] might contain "./build/release/bin/vast" and we are only
  // interested in "vast".
  const auto last_slash = path.find_last_of('/');
  const auto name
    = last_slash == std::string_view::npos ? path : path.substr(last_slash + 1);
  if (name == "tenzir-node") {
    auto cmd = make_start_command();
    cmd->name = "";
    add_root_opts(*cmd);
    return {
      std::move(cmd),
      command::factory{
        {"", start_command},
      },
    };
  }
  if (name == "tenzir") {
    const auto* exec = plugins::find<command_plugin>("exec");
    VAST_ASSERT(exec);
    auto [cmd, cmd_factory] = exec->make_command();
    VAST_ASSERT(cmd);
    add_root_opts(*cmd);
    VAST_ASSERT(cmd_factory.contains("exec"));
    cmd->name = "";
    return {
      std::move(cmd),
      command::factory{
        {"", cmd_factory["exec"]},
        {"exec", cmd_factory["exec"]},
      },
    };
  }
  if (name == "vast") {
    fmt::print(stderr, fmt::emphasis::bold, "\nVAST is now called Tenzir.\n\n");
    fmt::print(stderr, "For more information, see the announcement at ");
    fmt::print(stderr, fmt::emphasis::underline,
               "https://docs.tenzir.com/blog/vast-to-tenzir");
    fmt::print(stderr, ".\n\ntl;dr:\n- Use ");
    fmt::print(stderr, fmt::emphasis::bold, "tenzir-node");
    fmt::print(stderr, " instead of ");
    fmt::print(stderr, fmt::emphasis::bold, "vast start");
    fmt::print(stderr, "\n- Use ");
    fmt::print(stderr, fmt::emphasis::bold, "tenzir");
    fmt::print(stderr, " instead of ");
    fmt::print(stderr, fmt::emphasis::bold, "vast exec");
    fmt::print(stderr, "\n- Use ");
    fmt::print(stderr, fmt::emphasis::bold, "tenzir-ctl");
    fmt::print(stderr,
               " for all other commands\n- Move your configuration from ");
    fmt::print(stderr, fmt::emphasis::bold, "<prefix>/etc/vast/vast.yaml");
    fmt::print(stderr, " to ");
    fmt::print(stderr, fmt::emphasis::bold, "<prefix>/etc/tenzir/tenzir.yaml");
    fmt::print(stderr, "\n- Move your configuration from ");
    fmt::print(stderr, fmt::emphasis::bold, "$XDG_CONFIG_HOME/vast/vast.yaml");
    fmt::print(stderr, " to ");
    fmt::print(stderr, fmt::emphasis::bold,
               "$XDG_CONFIG_HOME/tenzir/tenzir.yaml");
    fmt::print(stderr, "\n- In your configuration, replace ");
    fmt::print(stderr, fmt::emphasis::bold, "vast:");
    fmt::print(stderr, " with ");
    fmt::print(stderr, fmt::emphasis::bold, "tenzir:");
    fmt::print(stderr, "\n- Prefix environment variables with ");
    fmt::print(stderr, fmt::emphasis::bold, "TENZIR_");
    fmt::print(stderr, " instead of ");
    fmt::print(stderr, fmt::emphasis::bold, "VAST_");
    fmt::print(stderr, "\n\n");
  }
  auto root = make_root_command(name);
  auto root_factory = make_command_factory();
  // Add additional commands from plugins.
  for (const auto* plugin : plugins::get<command_plugin>()) {
    auto [cmd, cmd_factory] = plugin->make_command();
    if (!cmd || cmd_factory.empty())
      continue;
    root->add_subcommand(std::move(cmd));
    root_factory.insert(std::make_move_iterator(cmd_factory.begin()),
                        std::make_move_iterator(cmd_factory.end()));
  }
  return {
    std::move(root),
    std::move(root_factory),
  };
}

void render_error(const command& root, const caf::error& err,
                  std::ostream& os) {
  if (!err || err == ec::silent)
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

} // namespace vast
