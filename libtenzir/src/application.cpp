//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/application.hpp"

#include "tenzir/command.hpp"
#include "tenzir/config.hpp"
#include "tenzir/configuration.hpp"
#include "tenzir/count_command.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/process.hpp"
#include "tenzir/error.hpp"
#include "tenzir/forked_command.hpp"
#include "tenzir/import_command.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/remote_command.hpp"
#include "tenzir/start_command.hpp"
#include "tenzir/writer_command.hpp"

#include <fmt/color.h>

namespace tenzir {

namespace {

void add_root_opts(command& cmd) {
  const auto binary = detail::objectpath();
  auto schema_paths = std::vector<std::filesystem::path>{"/etc/tenzir/schema"};
  if (binary) {
    schema_paths.push_back(binary->parent_path().parent_path() / "share"
                           / "tenzir" / "schema");
  }
  const auto module_desc
    = fmt::format("list of directories to look for schema files ([{}])",
                  fmt::join(schema_paths, ", "));
  cmd.options.add<std::string>("?tenzir", "config",
                               "path to a configuration file");
  cmd.options.add<bool>(
    "?tenzir", "bare-mode",
    "disable user and system configuration, schema and plugin "
    "directories lookup and static and dynamic plugin "
    "autoloading (this may only be used on the command line)");
  cmd.options.add<bool>(
    "?tenzir", "allow-unsafe-pipelines",
    "allow unsafe location overrides for pipelines with the "
    "'local' and 'remote' keywords, e.g., remotely reading from "
    "a file");
  cmd.options.add<std::string>("?tenzir", "console-verbosity",
                               "output verbosity level on the "
                               "console");
  cmd.options.add<std::string>("?tenzir", "console-format",
                               "format string for logging to the "
                               "console");
  cmd.options.add<caf::config_value::list>(
    "?tenzir", "components", "list of components to spawn in a node");
  cmd.options.add<caf::config_value::list>("?tenzir", "schema-dirs",
                                           module_desc);
  cmd.options.add<std::string>("?tenzir", "db-directory",
                               "deprecated; use state-directory instead");
  cmd.options.add<std::string>("?tenzir", "state-directory,d",
                               "directory for persistent state");
  cmd.options.add<std::string>("?tenzir", "cache-directory",
                               "directory for runtime state");
  cmd.options.add<std::string>("?tenzir", "log-file", "log filename");
  cmd.options.add<std::string>("?tenzir", "client-log-file",
                               "client log file (default: "
                               "disabled)");
  cmd.options.add<int64_t>("?tenzir", "log-queue-size",
                           "the queue size for the logger");
  cmd.options.add<std::string>("?tenzir", "endpoint,e", "node endpoint");
  cmd.options.add<std::string>("?tenzir", "node-id,i",
                               "the unique ID of this node");
  cmd.options.add<bool>("?tenzir", "node,N",
                        "spawn a node instead of connecting to one");
  cmd.options.add<bool>("?tenzir", "enable-metrics",
                        "keep track of performance metrics");
  cmd.options.add<caf::config_value::list>("?tenzir", "plugin-dirs",
                                           "additional directories "
                                           "to load plugins from");
  cmd.options.add<caf::config_value::list>(
    "?tenzir", "plugins",
    "plugins to load at startup; the special values 'bundled' "
    "and 'all' enable autoloading of bundled and all plugins "
    "respectively.");
  cmd.options.add<caf::config_value::list>(
    "?tenzir", "disable-plugins",
    "plugins and builtins to explicitly disable; use to forbid use of "
    "operators, connectors, or formats by policy.");
  cmd.options.add<std::string>("?tenzir", "aging-frequency",
                               "interval between two aging "
                               "cycles");
  cmd.options.add<std::string>("?tenzir", "aging-query",
                               "query for aging out obsolete data");
  cmd.options.add<std::string>("?tenzir", "store-backend",
                               "store plugin to use for imported "
                               "data");
  cmd.options.add<std::string>("?tenzir", "connection-timeout",
                               "the timeout for connecting to "
                               "a Tenzir server (default: 5m)");
  cmd.options.add<std::string>("?tenzir", "connection-retry-delay",
                               "the delay between tenzir node connection "
                               "attempts"
                               "a Tenzir server (default: 3s)");
  cmd.options.add<int64_t>("?tenzir", "max-partition-size",
                           "maximum number of events in a "
                           "partition");
  cmd.options.add<duration>("?tenzir", "active-partition-timeout",
                            "timespan after which an active partition is "
                            "forcibly flushed (default: 30s)");
  cmd.options.add<int64_t>("?tenzir", "max-resident-partitions",
                           "maximum number of in-memory "
                           "partitions (default: 1)");
  cmd.options.add<int64_t>("?tenzir", "max-taste-partitions",
                           "maximum number of immediately "
                           "scheduled partitions");
  cmd.options.add<int64_t>("?tenzir", "max-queries,q",
                           "maximum number of "
                           "concurrent queries");
  cmd.options.add<duration>("?tenzir", "rebuild-interval",
                            "timespan after which an automatic rebuild is "
                            "triggered (default: 2h)");
}

auto make_count_command() {
  return std::make_unique<command>(
    "count", "count hits for a query without exporting data",
    opts("?tenzir.count")
      .add<bool>("disable-taxonomies", "don't substitute taxonomy identifiers")
      .add<bool>("estimate,e", "estimate an upper bound by "
                               "skipping candidate checks"));
}

auto make_export_command() {
  auto export_ = std::make_unique<command>(
    "export",
    "exports query results to STDOUT or file, expects a subcommand to select "
    "the format",
    opts("?tenzir.export")
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
                          opts("?tenzir.export.zeek")
                            .add<bool>("disable-timestamp-tags",
                                       "whether the output should contain "
                                       "#open/#close tags"));
  export_->add_subcommand("csv", "exports query results in CSV format",
                          opts("?tenzir.export.csv"));
  export_->add_subcommand("ascii", "exports query results in ASCII format",
                          opts("?tenzir.export.ascii"));
  export_->add_subcommand(
    "json", "exports query results in JSON format",
    opts("?tenzir.export.json")
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
                          opts("?tenzir.export.null"));
  export_->add_subcommand("arrow",
                          "exports query results in Arrow format with separate "
                          "IPC streams for each schema, all concatenated "
                          "together",
                          opts("?tenzir.export.arrow"));
  return export_;
}

auto make_forked_command() {
  return std::make_unique<command>("forked", "for internal use only",
                                   opts("?tenzir.forked"));
}

auto make_status_command() {
  return std::make_unique<command>(
    "status",
    "shows properties of a server process by component; optional positional "
    "arguments allow for filtering by component name",
    opts("?tenzir.status")
      .add<std::string>("timeout", "how long to wait for components to report")
      .add<bool>("detailed", "add more information to the output")
      .add<bool>("debug", "include extra debug information"));
}

auto make_start_command() {
  return std::make_unique<command>(
    "start", "starts a node",
    opts("?tenzir.start")
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
    {"forked", forked_command},
    {"import csv", import_command},
    {"import json", import_command},
    {"import suricata", import_command},
    {"import syslog", import_command},
    {"import test", import_command},
    {"import zeek", import_command},
    {"import zeek-json", import_command},
    {"import arrow", import_command},
    {"status", remote_command},
  };
  // clang-format on
  return result;
} // namespace

auto make_root_command(std::string_view name) {
  using namespace std::string_literals;
  auto ob = opts("?tenzir");
  auto root = std::make_unique<command>(name, "", std::move(ob));
  add_root_opts(*root);
  root->add_subcommand(make_count_command());
  root->add_subcommand(make_export_command());
  root->add_subcommand(make_forked_command());
  root->add_subcommand(make_import_command());
  root->add_subcommand(make_status_command());
  return root;
}

} // namespace

std::unique_ptr<command> make_import_command() {
  auto import_ = std::make_unique<command>(
    "import", "imports data from STDIN or file",
    opts("?tenzir.import")
      .add<std::string>("batch-encoding", "encoding type of table slices")
      .add<int64_t>("batch-size", "upper bound for the size of a table slice")
      .add<std::string>("batch-timeout", "timeout after which batched table "
                                         "slices are forwarded (default: 1s)")
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
                          opts("?tenzir.import.zeek"));
  import_->add_subcommand("zeek-json",
                          "imports Zeek JSON logs from STDIN or file",
                          opts("?tenzir.import.zeek-json"));
  import_->add_subcommand(
    "csv", "imports CSV logs from STDIN or file",
    opts("?tenzir.import.csv")
      .add<std::string>("separator", "the single-character separator (default: "
                                     "',')"));
  import_->add_subcommand(
    "json", "imports JSON with schema",
    opts("?tenzir.import.json")
      .add<std::string>("selector", "read the event type from the given field "
                                    "(specify as '<field>[:<prefix>]')"));
  import_->add_subcommand("suricata", "imports suricata EVE JSON",
                          opts("?tenzir.import.suricata"));
  import_->add_subcommand("syslog", "imports syslog messages",
                          opts("?tenzir.import.syslog"));
  import_->add_subcommand("arrow", "import from an Arrow IPC stream",
                          opts("?tenzir.import.arrow"));
  import_->add_subcommand(
    "test", "imports random data for testing or benchmarking",
    opts("?tenzir.import.test").add<int64_t>("seed", "the PRNG seed"));
  return import_;
}

std::pair<std::unique_ptr<command>, command::factory>
make_application(std::string_view path) {
  // We're only interested in the application name, not in its path. For
  // example, argv[0] might contain "./build/release/bin/tenzir" and we are only
  // interested in "tenzir".
  const auto last_slash = path.find_last_of('/');
  const auto name
    = last_slash == std::string_view::npos ? path : path.substr(last_slash + 1);
  if (name == "tenzir-node") {
    static constexpr auto banner1 = R"_( _____ _____ _   _ ________ ____  )_";
    static constexpr auto banner2 = R"_(|_   _| ____| \ | |__  /_ _|  _ \ )_";
    static constexpr auto banner3 = R"_(  | | |  _| |  \| | / / | || |_) |)_";
    static constexpr auto banner4 = R"_(  | | | |___| |\  |/ /_ | ||  _ < )_";
    static constexpr auto banner5 = R"_(  |_| |_____|_| \_/____|___|_| \_\)_";
    auto notice = fmt::format("Visit ");
    fmt::format_to(std::back_inserter(notice), fmt::emphasis::underline,
                   "https://app.tenzir.com");
    fmt::format_to(std::back_inserter(notice), " to get started.");
    const auto width
      = fmt::formatted_size("Visit https://app.tenzir.com to get started.");
    fmt::print(stderr, fmt::fg(fmt::terminal_color::blue), "{1:^{0}}", width,
               banner1);
    fmt::print(stderr, "\n");
    fmt::print(stderr, fmt::fg(fmt::terminal_color::blue), "{1:^{0}}", width,
               banner2);
    fmt::print(stderr, "\n");
    fmt::print(stderr, fmt::fg(fmt::terminal_color::blue), "{1:^{0}}", width,
               banner3);
    fmt::print(stderr, "\n");
    fmt::print(stderr, fmt::fg(fmt::terminal_color::blue), "{1:^{0}}", width,
               banner4);
    fmt::print(stderr, "\n");
    fmt::print(stderr, fmt::fg(fmt::terminal_color::blue), "{1:^{0}}", width,
               banner5);
    fmt::print(stderr, "\n\n");
    fmt::print(stderr, "{1:^{0}}", width, version::version);
    fmt::print(stderr, "\n");
    fmt::print(stderr, "{1:^{0}}", width, notice);
    fmt::print(stderr, "\n\n");
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
    TENZIR_ASSERT(exec);
    auto [cmd, cmd_factory] = exec->make_command();
    TENZIR_ASSERT(cmd);
    add_root_opts(*cmd);
    TENZIR_ASSERT(cmd_factory.contains("exec"));
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
  if (err.category() == caf::type_id_v<tenzir::ec>) {
    auto x = static_cast<tenzir::ec>(err.code());
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
          TENZIR_ASSERT(
            !"User visible error contexts must consist of strings!");
        }
        break;
      }
    }
  }
}

command::opts_builder opts(std::string_view category) {
  return command::opts(category);
}

} // namespace tenzir
