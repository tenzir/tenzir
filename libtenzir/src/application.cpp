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
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/process.hpp"
#include "tenzir/error.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/start_command.hpp"

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
    "?tenzir", "no-location-overrides",
    "forbid unsafe location overrides for pipelines with the "
    "'local' and 'remote' keywords, e.g., remotely reading from "
    "a file");
  cmd.options.add<std::string>("?tenzir", "console-verbosity",
                               "output verbosity level on the "
                               "console");
  cmd.options.add<std::string>("?tenzir", "console-format",
                               "format string for logging to the "
                               "console");
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

auto make_root_command(std::string_view name) {
  using namespace std::string_literals;
  auto ob = opts("?tenzir");
  auto root = std::make_unique<command>(name, "", std::move(ob));
  add_root_opts(*root);
  return root;
}

} // namespace

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
  auto root_factory = command::factory{};
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
  const auto pretty_diagnostics = true;
  os << render(err, pretty_diagnostics) << '\n';
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
