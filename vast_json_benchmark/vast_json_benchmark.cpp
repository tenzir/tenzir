/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/atoms.hpp"
#include "vast/concept/convertible/to.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/config.hpp"
#include "vast/data.hpp"
#include "vast/detail/process.hpp"
#include "vast/detail/stable_set.hpp"
#include "vast/detail/system.hpp"
#include "vast/directory.hpp"
#include "vast/error.hpp"
#include "vast/event_types.hpp"
#include "vast/format/json.hpp"
#include "vast/format/json/default_selector.hpp"
#include "vast/format/json/suricata_selector.hpp"
#include "vast/format/json/zeek_selector.hpp"
#include "vast/format/reader.hpp"
#include "vast/format/simdjson.hpp"
#include "vast/logger.hpp"
#include "vast/path.hpp"
#include "vast/plugin.hpp"
#include "vast/schema.hpp"
#include "vast/system/application.hpp"
#include "vast/system/default_configuration.hpp"
#include "vast/system/import_command.hpp"

#include <caf/actor_system.hpp>
#include <caf/io/middleman.hpp>
#include <caf/settings.hpp>
#include <caf/timestamp.hpp>

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#if VAST_ENABLE_OPENSSL
#  include <caf/openssl/manager.hpp>
#endif

using namespace std::string_literals;
using namespace vast;
using namespace vast::system;

namespace vast::detail {

auto make_root_command(std::string_view path) {
  // We're only interested in the application name, not in its path. For
  // example, argv[0] might contain "./build/release/bin/vast" and we are only
  // interested in "vast".
  path.remove_prefix(std::min(path.find_last_of('/') + 1, path.size()));
  // For documentation, we use the complete man-page formatted as Markdown
  auto binary = detail::objectpath();
  auto schema_desc
    = "list of directories to look for schema files ([/etc/vast/schema"s;
  if (binary) {
    auto relative_schema_dir
      = binary->parent().parent() / "share" / "vast" / "schema";
    schema_desc += ", " + relative_schema_dir.str();
  }
  schema_desc += "])";
  auto ob
    = opts("?vast")
        .add<std::string>("config", "path to a configuration file")
        .add<caf::atom_value>("verbosity", "output verbosity level on the "
                                           "console")
        .add<std::vector<std::string>>("schema-dirs", schema_desc.c_str())
        .add<std::vector<std::string>>("schema-paths", "deprecated; use "
                                                       "schema-dirs instead")
        .add<std::string>("db-directory,d", "directory for persistent state")
        .add<std::string>("log-file", "log filename")
        .add<std::string>("client-log-file", "client log file (default: "
                                             "disabled)")
        .add<std::string>("endpoint,e", "node endpoint")
        .add<std::string>("node-id,i", "the unique ID of this node")
        .add<bool>("node,N", "spawn a node instead of connecting to one")
        .add<bool>("enable-metrics", "keep track of performance metrics")
        .add<bool>("no-default-schema", "don't load the default schema "
                                        "definitions")
        .add<std::vector<std::string>>("plugin-dirs", "additional directories "
                                                      "to load plugins from")
        .add<std::vector<std::string>>("plugins", "plugins to load at startup")
        .add<std::string>("aging-frequency", "interval between two aging "
                                             "cycles")
        .add<std::string>("aging-query", "query for aging out obsolete data")
        .add<std::string>("shutdown-grace-period",
                          "time to wait until component shutdown "
                          "finishes cleanly before inducing a hard kill");
  return std::make_unique<command>(path, "", documentation::vast,
                                   std::move(ob));
}

auto make_import_command() {
  auto import_ = std::make_unique<command>(
    "import", "imports data from STDIN or file", documentation::vast_import,
    opts("?vast.import")
      .add<std::string>("batch-encoding", "encoding type of table slices "
                                          "(arrow or msgpack)")
      .add<size_t>("batch-size", "upper bound for the size of a table slice")
      .add<std::string>("batch-timeout", "timeout after which batched "
                                         "table slices are forwarded")
      .add<std::string>("read-timeout", "timeout for waiting for incoming data")
      .add<bool>("blocking,b", "block until the IMPORTER forwarded all data")
      .add<size_t>("max-events,n", "the maximum number of events to import"));
  import_->add_subcommand("zeek-json",
                          "imports Zeek JSON logs from STDIN or file",
                          documentation::vast_import_zeek,
                          source_opts_json("?vast.import.zeek-json"));
  import_->add_subcommand("json", "imports JSON with schema",
                          documentation::vast_import_json,
                          source_opts_json("?vast.import.json"));
  import_->add_subcommand("suricata", "imports suricata eve json",
                          documentation::vast_import_suricata,
                          source_opts_json("?vast.import.suricata"));
  return import_;
}

struct perfect_sink_state {
  std::vector<table_slice> slices;
  inline static constexpr const char* name = "perfect-sink";
};

using perfect_sink_type = caf::stateful_actor<perfect_sink_state>;

caf::behavior perfect_sink(perfect_sink_type* self) {
  return {[=](caf::stream<table_slice> in, const std::string&) {
    return self->make_sink(
      in,
      [](caf::unit_t&) {
        // nop
      },
      [=](caf::unit_t&, table_slice) {},
      [=](caf::unit_t&, const caf::error&) {});
  }};
}

template <class Reader, class Defaults>
caf::message
local_import_command(const invocation& inv, caf::actor_system& sys) {
  VAST_TRACE(inv.full_name, VAST_ARG("options", inv.options), VAST_ARG(sys));
  auto self = caf::scoped_actor{sys};
  // Get VAST node.
  auto node_opt
    = spawn_or_connect_to_node(self, inv.options, content(sys.config()));
  if (auto err = caf::get_if<caf::error>(&node_opt))
    return caf::make_message(std::move(*err));
  auto& node = caf::holds_alternative<caf::actor>(node_opt)
                 ? caf::get<caf::actor>(node_opt)
                 : caf::get<scope_linked_actor>(node_opt).get();
  VAST_DEBUG(inv.full_name, "got node");
  // Start signal monitor.
  std::thread sig_mon_thread;
  auto guard = system::signal_monitor::run_guarded(
    sig_mon_thread, sys, defaults::system::signal_monitoring_interval, self);
  auto importer = self->spawn(perfect_sink);
  // Start the source.
  auto src_result = make_source<Reader, Defaults>(
    self, sys, inv, vast::system::accountant_actor{},
    vast::system::type_registry_actor{}, importer);
  if (!src_result)
    return caf::make_message(std::move(src_result.error()));
  auto src = std::move(src_result->src);
  auto name = std::move(src_result->name);
  bool stop = false;
  caf::error err;
  self->request(node, caf::infinite, atom::put_v, src, "source")
    .receive([&](atom::ok) { VAST_DEBUG(name, "registered source at node"); },
             [&](caf::error error) { err = std::move(error); });
  if (err) {
    self->send_exit(src, caf::exit_reason::user_shutdown);
    return caf::make_message(std::move(err));
  }
  self->monitor(src);
  self->monitor(importer);
  self
    ->do_receive(
      // C++20: remove explicit 'importer' parameter passing.
      [&, importer = importer](const caf::down_msg& msg) {
        if (msg.source == importer) {
          VAST_DEBUG(name, "received DOWN from node importer");
          self->send_exit(src, caf::exit_reason::user_shutdown);
          err = ec::remote_node_down;
          stop = true;
        } else if (msg.source == src) {
          VAST_DEBUG(name, "received DOWN from source");
          if (caf::get_or(inv.options, "vast.import.blocking", false))
            self->send(importer, atom::subscribe_v, atom::flush::value,
                       caf::actor_cast<flush_listener_actor>(self));
          else
            stop = true;
        } else {
          VAST_DEBUG(name, "received unexpected DOWN from", msg.source);
          VAST_ASSERT(!"unexpected DOWN message");
        }
      },
      [&](atom::flush) {
        VAST_DEBUG(name, "received flush from IMPORTER");
        stop = true;
      },
      [&](atom::signal, int signal) {
        VAST_DEBUG(name, "received signal", ::strsignal(signal));
        if (signal == SIGINT || signal == SIGTERM)
          self->send_exit(src, caf::exit_reason::user_shutdown);
      })
    .until(stop);
  if (err)
    return caf::make_message(std::move(err));
  return caf::none;
}

template <class Reader, class SimdjsonReader, class Defaults>
caf::message
local_import_command_json(const invocation& inv, caf::actor_system& sys) {
  const auto use_simdjson = caf::get_or(
    inv.options, Defaults::category + std::string{".simdjson"}, false);

  if (use_simdjson)
    return local_import_command<SimdjsonReader, Defaults>(inv, sys);
  return local_import_command<Reader, Defaults>(inv, sys);
}

template <template <class S, class B> class Reader,
          template <class S, class B> class SimdjsonReader, typename Selector,
          class Defaults>
caf::message local_import_command_json_with_benchmark(const invocation& inv,
                                                      caf::actor_system& sys) {
  const auto bench_value = caf::get_or(
    inv.options, Defaults::category + std::string{".benchmark"}, false);

  if (bench_value) {
    using json_reader_t = Reader<Selector, system::timer_benchmark_mixin<4>>;
    using simdjson_reader_t
      = SimdjsonReader<Selector, system::timer_benchmark_mixin<4>>;
    static_assert(system::has_benchmark_metrics<json_reader_t>{});
    static_assert(system::has_benchmark_metrics<simdjson_reader_t>{});

    return local_import_command_json<json_reader_t, simdjson_reader_t, Defaults>(
      inv, sys);
  }

  return local_import_command_json<
    Reader<Selector, system::noop_benchmark_mixin>,
    SimdjsonReader<Selector, system::noop_benchmark_mixin>, Defaults>(inv, sys);
}

auto make_command_factory() {
  // When updating this list, remember to update its counterpart in node.cpp as
  // well iff necessary
  // clang-format off
  return command::factory{
    {"import json", local_import_command_json_with_benchmark<
      format::json::reader,
      format::simdjson::reader,
      format::json::default_selector,
      defaults::import::json>},
    {"import suricata", local_import_command_json_with_benchmark<
      format::json::reader,
      format::simdjson::reader,
      format::json::suricata_selector,
      defaults::import::suricata>},
    {"import zeek-json", local_import_command_json_with_benchmark<
      format::json::reader,
      format::simdjson::reader,
      format::json::zeek_selector,
      defaults::import::zeek_json>},
  };
  // clang-format on
}

std::pair<std::unique_ptr<command>, command::factory>
make_application(std::string_view path) {
  auto root = make_root_command(path);
  root->add_subcommand(make_import_command());
  return {std::move(root), make_command_factory()};
}

// TODO: find a better location for this function.
stable_set<path> get_plugin_dirs(const caf::actor_system_config& cfg) {
  stable_set<path> result;
#if !VAST_ENABLE_RELOCATABLE_INSTALLATIONS
  result.insert(path{VAST_LIBDIR} / "vast" / "plugins");
#endif
  // FIXME: we technically should not use "lib" relative to the parent, because
  // it may be lib64 or something else. CMAKE_INSTALL_LIBDIR is probably the
  // best choice.
  if (auto binary = objectpath(nullptr))
    result.insert(binary->parent().parent() / "lib" / "vast" / "plugins");
  else
    VAST_ERROR_ANON(__func__, "failed to get program path");
  if (const char* home = std::getenv("HOME"))
    result.insert(path{home} / ".local" / "lib" / "vast" / "plugins");
  if (auto dirs = caf::get_if<std::vector<std::string>>(
        &cfg, "vast.plugin-dirs"))
    result.insert(dirs->begin(), dirs->end());
  return result;
}

} // namespace vast::detail

int main(int argc, char** argv) {
  // Set up our configuration, e.g., load of YAML config file(s).
  default_configuration cfg;
  if (auto err = cfg.parse(argc, argv)) {
    std::cerr << "failed to parse configuration: " << to_string(err)
              << std::endl;
    return EXIT_FAILURE;
  }
  // Application setup.
  auto [root, root_factory] = detail::make_application(argv[0]);
  if (!root)
    return EXIT_FAILURE;
  // Load plugins.
  auto& plugins = plugins::get();
  auto plugin_dirs = detail::get_plugin_dirs(cfg);
  // We need the below variables because we cannot log here, they are used for
  // deferred log statements essentially.
  auto plugin_load_errors = std::vector<caf::error>{};
  auto loaded_plugin_paths = std::vector<path>{};
  auto plugin_files
    = caf::get_or(cfg, "vast.plugins", std::vector<std::string>{});
  auto load_plugin = [&](path file) {
#if VAST_MACOS
    if (file.extension() == "")
      file = file.str() + ".dylib";
#else
    if (file.extension() == "")
      file = file.str() + ".so";
#endif
    if (!exists(file))
      return false;
    if (auto plugin = plugin_ptr::make(file.str().c_str())) {
      VAST_ASSERT(*plugin);
      auto has_same_name = [name = (*plugin)->name()](const auto& other) {
        return !std::strcmp(name, other->name());
      };
      if (std::none_of(plugins.begin(), plugins.end(), has_same_name)) {
        loaded_plugin_paths.push_back(std::move(file));
        plugins.push_back(std::move(*plugin));
        return true;
      } else {
        std::cerr << "failed to load plugin " << file.str()
                  << " because another plugin already uses the name "
                  << (*plugin)->name() << std::endl;
        std::exit(EXIT_FAILURE);
      }
    } else {
      plugin_load_errors.push_back(std::move(plugin.error()));
    }
    return false;
  };
  for (const auto& plugin_file : plugin_files) {
    // First, check if the plugin file is specified as an absolute path.
    if (load_plugin(plugin_file))
      continue;
    // Second, check if the plugin file is specified relative to the specified
    // plugin directories.
    auto plugin_found = false;
    for (const auto& dir : plugin_dirs) {
      auto file = dir / plugin_file;
      if (load_plugin(file)) {
        plugin_found = true;
        break;
      }
    }
    if (!plugin_found) {
      std::cerr << "failed to find plugin: " << plugin_file << std::endl;
      return EXIT_FAILURE;
    }
  }
  // Add additional commands from plugins.
  for (auto& plugin : plugins) {
    if (auto cp = plugin.as<command_plugin>()) {
      auto&& [cmd, cmd_factory] = cp->make_command();
      root->add_subcommand(std::move(cmd));
      root_factory.insert(std::make_move_iterator(cmd_factory.begin()),
                          std::make_move_iterator(cmd_factory.end()));
    }
  }
  // Parse CLI.
  auto invocation
    = parse(*root, cfg.command_line.begin(), cfg.command_line.end());
  if (!invocation) {
    if (invocation.error()) {
      render_error(*root, invocation.error(), std::cerr);
      return EXIT_FAILURE;
    }
    // Printing help/documentation returns a no_error, and we want to indicate
    // success when printing the help/documentation texts.
    return EXIT_SUCCESS;
  }
  // Initialize actor system (and thereby CAF's logger).
  if (!init_config(cfg, *invocation, std::cerr))
    return EXIT_FAILURE;
  caf::actor_system sys{cfg};
  fixup_logger(cfg);
  // Print the configuration file(s) that were loaded.
  if (!cfg.config_file_path.empty())
    cfg.config_files.emplace_back(std::move(cfg.config_file_path));
  for (auto& file : cfg.config_files)
    VAST_INFO_ANON("loaded configuration file:", file);
  // Print the plugins that were loaded, and errors that occured during loading.
  for (const auto& file : loaded_plugin_paths)
    VAST_VERBOSE_ANON("loaded plugin:", file);
  for (const auto& err : plugin_load_errors)
    VAST_ERROR_ANON("failed to load plugin:", render(err));
  // Initialize successfully loaded plugins.
  for (auto& plugin : plugins) {
    auto key = "plugins."s + plugin->name();
    if (auto opts = caf::get_if<caf::settings>(&cfg, key)) {
      if (auto config = to<data>(*opts)) {
        VAST_DEBUG_ANON("initializing plugin with options:", *config);
        plugin->initialize(std::move(*config));
      } else {
        VAST_ERROR_ANON("invalid plugin configuration for plugin",
                        plugin->name());
        plugin->initialize(data{});
      }
    } else {
      VAST_DEBUG_ANON("no configuration found for plugin", plugin->name());
      plugin->initialize(data{});
    }
  }
  // Load event types.
  if (auto schema = load_schema(cfg)) {
    event_types::init(*std::move(schema));
  } else {
    VAST_ERROR_ANON("failed to read schema dirs:", render(schema.error()));
    return EXIT_FAILURE;
  }
  // Dispatch to root command.
  auto result = run(*invocation, sys, root_factory);
  if (!result) {
    render_error(*root, result.error(), std::cerr);
    return EXIT_FAILURE;
  }
  if (result->match_elements<caf::error>()) {
    auto& err = result->get_as<caf::error>(0);
    if (err) {
      vast::system::render_error(*root, err, std::cerr);
      return EXIT_FAILURE;
    }
  }
  return EXIT_SUCCESS;
}
