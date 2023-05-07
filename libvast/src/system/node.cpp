//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/node.hpp"

#include "vast/fwd.hpp"

#include "vast/atoms.hpp"
#include "vast/concept/convertible/to.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/endpoint.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/json.hpp"
#include "vast/config.hpp"
#include "vast/data.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/internal_http_response.hpp"
#include "vast/detail/process.hpp"
#include "vast/detail/settings.hpp"
#include "vast/execution_node.hpp"
#include "vast/format/csv.hpp"
#include "vast/format/json.hpp"
#include "vast/format/json/default_selector.hpp"
#include "vast/format/json/suricata_selector.hpp"
#include "vast/format/json/zeek_selector.hpp"
#include "vast/format/syslog.hpp"
#include "vast/format/test.hpp"
#include "vast/format/zeek.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/system/accountant.hpp"
#include "vast/system/accountant_config.hpp"
#include "vast/system/configuration.hpp"
#include "vast/system/node.hpp"
#include "vast/system/posix_filesystem.hpp"
#include "vast/system/shutdown.hpp"
#include "vast/system/spawn_arguments.hpp"
#include "vast/system/spawn_catalog.hpp"
#include "vast/system/spawn_counter.hpp"
#include "vast/system/spawn_disk_monitor.hpp"
#include "vast/system/spawn_eraser.hpp"
#include "vast/system/spawn_exporter.hpp"
#include "vast/system/spawn_importer.hpp"
#include "vast/system/spawn_index.hpp"
#include "vast/system/spawn_node.hpp"
#include "vast/system/spawn_sink.hpp"
#include "vast/system/spawn_source.hpp"
#include "vast/system/status.hpp"
#include "vast/system/terminate.hpp"
#include "vast/system/version_command.hpp"
#include "vast/table_slice.hpp"
#include "vast/taxonomies.hpp"

#include <caf/function_view.hpp>
#include <caf/io/middleman.hpp>
#include <caf/settings.hpp>

#include <chrono>
#include <csignal>
#include <fstream>
#include <sstream>

namespace vast::system {

namespace {

// This is a side-channel to communicate the self pointer into the spawn- and
// send-command functions, whose interfaces are constrained by the command
// factory.
thread_local node_actor::stateful_pointer<node_state> this_node = nullptr;

// Convenience function for wrapping an error into a CAF message.
auto make_error_msg(ec code, std::string msg) {
  return caf::make_message(caf::make_error(code, std::move(msg)));
}

/// A list of components that are essential for importing and exporting data
/// from the node.
std::set<const char*> core_components = {
  "accountant", "catalog", "filesystem", "importer", "index",
};

bool is_core_component(std::string_view type) {
  auto pred = [&](const char* x) {
    return x == type;
  };
  return std::any_of(std::begin(core_components), std::end(core_components),
                     pred);
}

/// Helper function to determine whether a component can be spawned at most
/// once.
bool is_singleton(std::string_view type) {
  // TODO: All of these actor interfaces are strongly typed. The value of `type`
  // is received via the actor interface of the NODE sometimes, which means that
  // we cannot just pass arbitrary actors to it. Atoms aren't really an option
  // either, because `caf::atom_value` was removed with CAF 0.18. We can,
  // however, abuse the fact that every typed actor has a type ID assigned, and
  // change the node to work with type IDs over actor names everywhere. This
  // refactoring will be much easier once the NODE itself is a typed actor, so
  // let's hold off until then.
  const char* singletons[]
    = {"accountant", "disk-monitor", "eraser", "filesystem",
       "importer",   "index",        "catalog"};
  auto pred = [&](const char* x) {
    return x == type;
  };
  return std::any_of(std::begin(singletons), std::end(singletons), pred);
}

// Sends an atom to a registered actor. Blocks until the actor responds.
caf::message send_command(const invocation& inv, caf::actor_system&) {
  auto first = inv.arguments.begin();
  auto last = inv.arguments.end();
  // Expect exactly two arguments.
  if (std::distance(first, last) != 2)
    return make_error_msg(ec::syntax_error, "expected two arguments: receiver "
                                            "and message atom");
  // Get destination actor from the registry.
  auto dst = this_node->state.registry.find_by_label(*first);
  if (dst == nullptr)
    return make_error_msg(ec::syntax_error,
                          "registry contains no actor named " + *first);
  // Dispatch to destination.
  auto f = caf::make_function_view(caf::actor_cast<caf::actor>(dst));
  auto send = [&](auto atom_value) {
    if (auto res = f(atom_value))
      return std::move(*res);
    else
      return caf::make_message(std::move(res.error()));
  };
  if (*(first + 1) == "run")
    return send(atom::run_v);
  return make_error_msg(ec::unimplemented,
                        "trying to send unsupported atom: " + *(first + 1));
}

auto find_endpoint_plugin(const http_request_description& desc)
  -> const rest_endpoint_plugin* {
  for (auto const& plugin : plugins::get()) {
    auto const* rest_plugin = plugin.as<rest_endpoint_plugin>();
    if (!rest_plugin)
      continue;
    for (const auto& endpoint : rest_plugin->rest_endpoints())
      if (endpoint.canonical_path() == desc.canonical_path)
        return rest_plugin;
  }
  return nullptr;
}

void collect_component_status(node_actor::stateful_pointer<node_state> self,
                              status_verbosity v,
                              const std::vector<std::string>& components) {
  const auto has_component = [&](std::string_view name) {
    return components.empty()
           || std::any_of(components.begin(), components.end(),
                          [&](const auto& component) {
                            return component == name;
                          });
  };
  struct extra_state {
    size_t memory_usage = 0;
    static void deliver(caf::typed_response_promise<std::string>&& promise,
                        record&& content) {
      // We strip and sort the output for a cleaner presentation of the data.
      if (auto json = to_json(sort(strip(content))))
        promise.deliver(std::move(*json));
    }
    // In-place sort each level of the tree.
    static record& sort(record&& r) {
      std::sort(r.begin(), r.end());
      for (auto& [_, value] : r)
        if (auto* nested = caf::get_if<record>(&value))
          sort(std::move(*nested));
      return r;
    };
  };
  auto rs = make_status_request_state<extra_state, std::string>(self);
  // Pre-fill the version information.
  if (has_component("version")) {
    auto version = retrieve_versions();
    rs->content["version"] = version;
  }
  // Pre-fill our result with system stats.
  if (has_component("system")) {
    auto system = record{};
    if (v >= status_verbosity::info) {
      system["in-memory-table-slices"] = uint64_t{table_slice::instances()};
      system["database-path"] = self->state.dir.string();
      merge(detail::get_status(), system, policy::merge_lists::no);
    }
    if (v >= status_verbosity::detailed) {
      auto config_files = list{};
      std::transform(system::loaded_config_files().begin(),
                     system::loaded_config_files().end(),
                     std::back_inserter(config_files),
                     [](const std::filesystem::path& x) {
                       return x.string();
                     });
      std::transform(plugins::loaded_config_files().begin(),
                     plugins::loaded_config_files().end(),
                     std::back_inserter(config_files),
                     [](const std::filesystem::path& x) {
                       return x.string();
                     });
      system["config-files"] = std::move(config_files);
    }
    if (v >= status_verbosity::debug) {
      auto& sys = self->system();
      system["running-actors"] = uint64_t{sys.registry().running()};
      system["detached-actors"] = uint64_t{sys.detached_actors()};
      system["worker-threads"] = uint64_t{sys.scheduler().num_workers()};
    }
    rs->content["system"] = std::move(system);
  }
  const auto timeout = defaults::system::status_request_timeout;
  // Send out requests and collects answers.
  for (const auto& [label, component] : self->state.registry.components()) {
    // Requests to busy remote sources and sinks can easily delay the combined
    // response because the status requests don't get scheduled soon enough.
    // NOTE: We must use `caf::actor::node` rather than
    // `caf::actor_system::node`, because the actor system for remote actors is
    // proxied, so using `component.actor.home_system().node()` will result in
    // a different `node_id` from the one we actually want to compare.
    if (component.actor.node() != self->node())
      continue;
    if (!has_component(component.type))
      continue;
    collect_status(rs, timeout, v, component.actor, rs->content, label);
  }
}

/// Registers (and monitors) a component through the node.
caf::error
register_component(node_actor::stateful_pointer<node_state> self,
                   const caf::actor& component, std::string_view type,
                   std::string_view label = {}) {
  if (!self->state.registry.add(component, std::string{type},
                                std::string{label})) {
    auto msg // separate variable for clang-format only
      = fmt::format("{} failed to add component to registry: {}", *self,
                    label.empty() ? type : label);
    return caf::make_error(ec::unspecified, std::move(msg));
  }
  self->monitor(component);
  return caf::none;
}

/// Deregisters (and demonitors) a component through the node.
caf::expected<caf::actor>
deregister_component(node_actor::stateful_pointer<node_state> self,
                     std::string_view label) {
  auto component = self->state.registry.remove(label);
  if (!component) {
    auto msg // separate variable for clang-format only
      = fmt::format("{} failed to deregister non-existent component: {}", *self,
                    label);
    return caf::make_error(ec::unspecified, std::move(msg));
  }
  self->demonitor(component->actor);
  return component->actor;
}

/// Spawns the accountant actor.
accountant_actor
spawn_accountant(node_actor::stateful_pointer<node_state> self) {
  if (!caf::get_or(content(self->system().config()), "vast.enable-metrics",
                   false))
    return {};
  // It doesn't make much sense to run the accountant for one-shot commands
  // with a local database using `--node`, so this prevents spawning it.
  if (caf::get_or(content(self->system().config()), "vast.node", false))
    return {};
  const auto metrics_opts = caf::get_or(content(self->system().config()),
                                        "vast.metrics", caf::settings{});
  auto cfg = to_accountant_config(metrics_opts);
  if (!cfg) {
    VAST_ERROR("{} failed to parse metrics configuration: {}", *self,
               cfg.error());
    return {};
  }
  auto accountant = self->spawn<caf::detached>(
    vast::system::accountant, std::move(*cfg), self->state.dir);
  auto err = register_component(self, caf::actor_cast<caf::actor>(accountant),
                                "accountant");
  // Registration cannot fail; empty registry.
  VAST_ASSERT(err == caf::none);
  return accountant;
}

} // namespace

caf::message status_command(const invocation& inv, caf::actor_system&) {
  if (caf::get_or(inv.options, "vast.node", false)) {
    return caf::make_message(caf::make_error( //
      ec::invalid_configuration,
      "unable to execute status command when spawning a "
      "node locally instead of connecting to one; please "
      "unset the option vast.node"));
  }
  auto self = this_node;
  auto verbosity = status_verbosity::info;
  if (caf::get_or(inv.options, "vast.status.detailed", false))
    verbosity = status_verbosity::detailed;
  if (caf::get_or(inv.options, "vast.status.debug", false))
    verbosity = status_verbosity::debug;
  if (inv.arguments.empty())
    VAST_VERBOSE("{} collects status for all components", *self);
  else
    VAST_VERBOSE("{} collects status for components {}", *self, inv.arguments);
  collect_component_status(self, verbosity, inv.arguments);
  return {};
}

caf::expected<caf::actor>
spawn_accountant(node_actor::stateful_pointer<node_state> self,
                 spawn_arguments& args) {
  const auto& options = args.inv.options;
  auto metrics_opts = caf::get_or(options, "vast.metrics", caf::settings{});
  auto cfg = to_accountant_config(metrics_opts);
  if (!cfg)
    return cfg.error();
  return caf::actor_cast<caf::actor>(
    self->spawn(accountant, std::move(*cfg), self->state.dir));
}

caf::expected<caf::actor>
spawn_component(node_actor::stateful_pointer<node_state> self,
                const invocation& inv, spawn_arguments& args) {
  VAST_TRACE_SCOPE("{} {}", VAST_ARG(inv), VAST_ARG(args));
  auto i = node_state::component_factory.find(inv.full_name);
  if (i == node_state::component_factory.end())
    return caf::make_error(ec::unspecified, "invalid spawn component");
  return i->second(self, args);
}

caf::message kill_command(const invocation& inv, caf::actor_system&) {
  auto self = this_node;
  auto first = inv.arguments.begin();
  auto last = inv.arguments.end();
  if (std::distance(first, last) != 1)
    return make_error_msg(ec::syntax_error, "expected exactly one component "
                                            "argument");
  auto rp = self->make_response_promise();
  auto& label = *first;
  auto component = deregister_component(self, label);
  if (!component) {
    rp.deliver(caf::make_error(ec::unspecified,
                               fmt::format("no such component: {}", label)));
  } else {
    terminate<policy::parallel>(self, std::move(*component))
      .then(
        [=](atom::done) mutable {
          VAST_DEBUG("{} terminated component {}", *self, label);
          rp.deliver(atom::ok_v);
        },
        [=](const caf::error& err) mutable {
          VAST_WARN("{} failed to terminate component {}: {}", *self, label,
                    err);
          rp.deliver(err);
        });
  }
  return {};
}

/// Lifts a factory function that accepts `local_actor*` as first argument
/// to a function accpeting `node_actor::stateful_pointer<node_state>` instead.
template <caf::expected<caf::actor> (*Fun)(caf::local_actor*, spawn_arguments&)>
node_state::component_factory_fun lift_component_factory() {
  return
    [](node_actor::stateful_pointer<node_state> self, spawn_arguments& args) {
      // Delegate to lifted function.
      return Fun(self, args);
    };
}

template <caf::expected<caf::actor> (*Fun)(
  node_actor::stateful_pointer<node_state>, spawn_arguments&)>
node_state::component_factory_fun lift_component_factory() {
  return Fun;
}

auto make_component_factory() {
  auto result = node_state::named_component_factory{
    {"spawn accountant", lift_component_factory<spawn_accountant>()},
    {"spawn counter", lift_component_factory<spawn_counter>()},
    {"spawn disk-monitor", lift_component_factory<spawn_disk_monitor>()},
    {"spawn eraser", lift_component_factory<spawn_eraser>()},
    {"spawn exporter", lift_component_factory<spawn_exporter>()},
    {"spawn importer", lift_component_factory<spawn_importer>()},
    {"spawn catalog", lift_component_factory<spawn_catalog>()},
    {"spawn index", lift_component_factory<spawn_index>()},
    {"spawn source", lift_component_factory<spawn_source>()},
    {"spawn source arrow", lift_component_factory<spawn_source>()},
    {"spawn source csv", lift_component_factory<spawn_source>()},
    {"spawn source json", lift_component_factory<spawn_source>()},
    {"spawn source suricata", lift_component_factory<spawn_source>()},
    {"spawn source syslog", lift_component_factory<spawn_source>()},
    {"spawn source test", lift_component_factory<spawn_source>()},
    {"spawn source zeek", lift_component_factory<spawn_source>()},
    {"spawn source zeek-json", lift_component_factory<spawn_source>()},
    {"spawn sink zeek", lift_component_factory<spawn_sink>()},
    {"spawn sink csv", lift_component_factory<spawn_sink>()},
    {"spawn sink ascii", lift_component_factory<spawn_sink>()},
    {"spawn sink json", lift_component_factory<spawn_sink>()},
  };
  for (const auto& plugin : plugins::get()) {
    if (const auto* reader = plugin.as<reader_plugin>()) {
      auto command = fmt::format("spawn source {}", reader->reader_format());
      result.emplace(command, lift_component_factory<spawn_source>());
    }
    if (const auto* writer = plugin.as<writer_plugin>()) {
      auto command = fmt::format("spawn sink {}", writer->writer_format());
      result.emplace(command, lift_component_factory<spawn_sink>());
    }
  }
  return result;
}

auto make_command_factory() {
  // When updating this list, remember to update its counterpart in
  // application.cpp as well iff necessary
  auto result = command::factory{
    {"kill", kill_command},
    {"send", send_command},
    {"spawn accountant", node_state::spawn_command},
    {"spawn counter", node_state::spawn_command},
    {"spawn disk-monitor", node_state::spawn_command},
    {"spawn eraser", node_state::spawn_command},
    {"spawn exporter", node_state::spawn_command},
    {"spawn importer", node_state::spawn_command},
    {"spawn catalog", node_state::spawn_command},
    {"spawn index", node_state::spawn_command},
    {"spawn sink ascii", node_state::spawn_command},
    {"spawn sink csv", node_state::spawn_command},
    {"spawn sink json", node_state::spawn_command},
    {"spawn sink zeek", node_state::spawn_command},
    {"spawn source arrow", node_state::spawn_command},
    {"spawn source csv", node_state::spawn_command},
    {"spawn source json", node_state::spawn_command},
    {"spawn source suricata", node_state::spawn_command},
    {"spawn source syslog", node_state::spawn_command},
    {"spawn source test", node_state::spawn_command},
    {"spawn source zeek", node_state::spawn_command},
    {"spawn source zeek-json", node_state::spawn_command},
    {"status", status_command},
  };
  for (const auto& plugin : plugins::get()) {
    if (const auto* reader = plugin.as<reader_plugin>()) {
      auto command = fmt::format("spawn source {}", reader->reader_format());
      result.emplace(command, node_state::spawn_command);
    }
    if (const auto* writer = plugin.as<writer_plugin>()) {
      auto command = fmt::format("spawn sink {}", writer->writer_format());
      result.emplace(command, node_state::spawn_command);
    }
  }
  return result;
}

std::string generate_label(node_actor::stateful_pointer<node_state> self,
                           std::string_view component) {
  // C++20: remove the indirection through std::string.
  auto n = self->state.label_counters[std::string{component}]++;
  return std::string{component} + '-' + std::to_string(n);
}

caf::message
node_state::spawn_command(const invocation& inv,
                          [[maybe_unused]] caf::actor_system& sys) {
  VAST_TRACE_SCOPE("{}", inv);
  using std::begin;
  using std::end;
  auto* self = this_node;
  if (self->state.tearing_down)
    return caf::make_message(caf::make_error( //
      ec::no_error, "can't spawn a component while tearing down"));
  // We configured the command to have the name of the component.
  auto inv_name_split = detail::split(inv.full_name, " ");
  VAST_ASSERT(inv_name_split.size() > 1);
  std::string comp_type{inv_name_split[1]};
  // Auto-generate label if none given.
  std::string label;
  if (auto label_ptr = caf::get_if<std::string>(&inv.options, "vast.spawn."
                                                              "label")) {
    label = *label_ptr;
    if (label.empty()) {
      auto err = caf::make_error(ec::unspecified, "empty component label");
      return caf::make_message(std::move(err));
    }
    if (self->state.registry.find_by_label(label)) {
      auto err = caf::make_error(ec::unspecified, "duplicate component label");
      return caf::make_message(std::move(err));
    }
  } else {
    label = comp_type;
    if (!is_singleton(comp_type)) {
      label = generate_label(self, comp_type);
      VAST_DEBUG("{} auto-generated new label: {}", *self, label);
    }
  }
  VAST_DEBUG("{} spawns a {} with the label {}", *self, comp_type, label);
  auto spawn_inv = inv;
  if (comp_type == "source") {
    auto spawn_opt
      = caf::get_or(spawn_inv.options, "vast.spawn", caf::settings{});
    auto source_opt = caf::get_or(spawn_opt, "source", caf::settings{});
    auto import_opt
      = caf::get_or(spawn_inv.options, "vast.import", caf::settings{});
    detail::merge_settings(source_opt, import_opt, policy::merge_lists::no);
    spawn_inv.options["import"] = import_opt;
    caf::put(spawn_inv.options, "vast.import", import_opt);
  }
  // Spawn our new VAST component.
  spawn_arguments args{spawn_inv, self->state.dir, label};
  auto component = spawn_component(self, args.inv, args);
  if (!component) {
    if (component.error())
      VAST_WARN("{} failed to spawn component: {}", __func__,
                render(component.error()));
    return caf::make_message(std::move(component.error()));
  }
  if (auto err = register_component(self, *component, comp_type, label))
    return caf::make_message(std::move(err));
  VAST_DEBUG("{} registered {} as {}", *self, comp_type, label);
  return caf::make_message(*component);
}

auto node_state::get_endpoint_handler(const http_request_description& desc)
  -> const handler_and_endpoint& {
  static const auto empty_response = handler_and_endpoint{};
  auto it = rest_handlers.find(desc.canonical_path);
  if (it != rest_handlers.end())
    return it->second;
  // Spawn handler on first usage
  auto const* plugin = find_endpoint_plugin(desc);
  if (!plugin)
    return empty_response;
  // TODO: Monitor the spawned handler and restart if it goes down.
  auto handler = plugin->handler(self->system(), self);
  for (auto const& endpoint : plugin->rest_endpoints())
    rest_handlers[endpoint.canonical_path()]
      = std::make_pair(handler, endpoint);
  return rest_handlers.at(desc.canonical_path);
}

node_actor::behavior_type
node(node_actor::stateful_pointer<node_state> self, std::string name,
     std::filesystem::path dir, detach_components detach_filesystem) {
  self->state.name = std::move(name);
  self->state.dir = std::move(dir);
  // Initialize component and command factories.
  node_state::component_factory = make_component_factory();
  node_state::command_factory = make_command_factory();
  // Initialize the accountant.
  auto accountant = spawn_accountant(self);
  // Initialize the file system with the node directory as root.
  auto fs = detach_filesystem == detach_components::yes
              ? self->spawn<caf::detached>(posix_filesystem, self->state.dir,
                                           accountant)
              : self->spawn(posix_filesystem, self->state.dir, accountant);
  auto err
    = register_component(self, caf::actor_cast<caf::actor>(fs), "filesystem");
  VAST_ASSERT(err == caf::none); // Registration cannot fail; empty registry.
  // Remove monitored components.
  self->set_down_handler([=](const caf::down_msg& msg) {
    VAST_DEBUG("{} got DOWN from {}", *self, msg.source);
    if (!self->state.tearing_down) {
      auto actor = caf::actor_cast<caf::actor>(msg.source);
      auto component = self->state.registry.remove(actor);
      VAST_ASSERT(component);
      // Terminate if a singleton dies.
      if (is_core_component(component->type)) {
        VAST_ERROR("{} terminates after DOWN from {} with reason {}", *self,
                   component->type, msg.reason);
        self->send_exit(self, caf::exit_reason::user_shutdown);
      }
    }
  });
  // Terminate deterministically on shutdown.
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    VAST_DEBUG("{} got EXIT from {}", *self, msg.source);
    self->state.tearing_down = true;
    // Ignore duplicate EXIT messages except for hard kills.
    self->set_exit_handler([=](const caf::exit_msg& msg) {
      if (msg.reason == caf::exit_reason::kill) {
        VAST_WARN("{} received hard kill and terminates immediately", *self);
        self->quit(msg.reason);
      } else {
        VAST_DEBUG("{} ignores duplicate EXIT message from {}", *self,
                   msg.source);
      }
    });
    auto& registry = self->state.registry;
    // Core components are terminated in a second stage, we remove them from the
    // registry upfront and deal with them later.
    std::vector<caf::actor> core_shutdown_handles;
    caf::actor filesystem_handle;
    // The components listed here need to be terminated in sequential order.
    // The importer needs to shut down first because it might still have
    // buffered data. The filesystem is needed by all others for the persisting
    // logic.
    auto shutdown_sequence = std::initializer_list<const char*>{
      "importer", "index", "catalog", "accountant", "filesystem",
    };
    // Make sure that these remain in sync.
    VAST_ASSERT(std::set<const char*>{shutdown_sequence} == core_components);
    for (const char* name : shutdown_sequence) {
      if (auto comp = registry.remove(name)) {
        if (comp->type == "filesystem")
          filesystem_handle = comp->actor;
        else
          core_shutdown_handles.push_back(comp->actor);
      }
    }
    std::vector<caf::actor> aux_components;
    for (const auto& [_, comp] : registry.components()) {
      self->demonitor(comp.actor);
      // Ignore remote actors.
      if (comp.actor->node() != self->node())
        continue;
      aux_components.push_back(comp.actor);
    }
    // Drop everything.
    registry.clear();
    auto core_shutdown_sequence
      = [=, core_shutdown_handles = std::move(core_shutdown_handles),
         filesystem_handle = std::move(filesystem_handle)]() mutable {
          for (const auto& comp : core_shutdown_handles)
            self->demonitor(comp);
          shutdown<policy::sequential>(self, std::move(core_shutdown_handles));
          // We deliberately do not send an exit message to the filesystem
          // actor, as that would mean that actors not tracked by the component
          // registry which hold a strong handle to the filesystem actor cannot
          // use it for persistence on shutdown.
          self->demonitor(filesystem_handle);
          filesystem_handle = {};
        };
    terminate<policy::parallel>(self, std::move(aux_components))
      .then(
        [self, core_shutdown_sequence](atom::done) mutable {
          VAST_DEBUG("{} terminated auxiliary actors, commencing core shutdown "
                     "sequence...",
                     *self);
          core_shutdown_sequence();
        },
        [self, core_shutdown_sequence](const caf::error& err) mutable {
          VAST_ERROR("{} failed to cleanly terminate auxiliary actors {}, "
                     "shutting down core components",
                     *self, err);
          core_shutdown_sequence();
        });
  });
  // Define the node behavior.
  return {
    [self](atom::run, const invocation& inv) -> caf::result<caf::message> {
      VAST_DEBUG("{} got command {} with options {} and arguments {}", *self,
                 inv.full_name, inv.options, inv.arguments);
      // Run the command.
      this_node = self;
      return run(inv, self->system(), node_state::command_factory);
    },
    [self](atom::proxy,
           http_request_description& desc) -> caf::result<std::string> {
      VAST_VERBOSE("{} proxying request to {}", *self, desc.canonical_path);
      auto [handler, endpoint] = self->state.get_endpoint_handler(desc);
      if (!handler)
        return caf::make_error(ec::system_error,
                               "failed to spawn rest handler");
      auto rp = self->make_response_promise<std::string>();
      auto response = std::make_shared<detail::internal_http_response>(rp);
      auto params = parse_endpoint_parameters(endpoint, desc.params);
      if (!params)
        return caf::make_error(ec::invalid_argument, "invalid parameters");
      auto request = http_request{
        .params = *params,
        .response = response,
      };
      self
        ->request(handler, caf::infinite, atom::http_request_v,
                  endpoint.endpoint_id, std::move(request))
        .then(
          []() mutable {
            /* nop */
          },
          [response](const caf::error& e) mutable {
            // TODO: Should we switch to a request/response pattern for the
            // handlers so they can just return strings or errors? The downside
            // will be that it's going to be much harder to implement support
            // for chunked or streaming transfers that way.
            response->abort(500, "internal server error", e);
          });
      return rp;
    },
    [self](atom::internal, atom::spawn, atom::plugin) -> caf::result<void> {
      // Add all plugins to the component registry.
      for (const auto& component : plugins::get<component_plugin>()) {
        auto handle = component->make_component(self);
        if (!handle)
          return caf::make_error( //
            ec::unspecified,
            fmt::format("{} failed to spawn component {} from plugin {}", *self,
                        component->component_name(), component->name()));
        if (auto err
            = register_component(self, caf::actor_cast<caf::actor>(handle),
                                 component->component_name()))
          return caf::make_error( //
            ec::unspecified, fmt::format("{} failed to register component {} "
                                         "from plugin {} in component "
                                         "registry: {}",
                                         *self, component->component_name(),
                                         component->name(), err));
      }
      return {};
    },
    [self](atom::spawn, const invocation& inv) {
      VAST_DEBUG("{} got spawn command {} with options {} and arguments {}",
                 *self, inv.full_name, inv.options, inv.arguments);
      // Run the command.
      this_node = self;
      auto msg = run(inv, self->system(), node_state::command_factory);
      auto result = caf::result<caf::actor>{caf::error{}};
      if (!msg) {
        result = caf::result<caf::actor>{std::move(msg.error())};
      } else if (msg->empty()) {
        VAST_VERBOSE("{} encountered empty invocation response", *self);
      } else {
        auto f = caf::message_handler{
          [&](caf::error& x) {
            result.get_data() = std::move(x);
          },
          [&](caf::actor& x) {
            result = caf::result<caf::actor>{std::move(x)};
          },
          [&](caf::message& x) {
            VAST_ERROR("{} encountered invalid invocation response: {}", *self,
                       deep_to_string(x));
            result = caf::result<caf::actor>{caf::make_error(
              ec::invalid_result, "invalid spawn invocation response",
              std::move(x))};
          },
        };
        f(*msg);
      }
      return result;
    },
    [self](atom::put, const caf::actor& component,
           const std::string& type) -> caf::result<atom::ok> {
      VAST_DEBUG("{} got new {}", *self, type);
      if (type.empty())
        return caf::make_error(ec::unspecified, "empty component type");
      // Check if the new component is a singleton.
      auto& registry = self->state.registry;
      if (is_singleton(type) && registry.find_by_label(type))
        return caf::make_error(ec::unspecified, "component already exists");
      // Generate label
      auto label = generate_label(self, type);
      VAST_DEBUG("{} generated new component label {}", *self, label);
      if (auto err = register_component(self, component, type, label))
        return err;
      return atom::ok_v;
    },
    [self](atom::get, atom::type, const std::string& type) {
      VAST_DEBUG("{} got a request for a component of type {}", *self, type);
      auto result = self->state.registry.find_by_type(type);
      VAST_DEBUG("{} responds to the request for {} with {}", *self, type,
                 result);
      return result;
    },
    [self](atom::get, atom::label, const std::string& label) {
      VAST_DEBUG("{} got a request for the component {}", *self, label);
      auto result = self->state.registry.find_by_label(label);
      VAST_DEBUG("{} responds to the request for {} with {}", *self, label,
                 result);
      return result;
    },
    [self](atom::get, atom::label, const std::vector<std::string>& labels) {
      VAST_DEBUG("{} got a request for the components {}", *self, labels);
      std::vector<caf::actor> result;
      result.reserve(labels.size());
      for (const auto& label : labels)
        result.push_back(self->state.registry.find_by_label(label));
      VAST_DEBUG("{} responds to the request for {} with {}", *self, labels,
                 result);
      return result;
    },
    [](atom::get, atom::version) { //
      return retrieve_versions();
    },
    [self](atom::config) -> record {
      auto result = to_data(self->config().content);
      VAST_ASSERT(caf::holds_alternative<record>(result));
      return std::move(caf::get<record>(result));
    },
    [self](atom::spawn, pipeline& pipeline)
      -> caf::result<std::vector<std::pair<execution_node_actor, std::string>>> {
      auto ops = std::move(pipeline).unwrap();
      auto result = std::vector<std::pair<execution_node_actor, std::string>>{};
      result.reserve(ops.size());
      for (auto&& op : ops) {
        if (op->location() == operator_location::local) {
          return caf::make_error(ec::logic_error,
                                 fmt::format("{} cannot spawn pipeline with "
                                             "local operator '{}'",
                                             *self, op));
        }
        auto description = op->to_string();
        if (op->detached()) {
          result.emplace_back(
            self->spawn<caf::detached>(execution_node, std::move(op),
                                       caf::actor_cast<node_actor>(self)),
            std::move(description));
        } else {
          result.emplace_back(self->spawn(execution_node, std::move(op),
                                          caf::actor_cast<node_actor>(self)),
                              std::move(description));
        }
      }
      return result;
    },
  };
}

} // namespace vast::system
