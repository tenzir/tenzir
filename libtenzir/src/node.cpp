//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/node.hpp"

#include "tenzir/fwd.hpp"

#include "tenzir/accountant.hpp"
#include "tenzir/accountant_config.hpp"
#include "tenzir/atoms.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/execution_node.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/posix_filesystem.hpp"
#include "tenzir/shutdown.hpp"
#include "tenzir/spawn_arguments.hpp"
#include "tenzir/spawn_catalog.hpp"
#include "tenzir/spawn_disk_monitor.hpp"
#include "tenzir/spawn_importer.hpp"
#include "tenzir/spawn_index.hpp"
#include "tenzir/terminate.hpp"
#include "tenzir/version.hpp"

#include <caf/actor_system_config.hpp>
#include <caf/function_view.hpp>
#include <caf/io/middleman.hpp>
#include <caf/settings.hpp>

#include <chrono>
#include <ranges>

namespace tenzir {

namespace {

// This is a side-channel to communicate the self pointer into the spawn- and
// send-command functions, whose interfaces are constrained by the command
// factory.
thread_local node_actor::stateful_pointer<node_state> this_node = nullptr;

/// A list of components that are essential for importing and exporting data
/// from the node.
std::set<const char*> core_components = {
  "catalog",
  "filesystem",
  "importer",
  "index",
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
    = {"disk-monitor", "filesystem", "importer", "index", "catalog"};
  auto pred = [&](const char* x) {
    return x == type;
  };
  return std::any_of(std::begin(singletons), std::end(singletons), pred);
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
  auto tag = [&] {
    if (label.empty() or type == label) {
      return std::string{type};
    }
    return fmt::format("{}/{}", type, label);
  }();
  const auto [it, inserted] = self->state.alive_components.insert(
    std::pair{component->address(), std::move(tag)});
  TENZIR_ASSERT(
    inserted,
    fmt::format("failed to register component {}", it->second).c_str());
  TENZIR_VERBOSE("component {} registered with id {}", it->second,
                 component->id());
  self->monitor(component);
  return caf::none;
}

/// Spawns the accountant actor.
accountant_actor
spawn_accountant(node_actor::stateful_pointer<node_state> self) {
  if (!caf::get_or(content(self->system().config()), "tenzir.enable-metrics",
                   false)) {
    return {};
  }
  // It doesn't make much sense to run the accountant for one-shot commands
  // with a local database using `--node`, so this prevents spawning it.
  if (caf::get_or(content(self->system().config()), "tenzir.node", false)) {
    return {};
  }
  const auto metrics_opts = caf::get_or(content(self->system().config()),
                                        "tenzir.metrics", caf::settings{});
  auto cfg = to_accountant_config(metrics_opts);
  if (!cfg) {
    TENZIR_ERROR("{} failed to parse metrics configuration: {}", *self,
                 cfg.error());
    return {};
  }
  auto accountant = self->spawn<caf::detached + caf::linked>(
    tenzir::accountant, std::move(*cfg), self->state.dir);
  auto err = register_component(self, caf::actor_cast<caf::actor>(accountant),
                                "accountant");
  // Registration cannot fail; empty registry.
  TENZIR_ASSERT(err == caf::none);
  return accountant;
}

} // namespace

caf::expected<caf::actor>
spawn_component(node_actor::stateful_pointer<node_state> self,
                const invocation& inv, spawn_arguments& args) {
  TENZIR_TRACE_SCOPE("{} {}", TENZIR_ARG(inv), TENZIR_ARG(args));
  auto i = node_state::component_factory.find(inv.full_name);
  if (i == node_state::component_factory.end())
    return caf::make_error(ec::unspecified, "invalid spawn component");
  return i->second(self, args);
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
    {"spawn disk-monitor", lift_component_factory<spawn_disk_monitor>()},
    {"spawn importer", lift_component_factory<spawn_importer>()},
    {"spawn catalog", lift_component_factory<spawn_catalog>()},
    {"spawn index", lift_component_factory<spawn_index>()},
  };
  return result;
}

auto make_command_factory() {
  // When updating this list, remember to update its counterpart in
  // application.cpp as well iff necessary
  auto result = command::factory{
    {"spawn accountant", node_state::spawn_command},
    {"spawn disk-monitor", node_state::spawn_command},
    {"spawn importer", node_state::spawn_command},
    {"spawn catalog", node_state::spawn_command},
    {"spawn index", node_state::spawn_command},
  };
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
  TENZIR_TRACE_SCOPE("{}", inv);
  using std::begin;
  using std::end;
  auto* self = this_node;
  if (self->state.tearing_down)
    return caf::make_message(caf::make_error( //
      ec::no_error, "can't spawn a component while tearing down"));
  // We configured the command to have the name of the component.
  auto inv_name_split = detail::split(inv.full_name, " ");
  TENZIR_ASSERT(inv_name_split.size() > 1);
  std::string comp_type{inv_name_split[1]};
  // Auto-generate label if none given.
  std::string label;
  if (auto label_ptr = caf::get_if<std::string>(&inv.options, "tenzir.spawn."
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
      TENZIR_DEBUG("{} auto-generated new label: {}", *self, label);
    }
  }
  TENZIR_DEBUG("{} spawns a {} with the label {}", *self, comp_type, label);
  auto spawn_inv = inv;
  // Spawn our new Tenzir component.
  spawn_arguments args{spawn_inv, self->state.dir, label};
  auto component = spawn_component(self, args.inv, args);
  if (!component) {
    if (component.error())
      TENZIR_WARN("{} failed to spawn component: {}", __func__,
                  render(component.error()));
    return caf::make_message(std::move(component.error()));
  }
  if (auto err = register_component(self, *component, comp_type, label))
    return caf::make_message(std::move(err));
  TENZIR_DEBUG("{} registered {} as {}", *self, comp_type, label);
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
  auto result = rest_handlers.find(desc.canonical_path);
  // If no canonical path matches, `find_endpoint_plugin()` should
  // have already returned `nullptr`.
  TENZIR_ASSERT(result != rest_handlers.end());
  return result->second;
}

auto spawn_components(node_actor::stateful_pointer<node_state> self) -> void {
  using component_plugin_map
    = std::unordered_map<std::string, const component_plugin*>;
  // 1. Collect all component_plugins into a name -> plugin* map:
  component_plugin_map todo = {};
  for (const auto* component : plugins::get<component_plugin>()) {
    todo.emplace(component->component_name(), component);
  }
  // 2. Calculate an ordered loading sequnce based on the wanted_components of
  //    each plugin.
  std::vector<const component_plugin*> sequenced_components = {};
  std::unordered_set<std::string> done = {};
  auto derive_sequence = [&](auto derive_sequence, const std::string& name) {
    auto entry = todo.find(name);
    if (entry == todo.end()) {
      return;
    }
    const auto* plugin = entry->second;
    todo.erase(entry);
    if (done.contains(name)) {
      return;
    }
    for (auto& wanted : plugin->wanted_components()) {
      derive_sequence(derive_sequence, wanted);
    }
    done.insert(name);
    sequenced_components.push_back(plugin);
  };
  while (not todo.empty()) {
    derive_sequence(derive_sequence, todo.begin()->second->component_name());
  }
  // 3. Load all components in order.
  for (const auto* plugin : sequenced_components) {
    auto name = plugin->component_name();
    auto handle = plugin->make_component(self);
    if (!handle) {
      diagnostic::error("{} failed to create the {} component", *self, name)
        .throw_();
    }
    self->system().registry().put(fmt::format("tenzir.{}", name), handle);
    if (auto err
        = register_component(self, caf::actor_cast<caf::actor>(handle), name)) {
      diagnostic::error(err)
        .note("{} failed to register component {} in component registry", *self,
              name)
        .throw_();
    }
    self->state.ordered_components.push_back(name);
  }
}

node_actor::behavior_type
node(node_actor::stateful_pointer<node_state> self, std::string /*name*/,
     std::filesystem::path dir) {
  self->state.self = self;
  self->state.dir = std::move(dir);
  // Initialize component and command factories.
  node_state::component_factory = make_component_factory();
  node_state::command_factory = make_command_factory();
  // Initialize the accountant.
  auto accountant = spawn_accountant(self);
  // Initialize the file system with the node directory as root.
  auto fs
    = self->spawn<caf::detached>(posix_filesystem, self->state.dir, accountant);
  auto err
    = register_component(self, caf::actor_cast<caf::actor>(fs), "filesystem");
  TENZIR_ASSERT(err == caf::none); // Registration cannot fail; empty registry.
  // Remove monitored components.
  self->set_down_handler([=](const caf::down_msg& msg) {
    TENZIR_DEBUG("{} got DOWN from {}", *self, msg.source);
    if (self->state.monitored_exec_nodes.erase(msg.source) > 0) {
      return;
    }
    auto it = std::ranges::find_if(self->state.alive_components,
                                   [&](const auto& alive_component) {
                                     return alive_component.first == msg.source;
                                   });
    if (it != self->state.alive_components.end()) {
      TENZIR_VERBOSE("component {} deregistered; {} remaining: [{}])",
                     it->second, self->state.alive_components.size(),
                     fmt::join(self->state.alive_components
                                 | std::ranges::views::values,
                               ", "));
      self->state.alive_components.erase(it);
    }
    if (!self->state.tearing_down) {
      auto actor = caf::actor_cast<caf::actor>(msg.source);
      auto component = self->state.registry.remove(actor);
      TENZIR_ASSERT(component);
      // Terminate if a singleton dies.
      if (is_core_component(component->type)) {
        TENZIR_ERROR("{} terminates after DOWN from {} with reason {}", *self,
                     component->type, msg.reason);
        self->send_exit(self, caf::exit_reason::user_shutdown);
      }
    }
  });
  self->set_exception_handler([=](std::exception_ptr& ptr) -> caf::error {
    try {
      std::rethrow_exception(ptr);
    } catch (const diagnostic& diag) {
      return diag.to_error();
    } catch (const std::exception& err) {
      return diagnostic::error("{}", err.what())
        .note("unhandled exception in {}", *self)
        .to_error();
    } catch (...) {
      return diagnostic::error("unhandled exception in {}", *self).to_error();
    }
  });
  // Terminate deterministically on shutdown.
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    TENZIR_DEBUG("{} got EXIT from {}", *self, msg.source);
    self->state.tearing_down = true;
    for (auto&& exec_node :
         std::exchange(self->state.monitored_exec_nodes, {})) {
      if (auto handle = caf::actor_cast<caf::actor>(exec_node)) {
        self->send_exit(handle, msg.reason);
      }
    }
    // Ignore duplicate EXIT messages except for hard kills.
    self->set_exit_handler([=](const caf::exit_msg& msg) {
      if (msg.reason == caf::exit_reason::kill) {
        TENZIR_WARN("{} received hard kill and terminates immediately", *self);
        self->quit(msg.reason);
      } else {
        TENZIR_DEBUG("{} ignores duplicate EXIT message from {}", *self,
                     msg.source);
      }
    });
    auto& registry = self->state.registry;
    // Core components are terminated in a second stage, we remove them from the
    // registry upfront and deal with them later.
    std::vector<caf::actor> core_shutdown_handles;
    for (const auto& name :
         self->state.ordered_components | std::ranges::views::reverse) {
      if (auto comp = registry.remove(name)) {
        core_shutdown_handles.push_back(comp->actor);
      }
    }
    caf::actor filesystem_handle;
    // The components listed here need to be terminated in sequential order.
    // The importer needs to shut down first because it might still have
    // buffered data. The filesystem is needed by all others for the persisting
    // logic.
    auto shutdown_sequence = std::initializer_list<const char*>{
      "importer",
      "index",
      "catalog",
      "filesystem",
    };
    // Make sure that these remain in sync.
    TENZIR_ASSERT(std::set<const char*>{shutdown_sequence} == core_components);
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
          shutdown<policy::sequential>(self, std::move(core_shutdown_handles),
                                       msg.reason);
          // We deliberately do not send an exit message to the filesystem
          // actor, as that would mean that actors not tracked by the component
          // registry which hold a strong handle to the filesystem actor cannot
          // use it for persistence on shutdown.
          filesystem_handle = {};
        };
    terminate<policy::parallel>(self, std::move(aux_components))
      .then(
        [self, core_shutdown_sequence](atom::done) mutable {
          TENZIR_DEBUG("{} terminated auxiliary actors, commencing core "
                       "shutdown "
                       "sequence...",
                       *self);
          core_shutdown_sequence();
        },
        [self, core_shutdown_sequence](const caf::error& err) mutable {
          TENZIR_ERROR("{} failed to cleanly terminate auxiliary actors {}, "
                       "shutting down core components",
                       *self, err);
          core_shutdown_sequence();
        });
  });
  // Define the node behavior.
  return {
    [self](atom::proxy,
           http_request_description& desc) -> caf::result<rest_response> {
      TENZIR_VERBOSE("{} proxying request to {}", *self, desc.canonical_path);
      auto [handler, endpoint] = self->state.get_endpoint_handler(desc);
      if (!handler) {
        auto canonical_paths = std::unordered_set<std::string>{};
        for (const auto& plugin : plugins::get<rest_endpoint_plugin>()) {
          for (const auto& endpoint : plugin->rest_endpoints()) {
            canonical_paths.insert(endpoint.canonical_path());
          }
        }
        if (not canonical_paths.contains(desc.canonical_path)) {
          return rest_response::make_error(
            404, fmt::format("unknown path {}", desc.canonical_path),
            caf::make_error(ec::invalid_argument,
                            fmt::format("available paths: {}",
                                        fmt::join(canonical_paths, ", "))));
        }
        return rest_response::make_error(
          500, "internal server error",
          caf::make_error(ec::logic_error, "failed to spawn endpoint handler"));
      }
      auto unparsed_params = http_parameter_map::from_json(desc.json_body);
      if (!unparsed_params)
        return rest_response::make_error(400, "invalid json",
                                         unparsed_params.error());
      auto params = parse_endpoint_parameters(endpoint, *unparsed_params);
      if (!params)
        return rest_response::make_error(400, "invalid parameters",
                                         params.error());
      auto rp = self->make_response_promise<rest_response>();
      self
        ->request(handler, caf::infinite, atom::http_request_v,
                  endpoint.endpoint_id, *params)
        .then(
          [rp](rest_response& rsp) mutable {
            rp.deliver(std::move(rsp));
          },
          [rp](const caf::error& e) mutable {
            rp.deliver(rest_response::make_error(500, "internal error", e));
          });
      return rp;
    },
    [self](atom::internal, atom::spawn, atom::plugin) -> caf::result<void> {
      spawn_components(self);
      return {};
    },
    [self](atom::spawn, const invocation& inv) {
      TENZIR_DEBUG("{} got spawn command {} with options {} and arguments {}",
                   *self, inv.full_name, inv.options, inv.arguments);
      // Run the command.
      this_node = self;
      auto msg = run(inv, self->system(), node_state::command_factory);
      auto result = caf::result<caf::actor>{caf::error{}};
      if (!msg) {
        result = caf::result<caf::actor>{std::move(msg.error())};
      } else if (msg->empty()) {
        TENZIR_VERBOSE("{} encountered empty invocation response", *self);
      } else {
        auto f = caf::message_handler{
          [&](caf::error& x) {
            result.get_data() = std::move(x);
          },
          [&](caf::actor& x) {
            result = caf::result<caf::actor>{std::move(x)};
          },
          [&](caf::message& x) {
            TENZIR_ERROR("{} encountered invalid invocation response: {}",
                         *self, deep_to_string(x));
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
      TENZIR_DEBUG("{} got new {}", *self, type);
      if (type.empty())
        return caf::make_error(ec::unspecified, "empty component type");
      // Check if the new component is a singleton.
      auto& registry = self->state.registry;
      if (is_singleton(type) && registry.find_by_label(type))
        return caf::make_error(ec::unspecified, "component already exists");
      // Generate label
      auto label = generate_label(self, type);
      TENZIR_DEBUG("{} generated new component label {}", *self, label);
      if (auto err = register_component(self, component, type, label))
        return err;
      return atom::ok_v;
    },
    [self](atom::get, atom::type, const std::string& type) {
      TENZIR_DEBUG("{} got a request for a component of type {}", *self, type);
      auto result = self->state.registry.find_by_type(type);
      TENZIR_DEBUG("{} responds to the request for {} with {}", *self, type,
                   result);
      return result;
    },
    [self](atom::get, atom::label, const std::string& label) {
      TENZIR_DEBUG("{} got a request for the component {}", *self, label);
      auto result = self->state.registry.find_by_label(label);
      TENZIR_DEBUG("{} responds to the request for {} with {}", *self, label,
                   result);
      return result;
    },
    [self](atom::get, atom::label, const std::vector<std::string>& labels) {
      TENZIR_DEBUG("{} got a request for the components {}", *self, labels);
      std::vector<caf::actor> result;
      result.reserve(labels.size());
      for (const auto& label : labels)
        result.push_back(self->state.registry.find_by_label(label));
      TENZIR_DEBUG("{} responds to the request for {} with {}", *self, labels,
                   result);
      return result;
    },
    [](atom::get, atom::version) { //
      return retrieve_versions();
    },
    [self](atom::config) -> record {
      auto result = to_data(self->config().content);
      TENZIR_ASSERT(caf::holds_alternative<record>(result));
      return std::move(caf::get<record>(result));
    },
    [self](atom::spawn, operator_box& box, operator_type input_type,
           const receiver_actor<diagnostic>& diagnostic_handler,
           const receiver_actor<metric>& metrics_handler,
           int index) -> caf::result<exec_node_actor> {
      auto op = std::move(box).unwrap();
      if (op->location() == operator_location::local) {
        return caf::make_error(ec::logic_error,
                               fmt::format("{} cannot spawn local operator "
                                           "'{}' in remote node",
                                           *self, op->name()));
      }
      auto description = fmt::format("{:?}", op);
      auto spawn_result
        = spawn_exec_node(self, std::move(op), input_type,
                          static_cast<node_actor>(self), diagnostic_handler,
                          metrics_handler, index, false);
      if (not spawn_result) {
        return caf::make_error(ec::logic_error,
                               fmt::format("{} failed to spawn execution node "
                                           "for operator '{}': {}",
                                           *self, description,
                                           spawn_result.error()));
      }
      self->monitor(spawn_result->first);
      self->state.monitored_exec_nodes.insert(spawn_result->first->address());
      // TODO: Check output type.
      return spawn_result->first;
    },
  };
}

} // namespace tenzir
