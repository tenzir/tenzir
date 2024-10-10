//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/node.hpp"

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/atoms.hpp"
#include "tenzir/catalog.hpp"
#include "tenzir/concept/convertible/data.hpp"
#include "tenzir/concept/parseable/tenzir/si.hpp"
#include "tenzir/concept/parseable/tenzir/time.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/data.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/settings.hpp"
#include "tenzir/detail/weak_run_delayed.hpp"
#include "tenzir/disk_monitor.hpp"
#include "tenzir/execution_node.hpp"
#include "tenzir/importer.hpp"
#include "tenzir/index.hpp"
#include "tenzir/index_config.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/node.hpp"
#include "tenzir/node_control.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/posix_filesystem.hpp"
#include "tenzir/shutdown.hpp"
#include "tenzir/terminate.hpp"
#include "tenzir/uuid.hpp"
#include "tenzir/version.hpp"

#include <caf/actor_system_config.hpp>
#include <caf/function_view.hpp>
#include <caf/io/middleman.hpp>
#include <caf/settings.hpp>

#include <chrono>
#include <ranges>

namespace tenzir {

namespace {

/// A list of components that are essential for importing and exporting data
/// from the node.
constexpr auto ordered_core_components = std::array{
  "disk-monitor", "importer", "index", "catalog", "filesystem",
};

auto is_core_component(std::string_view type) -> bool {
  return std::ranges::any_of(ordered_core_components, [&](auto x) {
    return x == type;
  });
}

auto find_endpoint_plugin(const http_request_description& desc)
  -> const rest_endpoint_plugin* {
  for (auto const& plugin : plugins::get()) {
    auto const* rest_plugin = plugin.as<rest_endpoint_plugin>();
    if (!rest_plugin) {
      continue;
    }
    for (const auto& endpoint : rest_plugin->rest_endpoints()) {
      if (endpoint.canonical_path() == desc.canonical_path) {
        return rest_plugin;
      }
    }
  }
  return nullptr;
}

/// Registers (and monitors) a component through the node.
auto register_component(node_actor::stateful_pointer<node_state> self,
                        const caf::actor& component, std::string_view type,
                        std::string_view label = {}) -> caf::error {
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
  if (tag != "filesystem") {
    // TODO: This is a terrible hack, but for historical reasons the filesystem
    // actor is kept alive through a very manual process, and putting it into
    // the registry makes that situation even worse for some reason. There's a
    // related comment in the core shutdown sequence in the node actor's exit
    // handler; we should really aim to fix this sometime in the future.
    self->system().registry().put(fmt::format("tenzir.{}", tag), component);
  }
  self->state.component_names.emplace(component->address(), tag);
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

} // namespace

auto node_state::get_endpoint_handler(const http_request_description& desc)
  -> const handler_and_endpoint& {
  static const auto empty_response = handler_and_endpoint{};
  auto it = rest_handlers.find(desc.canonical_path);
  if (it != rest_handlers.end()) {
    return it->second;
  }
  // Spawn handler on first usage
  auto const* plugin = find_endpoint_plugin(desc);
  if (!plugin) {
    return empty_response;
  }
  // TODO: Monitor the spawned handler and restart if it goes down.
  auto handler = plugin->handler(self->system(), self);
  for (auto const& endpoint : plugin->rest_endpoints()) {
    rest_handlers[endpoint.canonical_path()]
      = std::make_pair(handler, endpoint);
  }
  auto result = rest_handlers.find(desc.canonical_path);
  // If no canonical path matches, `find_endpoint_plugin()` should
  // have already returned `nullptr`.
  TENZIR_ASSERT(result != rest_handlers.end());
  return result->second;
}

auto spawn_filesystem(node_actor::stateful_pointer<node_state> self)
  -> filesystem_actor {
  auto filesystem
    = self->spawn<caf::detached>(posix_filesystem, self->state.dir);
  TENZIR_ASSERT(filesystem);
  if (auto err = register_component(
        self, caf::actor_cast<caf::actor>(filesystem), "filesystem")) {
    diagnostic::error(err).note("failed to register filesystem").throw_();
  }
  return filesystem;
}

auto spawn_catalog(node_actor::stateful_pointer<node_state> self)
  -> catalog_actor {
  auto catalog = self->spawn<caf::detached>(tenzir::catalog);
  TENZIR_ASSERT(catalog);
  if (auto err = register_component(self, caf::actor_cast<caf::actor>(catalog),
                                    "catalog")) {
    diagnostic::error(err).note("failed to register catalog").throw_();
  }
  return catalog;
}

auto spawn_index(node_actor::stateful_pointer<node_state> self,
                 const caf::settings& settings,
                 const filesystem_actor& filesystem,
                 const catalog_actor& catalog) -> index_actor {
  auto index = [&] {
    const auto* index_settings = get_if(&settings, "tenzir.index");
    auto index_config = tenzir::index_config{};
    if (index_settings) {
      const auto index_settings_data = to<data>(*index_settings);
      if (not index_settings_data) {
        diagnostic::error(index_settings_data.error())
          .note("failed to convert `tenzir.index` configuration")
          .throw_();
      }
      if (auto err = convert(*index_settings_data, index_config)) {
        diagnostic::error(err)
          .note("failed to parse `tenzir.index` configuration")
          .throw_();
      }
    }
    return self->spawn<caf::detached>(
      tenzir::index, filesystem, catalog, self->state.dir / "index",
      std::string{defaults::store_backend},
      get_or(settings, "tenzir.max-partition-size",
             defaults::max_partition_size),
      get_or(settings, "tenzir.active-partition-timeout",
             defaults::active_partition_timeout),
      defaults::max_in_mem_partitions, defaults::taste_partitions,
      defaults::num_query_supervisors, self->state.dir / "index",
      std::move(index_config));
  }();
  TENZIR_ASSERT(index);
  if (auto err
      = register_component(self, caf::actor_cast<caf::actor>(index), "index")) {
    diagnostic::error(err).note("failed to register index").throw_();
  }
  return index;
}

auto spawn_importer(node_actor::stateful_pointer<node_state> self,
                    const index_actor& index) -> importer_actor {
  auto importer
    = self->spawn(tenzir::importer, self->state.dir / "importer", index);
  TENZIR_ASSERT(importer);
  if (auto err = register_component(self, caf::actor_cast<caf::actor>(importer),
                                    "importer")) {
    diagnostic::error(err).note("failed to register importer").throw_();
  }
  return importer;
}

auto spawn_disk_monitor(node_actor::stateful_pointer<node_state> self,
                        const caf::settings& settings, const index_actor& index)
  -> disk_monitor_actor {
  auto disk_monitor = [&] {
    const auto* command = caf::get_if<std::string>(
      &settings, "tenzir.start.disk-budget-check-binary");
    const auto hiwater
      = detail::get_bytesize(settings, "tenzir.start.disk-budget-high", 0);
    if (!hiwater) {
      diagnostic::error(hiwater.error())
        .note("failed to parse `tenzir.start.disk-budget-high`")
        .throw_();
    }
    auto lowater
      = detail::get_bytesize(settings, "tenzir.start.disk-budget-low", 0);
    if (!lowater) {
      diagnostic::error(lowater.error())
        .note("failed to parse `tenzir.start.disk-budget-low`")
        .throw_();
    }
    // Set low == high as the default value.
    if (not *lowater) {
      *lowater = *hiwater;
    }
    const auto step_size
      = caf::get_or(settings, "tenzir.start.disk-budget-step-size",
                    defaults::disk_monitor_step_size);
    const auto interval
      = caf::get_or(settings, "tenzir.start.disk-budget-check-interval",
                    std::chrono::seconds{defaults::disk_scan_interval}.count());
    auto disk_monitor_config = tenzir::disk_monitor_config{
      *hiwater,
      *lowater,
      step_size,
      command ? *command : std::optional<std::string>{},
      std::chrono::seconds{interval},
    };
    if (auto err = validate(disk_monitor_config)) {
      diagnostic::error(err)
        .note("failed to validate disk monitor config")
        .throw_();
    }
    if (*hiwater == 0) {
      if (command) {
        diagnostic::error("invalid configuration")
          .note("'tenzir.start.disk-budget-check-binary' is configured but "
                "'tenzir.start.disk-budget-high' is unset")
          .throw_();
      }
      return disk_monitor_actor{};
    }
    const auto db_dir_abs = std::filesystem::absolute(self->state.dir);
    return self->spawn(tenzir::disk_monitor, disk_monitor_config, db_dir_abs,
                       index);
  }();
  if (disk_monitor) {
    if (auto err = register_component(
          self, caf::actor_cast<caf::actor>(disk_monitor), "disk-monitor")) {
      diagnostic::error(err).note("failed to register disk-monitor").throw_();
    }
  }
  return disk_monitor;
}

auto spawn_components(node_actor::stateful_pointer<node_state> self) -> void {
  // Before we laod any component plugins, we first load all the core components.
  const auto& settings = content(self->system().config());
  const auto filesystem = spawn_filesystem(self);
  const auto catalog = spawn_catalog(self);
  const auto index = spawn_index(self, settings, filesystem, catalog);
  [[maybe_unused]] const auto importer = spawn_importer(self, index);
  [[maybe_unused]] const auto disk_monitor
    = spawn_disk_monitor(self, settings, index);
  // 1. Collect all component_plugins into a name -> plugin* map:
  using component_plugin_map
    = std::unordered_map<std::string, const component_plugin*>;
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

auto node(node_actor::stateful_pointer<node_state> self, std::string /*name*/,
          std::filesystem::path dir) -> node_actor::behavior_type {
  self->state.self = self;
  self->state.dir = std::move(dir);
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
      auto component = it->second;
      self->state.alive_components.erase(it);
      TENZIR_VERBOSE("component {} deregistered; {} remaining: [{}])",
                     component, self->state.alive_components.size(),
                     fmt::join(self->state.alive_components
                                 | std::ranges::views::values,
                               ", "));
    }
    if (!self->state.tearing_down) {
      auto actor = caf::actor_cast<caf::actor>(msg.source);
      auto component = self->state.registry.remove(actor);
      TENZIR_ASSERT(component);
      self->system().registry().erase(component->actor.id());
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
  auto shutdown_grace_period = std::optional<duration>{};
  if (const auto* option = caf::get_if<std::string>(
        &content(self->config()), "tenzir.shutdown-grace-period")) {
    if (auto period = duration{}; parsers::duration(*option, period)) {
      TENZIR_INFO("setting shutdown grace period to {}", data{period});
      shutdown_grace_period.emplace(period);
    } else {
      self->quit(diagnostic::error("invalid value for option "
                                   "`tenzir.shutdown-grace-period`")
                   .note("got `{}`", *option)
                   .hint("expected a duration, e.g., '10s'")
                   .to_error());
    }
  }
  // Terminate deterministically on shutdown.
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    const auto source_name = [&]() -> std::string {
      const auto component = self->state.component_names.find(msg.source);
      if (component == self->state.component_names.end()) {
        return "an unknown component";
      }
      return fmt::format("the {} component", component->second);
    }();
    TENZIR_DEBUG("{} got EXIT from {}: {}", *self, source_name, msg.reason);
    const auto node_shutdown_reason
      = msg.reason == caf::exit_reason::user_shutdown
          ? msg.reason
          : diagnostic::error(msg.reason)
              .note("node terminates after receiving error from {}",
                    source_name)
              .to_error();
    if (not self->state.tearing_down) {
      TENZIR_INFO("node shuts down after receiving an exit message from {}: {}",
                  source_name, msg.reason);
      if (shutdown_grace_period) {
        detail::weak_run_delayed(
          self, *shutdown_grace_period,
          [period = *shutdown_grace_period, source_name, reason = msg.reason] {
            TENZIR_ERROR("node failed to terminate cleanly within {} after "
                         "receiving an exit message from {} and forces a "
                         "shutdown: {}",
                         data{period}, source_name, reason);
            // Wait for the log message to be written.
            std::this_thread::sleep_for(std::chrono::seconds{1});
            std::exit(1); // NOLINT
          });
      }
    }
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
    for (const char* name : ordered_core_components) {
      if (auto comp = registry.remove(name)) {
        if (comp->type == "filesystem") {
          filesystem_handle = comp->actor;
        } else {
          core_shutdown_handles.push_back(comp->actor);
        }
      }
    }
    std::vector<caf::actor> aux_components;
    for (const auto& [_, comp] : registry.components()) {
      // Ignore remote actors.
      if (comp.actor->node() != self->node()) {
        continue;
      }
      aux_components.push_back(comp.actor);
    }
    // Drop everything.
    registry.clear();
    auto core_shutdown_sequence
      = [=, core_shutdown_handles = std::move(core_shutdown_handles),
         filesystem_handle = std::move(filesystem_handle)]() mutable {
          shutdown<policy::sequential>(self, std::move(core_shutdown_handles),
                                       node_shutdown_reason);
          // We deliberately do not send an exit message to the filesystem
          // actor, as that would mean that actors not tracked by the component
          // registry which hold a strong handle to the filesystem actor cannot
          // use it for persistence on shutdown.
        };
    terminate<policy::parallel>(self, std::move(aux_components))
      .then(
        [self, core_shutdown_sequence](atom::done) mutable {
          TENZIR_DEBUG("{} terminated auxiliary actors, commencing core "
                       "shutdown sequence...",
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
  spawn_components(self);
  // Emit metrics about called APIs once per second.
  detail::weak_run_delayed_loop(self, defaults::metrics_interval, [self] {
    const auto importer
      = self->system().registry().get<importer_actor>("tenzir.importer");
    TENZIR_ASSERT(importer);
    for (auto& [_, builder] : self->state.api_metrics_builders) {
      if (builder.length() == 0) {
        continue;
      }
      self->send(importer, builder.finish_assert_one_slice());
    }
  });
  return {
    [self](atom::proxy, http_request_description& desc,
           std::string& request_id) -> caf::result<rest_response> {
      TENZIR_VERBOSE("{} proxying request with id {} to {} with {}", *self,
                     request_id, desc.canonical_path, desc.json_body);
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
      if (!unparsed_params) {
        return rest_response::make_error(400, "invalid json",
                                         unparsed_params.error());
      }
      auto params = parse_endpoint_parameters(endpoint, *unparsed_params);
      if (!params) {
        return rest_response::make_error(400, "invalid parameters",
                                         params.error());
      }
      auto rp = self->make_response_promise<rest_response>();
      auto deliver = [rp, self, desc, params, endpoint,
                      request_id = std::move(request_id),
                      start_time = std::chrono::steady_clock::now()](
                       caf::expected<rest_response> response) mutable {
        auto it = self->state.api_metrics_builders.find(desc.canonical_path);
        if (it == self->state.api_metrics_builders.end()) {
          auto builder = series_builder{type{
            "tenzir.metrics.api",
            record_type{
              {"timestamp", time_type{}},
              {"request_id", string_type{}},
              {"method", string_type{}},
              {"path", string_type{}},
              {"response_time", duration_type{}},
              {"status_code", uint64_type{}},
              {"params", endpoint.params.value_or(record_type{})},
            },
            {{"internal"}},
          }};
          it = self->state.api_metrics_builders.emplace_hint(
            it, desc.canonical_path, std::move(builder));
        }
        auto metric = it->second.record();
        metric.field("timestamp", time::clock::now());
        if (not request_id.empty()) {
          metric.field("request_id", request_id);
        }
        metric.field("method", fmt::to_string(endpoint.method));
        metric.field("path", endpoint.path);
        metric.field("response_time",
                     duration{std::chrono::steady_clock::now() - start_time});
        metric.field("status_code",
                     response ? uint64_t{response->code()} : uint64_t{500});
        metric.field("params", *params);
        if (not response) {
          rp.deliver(
            rest_response::make_error(500, "internal error", response.error()));
          return;
        }
        rp.deliver(std::move(*response));
      };
      self
        ->request(handler, caf::infinite, atom::http_request_v,
                  endpoint.endpoint_id, *params)
        .then(
          [deliver](rest_response& rsp) mutable {
            deliver(std::move(rsp));
          },
          [deliver](caf::error& err) mutable {
            deliver(std::move(err));
          });
      return rp;
    },
    [self](atom::get, atom::label, const std::vector<std::string>& labels)
      -> caf::result<std::vector<caf::actor>> {
      TENZIR_DEBUG("{} got a request for the components {}", *self, labels);
      std::vector<caf::actor> result;
      result.reserve(labels.size());
      auto failed = std::vector<std::string>{};
      for (const auto& label : labels) {
        auto handle = self->state.registry.find_by_label(label);
        if (not handle) {
          failed.push_back(label);
          continue;
        } else {
          result.push_back(std::move(handle));
        }
      }
      if (not failed.empty()) {
        return diagnostic::error("node failed to retrieve components: {}",
                                 fmt::join(failed, ", "))
          .to_error();
      }
      TENZIR_DEBUG("{} responds to the request for {} with {}", *self, labels,
                   result);
      return result;
    },
    [](atom::get, atom::version) { //
      return retrieve_versions();
    },
    [self](atom::spawn, operator_box& box, operator_type input_type,
           const receiver_actor<diagnostic>& diagnostic_handler,
           const metrics_receiver_actor& metrics_receiver, int index,
           bool is_hidden) -> caf::result<exec_node_actor> {
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
                          metrics_receiver, index, false, is_hidden);
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
