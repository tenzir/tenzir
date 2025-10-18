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
#include "tenzir/allocator.hpp"
#include "tenzir/atoms.hpp"
#include "tenzir/catalog.hpp"
#include "tenzir/concept/convertible/data.hpp"
#include "tenzir/data.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/actor_metrics.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/env.hpp"
#include "tenzir/detail/process.hpp"
#include "tenzir/detail/settings.hpp"
#include "tenzir/detail/weak_run_delayed.hpp"
#include "tenzir/disk_monitor.hpp"
#include "tenzir/ecc.hpp"
#include "tenzir/execution_node.hpp"
#include "tenzir/importer.hpp"
#include "tenzir/index.hpp"
#include "tenzir/index_config.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/posix_filesystem.hpp"
#include "tenzir/secret_store.hpp"
#include "tenzir/shutdown.hpp"
#include "tenzir/terminate.hpp"
#include "tenzir/uuid.hpp"
#include "tenzir/version.hpp"

#include <boost/asio/execution_context.hpp>
#include <boost/process/v2/environment.hpp>
#include <caf/actor_from_state.hpp>
#include <caf/actor_registry.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/function_view.hpp>
#include <caf/io/middleman.hpp>
#include <caf/settings.hpp>

#if (defined(__GLIBC__))
#  include <malloc.h>
#endif

#include <chrono>
#include <ranges>
#include <string_view>
#include <utility>

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
    if (not rest_plugin) {
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
  if (not self->state().registry.add(component, std::string{type},
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
  self->system().registry().put(fmt::format("tenzir.{}", tag), component);
  self->state().component_names.emplace(component->address(), tag);
  const auto [it, inserted] = self->state().alive_components.insert(
    std::pair{component->address(), std::move(tag)});
  TENZIR_ASSERT(
    inserted,
    fmt::format("failed to register component {}", it->second).c_str());
  TENZIR_VERBOSE("component {} registered with id {}", it->second,
                 component->id());
  self->monitor(
    component, [self, source = component->address()](const caf::error& err) {
      TENZIR_DEBUG("{} got DOWN from {}", *self, err);
      const auto it = std::ranges::find_if(
        self->state().alive_components, [&](const auto& alive_component) {
          return alive_component.first == source;
        });
      TENZIR_ASSERT(it != self->state().alive_components.end());
      auto component = it->second;
      self->state().alive_components.erase(it);
      TENZIR_VERBOSE("component {} deregistered; {} remaining: [{}])",
                     component, self->state().alive_components.size(),
                     fmt::join(self->state().alive_components
                                 | std::ranges::views::values,
                               ", "));
      self->system().registry().erase(source.id());
      if (not self->state().tearing_down) {
        auto component = self->state().registry.remove(source);
        // Terminate if a singleton dies.
        if (is_core_component(component->type)) {
          TENZIR_ERROR("{} terminates after DOWN from {} with reason {}", *self,
                       component->type, err);
          self->send_exit(self, caf::exit_reason::user_shutdown);
        }
      }
    });
  return caf::none;
}

auto spawn_filesystem(node_actor::stateful_pointer<node_state> self)
  -> filesystem_actor {
  auto filesystem = self->spawn<caf::detached + caf::hidden>(posix_filesystem,
                                                             self->state().dir);
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
      tenzir::index, filesystem, catalog, self->state().dir / "index",
      std::string{defaults::store_backend},
      // By default, we allow the event count corresponding to 3 *full*
      // partitions. Assuming 250 bytes for each event and a default partition
      // size of 4Mi, this works out to be around ~3 GB maximum memory.
      get_or(settings, "tenzir.max-buffered-events",
             defaults::max_partition_size * 3),
      get_or(settings, "tenzir.max-partition-size",
             defaults::max_partition_size),
      get_or(settings, "tenzir.active-partition-timeout",
             defaults::active_partition_timeout),
      defaults::max_in_mem_partitions, defaults::taste_partitions,
      defaults::num_query_supervisors, self->state().dir / "index",
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
  auto importer = self->spawn(caf::actor_from_state<tenzir::importer>, index);
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
    if (not hiwater) {
      diagnostic::error(hiwater.error())
        .note("failed to parse `tenzir.start.disk-budget-high`")
        .throw_();
    }
    auto lowater
      = detail::get_bytesize(settings, "tenzir.start.disk-budget-low", 0);
    if (not lowater) {
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
    const auto db_dir_abs = std::filesystem::absolute(self->state().dir);
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
    if (not handle) {
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
    self->state().ordered_components.push_back(name);
  }
}

} // namespace

auto node_state::create_pipeline_shell() -> void {
  TENZIR_ASSERT(endpoint.has_value());
  static const auto tenzir_ctl
    = detail::objectpath()->parent_path().parent_path() / "bin" / "tenzir-ctl";
  auto proc = reproc::process{};
  auto options = reproc::options{};
  options.redirect.err.type = reproc::redirect::parent;
  auto proc_stop = reproc::stop_actions{
    .first = {.action = reproc::stop::terminate,
              .timeout = reproc::milliseconds(10),},
    .second = {.action = reproc::stop::kill,
               .timeout = reproc::milliseconds(0),},
    .third = {},
  };
  options.stop = proc_stop;
  auto console_verbosity
    = caf::get_or<std::string>(self->config().content,
                               "tenzir.console-verbosity",
                               tenzir::defaults::logger::console_verbosity);
  auto args = std::vector<std::string>{
    tenzir_ctl,
    fmt::format("--console-verbosity={}", console_verbosity),
    "pipeline_shell",
    fmt::to_string(*endpoint),
    fmt::to_string(child_id),
  };
  if (auto err = proc.start(args, options)) {
    TENZIR_WARN("Failed to start child process: {}", err);
    return;
  }
  creating_pipeline_shells.emplace(child_id, std::move(proc));
  child_id++;
}

auto node_state::monitor_shell_for_pipe(caf::strong_actor_ptr client,
                                        reproc::process proc) -> void {
  auto addr = client->address();
  owned_shells.emplace(addr, std::move(proc));
  self->monitor(client, [this, addr](const caf::error&) {
    auto& ps = owned_shells;
    const auto it = ps.find(addr);
    if (it == ps.end()) {
      return;
    }
    TENZIR_ASSERT(it != ps.end(),
                  "child terminator got down from unknown client");
    if (auto err = it->second.terminate()) {
      TENZIR_WARN("failed to terminate subprocess: {}", err);
    }
    ps.erase(it);
  });
}

auto node_state::connect_pipeline_shell(uint32_t child_id,
                                        pipeline_shell_actor handle)
  -> caf::result<void> {
  auto it = creating_pipeline_shells.find(child_id);
  TENZIR_ASSERT(it != creating_pipeline_shells.end());
  auto proc = std::move(it->second);
  creating_pipeline_shells.erase(it);
  if (shell_response_promises.empty()) {
    created_pipeline_shells.emplace_back(std::move(proc), std::move(handle));
    return {};
  }
  auto promise = shell_response_promises.front();
  shell_response_promises.pop_front();
  auto client = promise.source();
  promise.deliver(std::move(handle));
  monitor_shell_for_pipe(client, std::move(proc));
  return {};
}

auto node_state::get_pipeline_shell() -> caf::result<pipeline_shell_actor> {
  self->schedule_fn([this]() {
    create_pipeline_shell();
  });
  if (not created_pipeline_shells.empty()) {
    auto [proc, shell] = std::move(created_pipeline_shells.front());
    created_pipeline_shells.pop_front();
    auto client = self->current_sender();
    monitor_shell_for_pipe(client, std::move(proc));
    return shell;
  }
  // empty
  auto rp = self->make_response_promise<pipeline_shell_actor>();
  shell_response_promises.push_back(rp);
  return rp;
}

auto node_state::get_endpoint_handler(const http_request_description& desc)
  -> const handler_and_endpoint& {
  static const auto empty_response = handler_and_endpoint{};
  auto it = rest_handlers.find(desc.canonical_path);
  if (it != rest_handlers.end()) {
    return it->second;
  }
  // Spawn handler on first usage
  auto const* plugin = find_endpoint_plugin(desc);
  if (not plugin) {
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

auto node(node_actor::stateful_pointer<node_state> self,
          std::filesystem::path dir, bool pipeline_subprocesses)
  -> node_actor::behavior_type {
  self->state().self = self;
  self->state().dir = std::move(dir);
  self->state().pipeline_subprocesses = pipeline_subprocesses;
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
  spawn_components(self);
  // Emit metrics once per second.
  detail::weak_run_delayed_loop(
    self, defaults::metrics_interval,
    [self, actor_metrics_builder
           = detail::make_actor_metrics_builder()]() mutable {
      const auto importer
        = self->system().registry().get<importer_actor>("tenzir.importer");
      self->mail(detail::generate_actor_metrics(actor_metrics_builder, self))
        .send(importer);
      TENZIR_ASSERT(importer);
      for (auto& [_, builder] : self->state().api_metrics_builders) {
        if (builder.length() == 0) {
          continue;
        }
        self->mail(builder.finish_assert_one_slice()).send(importer);
      }
    });
  constexpr auto get_interval = [](const char* env) -> duration {
    duration trim_interval = std::chrono::minutes{10};
    const auto allocator_trim_interval_env = detail::getenv(env);
    if (allocator_trim_interval_env) {
      auto begin = allocator_trim_interval_env->begin();
      auto end = allocator_trim_interval_env->end();
      if (not parsers::simple_duration.parse(begin, end, trim_interval)) {
        TENZIR_WARN("failed to parsed environment variable "
                    "`{}={}`; Using ",
                    env, *allocator_trim_interval_env, trim_interval);
      }
    }
    return trim_interval;
  };
  detail::weak_run_delayed_loop(
    self, get_interval("TENZIR_ALLOC_CPP_TRIM_INTERVAL"), []() {
      memory::cpp_allocator().trim();
    });
  if (memory::cpp_allocator().backend()
      != memory::arrow_allocator().backend()) {
    detail::weak_run_delayed_loop(
      self, get_interval("TENZIR_ALLOC_ARROW_TRIM_INTERVAL"), []() {
        memory::arrow_allocator().trim();
      });
  }
  return {
    [self](atom::proxy, http_request_description& desc,
           std::string& request_id) -> caf::result<rest_response> {
      TENZIR_DEBUG("{} proxying request with id {} to {} with {}", *self,
                   request_id, desc.canonical_path, desc.json_body);
      auto [handler, endpoint] = self->state().get_endpoint_handler(desc);
      if (not handler) {
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
      if (not unparsed_params) {
        return rest_response::make_error(400, "invalid json",
                                         unparsed_params.error());
      }
      auto params = parse_endpoint_parameters(endpoint, *unparsed_params);
      if (not params) {
        return rest_response::make_error(400, "invalid parameters",
                                         params.error());
      }
      auto rp = self->make_response_promise<rest_response>();
      auto deliver = [rp, self, desc, params, endpoint,
                      request_id = std::move(request_id),
                      start_time = std::chrono::steady_clock::now()](
                       caf::expected<rest_response> response) mutable {
        auto it = self->state().api_metrics_builders.find(desc.canonical_path);
        if (it == self->state().api_metrics_builders.end()) {
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
          it = self->state().api_metrics_builders.emplace_hint(
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
      self->mail(atom::http_request_v, endpoint.endpoint_id, *params)
        .request(handler, caf::infinite)
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
        auto handle = self->state().registry.find_by_label(label);
        if (not handle) {
          failed.push_back(label);
          continue;
        }
        result.push_back(std::move(handle));
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
    [self](atom::get, atom::version) {
      return retrieve_versions(check(to<record>(content(self->config()))));
    },
    [self](atom::spawn, operator_box& box, operator_type input_type,
           std::string definition,
           const receiver_actor<diagnostic>& diagnostic_handler,
           const metrics_receiver_actor& metrics_receiver, int index,
           bool is_hidden, uuid run_id) -> caf::result<exec_node_actor> {
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
                          std::move(definition), static_cast<node_actor>(self),
                          diagnostic_handler, metrics_receiver, index, false,
                          is_hidden, run_id);
      if (not spawn_result) {
        return caf::make_error(ec::logic_error,
                               fmt::format("{} failed to spawn execution node "
                                           "for operator '{}': {}",
                                           *self, description,
                                           spawn_result.error()));
      }
      self->monitor(
        spawn_result->first,
        [self, source = spawn_result->first->address()](const caf::error&) {
          if (self->state().tearing_down) {
            return;
          }
          const auto num_erased
            = self->state().monitored_exec_nodes.erase(source);
          TENZIR_ASSERT(num_erased == 1);
        });
      self->state().monitored_exec_nodes.insert(spawn_result->first->address());
      // TODO: Check output type.
      return spawn_result->first;
    },
    [self](const caf::exit_msg& msg) {
      const auto source_name = [&]() -> std::string {
        const auto component = self->state().component_names.find(msg.source);
        if (component == self->state().component_names.end()) {
          return "an unknown component";
        }
        return fmt::format("the {} component", component->second);
      }();
      if (self->state().tearing_down) {
        if (msg.reason == caf::exit_reason::kill) {
          TENZIR_WARN("{} received hard kill from {} and terminates "
                      "immediately",
                      *self, source_name);
          self->quit(msg.reason);
        } else {
          TENZIR_DEBUG("{} ignores duplicate EXIT message from {}", *self,
                       source_name);
        }
        return;
      }
      TENZIR_DEBUG("{} got EXIT from {}: {}", *self, source_name, msg.reason);
      const auto node_shutdown_reason
        = not msg.reason or msg.reason == caf::exit_reason::user_shutdown
              or msg.reason == ec::silent
            ? msg.reason
            : diagnostic::error(msg.reason)
                .note("node terminates after receiving error from {}",
                      source_name)
                .to_error();
      self->state().tearing_down = true;
      for (auto&& exec_node :
           std::exchange(self->state().monitored_exec_nodes, {})) {
        if (auto handle = caf::actor_cast<caf::actor>(exec_node)) {
          self->send_exit(handle, msg.reason);
        }
      }
      // Tell pipeline executors that are waiting for pipeline shells that we
      // are shutting down. This should not be treated as an error in the
      // pipeline itself.
      auto& ps = self->state().shell_response_promises;
      for (auto& p : ps) {
        p.deliver(caf::make_error(ec::silent));
      }
      ps.clear();
      auto& registry = self->state().registry;
      // Core components are terminated in a second stage, we remove them from
      // the registry upfront and deal with them later.
      std::vector<caf::actor> core_shutdown_handles;
      // Always shut down the pipeline manager first. This is a must, as
      // otherwise the shutdown of other components can cause dependent
      // pipelines to either complete, stop, or fail, and if the pipeline
      // manager hasn't yet noticed that it's supposed to shutdown it may still
      // want to persist its state afterwards, causing the pipelines to be in
      // the wrong state after restarting.
      if (auto pm = registry.remove("pipeline-manager")) {
        core_shutdown_handles.push_back(std::move(pm->actor));
      }
      for (const auto& name :
           self->state().ordered_components | std::ranges::views::reverse) {
        if (auto comp = registry.remove(name)) {
          core_shutdown_handles.push_back(comp->actor);
        }
      }
      for (const char* name : ordered_core_components) {
        if (auto comp = registry.remove(name)) {
          core_shutdown_handles.push_back(comp->actor);
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
        = [=, core_shutdown_handles
              = std::move(core_shutdown_handles)]() mutable {
            shutdown<policy::sequential>(self, std::move(core_shutdown_handles),
                                         node_shutdown_reason);
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
    },
    [self](atom::set, endpoint endpoint) {
      TENZIR_ASSERT(endpoint.port != 0);
      self->state().endpoint = std::move(endpoint);
      if (self->state().pipeline_subprocesses) {
        for (int i = 0; i < 5; i++) {
          self->state().create_pipeline_shell();
        }
      }
    },
    [self](atom::spawn, atom::shell) -> caf::result<pipeline_shell_actor> {
      if (not self->state().pipeline_subprocesses) {
        return pipeline_shell_actor{};
      }
      if (not self->state().endpoint) {
        return self->mail(atom::spawn_v, atom::shell_v)
          .delegate(static_cast<node_actor>(self));
      }
      return self->state().get_pipeline_shell();
    },
    [self](atom::connect, atom::shell, uint32_t child_id,
           pipeline_shell_actor handle) -> caf::result<void> {
      if (self->state().tearing_down) {
        // Just ignore.
        return ec::no_error;
      }
      return self->state().connect_pipeline_shell(child_id, std::move(handle));
    },
    [self](atom::resolve, std::string name,
           std::string public_key) -> caf::result<secret_resolution_result> {
      const auto& cfg = content(self->system().config());
      const auto key = fmt::format("tenzir.secrets.{}", name);
      const auto* value = caf::get_if(&cfg, key);
      if (value) {
        auto value_string = caf::get_as<std::string>(*value);
        if (not value_string) {
          return secret_resolution_error{"config secret is not a string"};
        }
        auto encrypted = ecc::encrypt(*value_string, public_key);
        if (not encrypted) {
          return encrypted.error();
        }
        return encrypted_secret_value{*encrypted};
      }
      auto store
        = self->system().registry().get<secret_store_actor>("tenzir.platform");
      if (not store) {
        return secret_resolution_error{
          "secret does not exist locally and no secret store is available"};
      }
      auto rp = self->make_response_promise<secret_resolution_result>();
      // We apparently cannot `delegate` here, since this may be across process
      // boundaries if the request came from the client process.
      // https://github.com/actor-framework/actor-framework/issues/2056
      self->mail(atom::resolve_v, std::move(name), std::move(public_key))
        .request(store, caf::infinite)
        .then(
          [rp = rp](secret_resolution_result r) mutable {
            rp.deliver(std::move(r));
          },
          [rp = rp](caf::error e) mutable {
            rp.deliver(std::move(e));
          });
      return rp;
    },
  };
}

} // namespace tenzir
