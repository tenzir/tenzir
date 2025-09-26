//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/registry.hpp"

#include "tenzir/ir.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/tql2/exec.hpp"

#include <ranges>
#include <tenzir/logger.hpp>
#include <atomic>
#include <memory>
#include <mutex>

namespace tenzir {

namespace {

void gather_names(const module_def& mod, entity_ns ns, std::string prefix,
                  std::vector<std::string>& out) {
  auto result = std::vector<std::string>{};
  for (auto& [name, def] : mod.defs) {
    auto eligible = std::invoke([&] {
      switch (ns) {
        case entity_ns::op:
          return def.op.has_value();
        case entity_ns::fn:
          return def.fn != nullptr;
        case entity_ns::mod:
          return def.mod != nullptr;
      }
      TENZIR_UNREACHABLE();
    });
    if (eligible) {
      out.push_back(prefix + name);
    }
    if (def.mod) {
      gather_names(*def.mod, ns, prefix + name + "::", out);
    }
  }
}

} // namespace

auto operator_def::make(operator_factory_plugin::invocation inv,
                        session ctx) const -> failure_or<operator_ptr> {
  return match(
    kind_,
    [&](const user_defined_operator& udo) -> failure_or<operator_ptr> {
      if (not inv.args.empty()) {
        diagnostic::error("user-defined operator does not support arguments")
          .primary(inv.self)
          .emit(ctx);
        return failure::promise();
      }
      TRY(auto compiled, compile(ast::pipeline{udo.definition}, ctx));
      return std::make_unique<pipeline>(std::move(compiled));
    },
    [&](const native_operator& op) -> failure_or<operator_ptr> {
      if (not op.factory_plugin) {
        diagnostic::error("this operator can only be used with the new IR")
          .primary(inv.self)
          .emit(ctx);
        return failure::promise();
      }
      return op.factory_plugin->make(inv, ctx);
    });
}

thread_local const registry* g_thread_local_registry = nullptr;

static auto global_registry_atom()
  -> std::atomic<std::shared_ptr<const registry>>& {
  static std::atomic<std::shared_ptr<const registry>> atom{};
  return atom;
}

static auto global_registry_mutex() -> std::mutex& {
  static std::mutex mtx;
  return mtx;
}

auto thread_local_registry() -> const registry* {
  return g_thread_local_registry;
}

void set_thread_local_registry(const registry* reg) {
  g_thread_local_registry = reg;
}

auto global_registry() -> std::shared_ptr<const registry> {
  auto& atom = global_registry_atom();
  auto reg = atom.load(std::memory_order_acquire);
  if (!reg) [[unlikely]] {
    static std::once_flag init_once;
    std::call_once(init_once, [&] {
      auto init = std::make_shared<registry>();
      for (const auto* op : plugins::get<operator_factory_plugin>()) {
        auto name = op->name();
        if (name.starts_with("tql2.")) {
          name.erase(0, 5);
        }
        init->add(std::string{entity_pkg_std}, name, native_operator{nullptr, op});
      }
      init->add(std::string{entity_pkg_std}, name,
                native_operator{nullptr, op});
    }
    for (const auto* op : plugins::get<operator_compiler_plugin>()) {
      auto name = op->operator_name();
      if (name.starts_with("tql2.")) {
        name.erase(0, 5);
      }
      init->add(std::string{entity_pkg_std}, name,
                native_operator{op, nullptr});
    }
    for (const auto* fn : plugins::get<function_plugin>()) {
      auto name = fn->function_name();
      if (name.starts_with("tql2.")) {
        name.erase(0, 5);
      }
      atom.store(std::shared_ptr<const registry>{init},
                 std::memory_order_release);
    });
    reg = atom.load(std::memory_order_acquire);
  }
  return reg;
}

auto begin_registry_update() -> registry_update_guard {
  return registry_update_guard{std::unique_lock<std::mutex>{global_registry_mutex()}};
}

auto registry_update_guard::current() const -> std::shared_ptr<const registry> {
  // Ensure a snapshot exists by going through the public accessor.
  return global_registry();
}

void registry_update_guard::publish(std::shared_ptr<const registry>&& next) const {
  global_registry_atom().store(std::move(next), std::memory_order_release);
}

auto registry::get(const ast::function_call& call) const
  -> const function_plugin& {
  auto def = get(call.fn.ref);
  auto fn = std::get_if<std::reference_wrapper<const function_plugin>>(&def);
  TENZIR_ASSERT(fn);
  return *fn;
}

auto registry::get(const ast::invocation& inv) const -> const operator_def& {
  auto def = get(inv.op.ref);
  auto op = std::get_if<std::reference_wrapper<const operator_def>>(&def);
  TENZIR_ASSERT(op);
  return *op;
}

auto registry::try_get(const entity_path& path) const
  -> variant<entity_ref, error> {
  TENZIR_ASSERT(path.resolved());
  auto current = &root(path.pkg());
  auto&& segments = path.segments();
  for (auto i = size_t{0}; i < segments.size(); ++i) {
    auto it = current->defs.find(segments[i]);
    if (it == current->defs.end()) {
      if (i == 0) {
        TENZIR_INFO("registry.try_get: root '{}' missing first segment '{}' in ns {}. Available entries: {}",
                    path.pkg(), segments[i], path.ns(),
                    fmt::join(std::views::keys(current->defs), ", "));
      }
      // No such entity.
      return error{i, false};
    }
    auto& set = it->second;
    if (i == segments.size() - 1) {
      // Failure here indicates that it has the wrong type.
      switch (path.ns()) {
        case entity_ns::fn:
          if (set.fn) {
            return *set.fn;
          }
          return error{i, true};
        case entity_ns::op:
          if (set.op) {
            return *set.op;
          }
          return error{i, true};
        case entity_ns::mod:
          if (set.mod) {
            return *set.mod;
          }
          return error{i, true};
      }
      TENZIR_UNREACHABLE();
    }
    if (not set.mod) {
      // Entity found but it is not a module.
      return error{i, true};
    }
    current = set.mod.get();
  }
  TENZIR_UNREACHABLE();
}

auto registry::get(const entity_path& path) const -> entity_ref {
  auto result = try_get(path);
  TENZIR_ASSERT(std::holds_alternative<entity_ref>(result));
  return std::get<entity_ref>(result);
}

auto registry::operator_names() const -> std::vector<std::string> {
  return entity_names(entity_ns::op);
}

auto registry::function_names() const -> std::vector<std::string> {
  return entity_names(entity_ns::fn);
}

auto registry::module_names() const -> std::vector<std::string> {
  return entity_names(entity_ns::mod);
}

auto registry::entity_names(entity_ns ns) const -> std::vector<std::string> {
  auto result = std::vector<std::string>{};
  for (const auto& [_, mod] : roots_) {
    gather_names(mod, ns, "", result);
  }
  std::ranges::sort(result);
  return result;
}

void registry::add(const entity_pkg& package, std::string_view name,
                   entity_def def) {
  TENZIR_ASSERT(not name.empty());
  auto path = detail::split(name, "::");
  TENZIR_ASSERT(not path.empty());
  // Find the correct module first.
  auto mod = &root(package);
  for (auto& segment : path) {
    if (&segment == &path.back()) {
      break;
    }
    auto& set = mod->defs[std::string{segment}];
    if (not set.mod) {
      set.mod = std::make_unique<module_def>();
    }
    mod = set.mod.get();
  }
  // Insert the entity definition into the module.
  auto& set = mod->defs[std::string{path.back()}];
  match(
    std::move(def),
    [&](std::reference_wrapper<const function_plugin> plugin) {
      TENZIR_ASSERT(not set.fn);
      set.fn = &plugin.get();
    },
    [&](operator_def def) {
      // For compatibility reasons, we handle the case where it was already
      // registered but only with the legacy plugin type.
      // TENZIR_ASSERT(not set.op);
      if (not set.op) {
        set.op = std::move(def);
        return;
      }
      auto existing = try_as<native_operator>(set.op->inner());
      TENZIR_ASSERT(existing);
      auto incoming = try_as<native_operator>(def.inner());
      TENZIR_ASSERT(incoming);
      if (incoming->factory_plugin) {
        TENZIR_ASSERT(not existing->factory_plugin);
        existing->factory_plugin = incoming->factory_plugin;
      }
      if (incoming->ir_plugin) {
        TENZIR_ASSERT(not existing->ir_plugin);
        existing->ir_plugin = incoming->ir_plugin;
      }
    });
}

void registry::add_module(const entity_pkg& package, std::string_view name,
                          std::unique_ptr<module_def> mod) {
  TENZIR_ASSERT(! name.empty());
  auto path = detail::split(name, "::");
  TENZIR_ASSERT(! path.empty());
  // Find or create parent module.
  auto* parent = &root(package);
  for (auto& segment : path) {
    if (&segment == &path.back()) {
      break;
    }
    auto& set = parent->defs[std::string{segment}];
    if (not set.mod) {
      set.mod = std::make_unique<module_def>();
    }
    parent = set.mod.get();
  }
  auto& set = parent->defs[std::string{path.back()}];
  TENZIR_ASSERT(! set.mod && "module already exists at path");
  set.mod = std::move(mod);
}

void registry::replace_module(const entity_pkg& package, std::string_view name,
                              std::unique_ptr<module_def> mod) {
  TENZIR_ASSERT(! name.empty());
  auto path = detail::split(name, "::");
  TENZIR_ASSERT(! path.empty());
  auto* parent = &root(package);
  for (auto& segment : path) {
    if (&segment == &path.back()) {
      break;
    }
    auto& set = parent->defs[std::string{segment}];
    if (not set.mod) {
      set.mod = std::make_unique<module_def>();
    }
    parent = set.mod.get();
  }
  auto& set = parent->defs[std::string{path.back()}];
  set.mod = std::move(mod);
}

void registry::remove_module(const entity_pkg& package, std::string_view name) {
  TENZIR_ASSERT(! name.empty());
  auto path = detail::split(name, "::");
  TENZIR_ASSERT(! path.empty());
  auto* parent = &root(package);
  for (auto& segment : path) {
    if (&segment == &path.back()) {
      break;
    }
    auto it = parent->defs.find(std::string{segment});
    if (it == parent->defs.end() || ! it->second.mod) {
      // Nothing to remove; path does not exist.
      return;
    }
    parent = it->second.mod.get();
  }
  auto it = parent->defs.find(std::string{path.back()});
  if (it == parent->defs.end()) {
    return;
  }
  it->second.mod.reset();
  if (! it->second.fn && ! it->second.op && ! it->second.mod) {
    parent->defs.erase(it);
  }
}

namespace {
auto clone_module(const module_def& src) -> std::unique_ptr<module_def> {
  auto out = std::make_unique<module_def>();
  for (const auto& [k, v] : src.defs) {
    entity_set set{};
    set.fn = v.fn;
    set.op = v.op; // deep copy of optional operator_def
    if (v.mod) {
      set.mod = clone_module(*v.mod);
    }
    out->defs.emplace(k, std::move(set));
  }
  return out;
}
} // namespace

auto registry::clone() const -> std::unique_ptr<registry> {
  auto out = std::make_unique<registry>();
  for (const auto& [name, mod] : roots_) {
    auto cloned = clone_module(mod);
    out->roots_.emplace(name, std::move(*cloned));
  }
  return out;
}

auto registry::root(const entity_pkg& package) -> module_def& {
  return roots_[package];
}

auto registry::root(const entity_pkg& package) const -> const module_def& {
  if (auto it = roots_.find(package); it != roots_.end()) {
    return it->second;
  }
  static const module_def empty;
  return empty;
}

} // namespace tenzir
