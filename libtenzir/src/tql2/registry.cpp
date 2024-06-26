//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/registry.hpp"

#include "tenzir/plugin.hpp"

namespace tenzir {

thread_local const registry* g_thread_local_registry = nullptr;

auto thread_local_registry() -> const registry* {
  return g_thread_local_registry;
}

void set_thread_local_registry(const registry* reg) {
  g_thread_local_registry = reg;
}

auto global_registry() -> const registry& {
  static auto reg = std::invoke([] {
    auto reg = registry{};
    for (auto op : plugins::get<operator_factory_plugin>()) {
      auto name = op->name();
      // TODO
      if (name.starts_with("tql2.")) {
        name = name.substr(5);
      }
      reg.add(name, op);
    }
    for (auto fn : plugins::get<function_plugin>()) {
      auto name = fn->name();
      // TODO
      if (name.starts_with("tql2.")) {
        name = name.substr(5);
      }
      reg.add(name, fn);
    }
    return reg;
  });
  return reg;
}

auto registry::get(const ast::function_call& call) const
  -> const function_plugin& {
  TENZIR_ASSERT(call.fn.ref.resolved());
  auto def = try_get(call.fn.ref);
  TENZIR_ASSERT(def);
  auto fn = std::get_if<const function_plugin*>(def);
  TENZIR_ASSERT(fn);
  TENZIR_ASSERT(*fn);
  return **fn;
}

auto registry::try_get(const entity_path& path) const -> const entity_def* {
  if (path.segments().size() != 1) {
    // TODO: We pretend here that only single-name paths exist.
    return nullptr;
  }
  auto it = defs_.find(path.segments()[0]);
  if (it == defs_.end()) {
    return nullptr;
  }
  return &it->second;
}

auto registry::get(const entity_path& path) const -> const entity_def& {
  auto result = try_get(path);
  TENZIR_ASSERT(result);
  return *result;
}

auto registry::operator_names() const -> std::vector<std::string_view> {
  // TODO: This cannot stay this way, but for now we use it in error messages.
  auto result = std::vector<std::string_view>{};
  for (auto& [name, def] : defs_) {
    if (std::holds_alternative<const operator_factory_plugin*>(def)) {
      result.push_back(name);
    }
  }
  std::ranges::sort(result);
  return result;
}

auto registry::function_names() const -> std::vector<std::string_view> {
  auto result = std::vector<std::string_view>{};
  for (auto& [name, def] : defs_) {
    if (auto function = std::get_if<const function_plugin*>(&def)) {
      if (not dynamic_cast<const method_plugin*>(*function)) {
        result.push_back(name);
      }
    }
  }
  std::ranges::sort(result);
  return result;
}

auto registry::method_names() const -> std::vector<std::string_view> {
  auto result = std::vector<std::string_view>{};
  for (auto& [name, def] : defs_) {
    if (auto function = std::get_if<const function_plugin*>(&def)) {
      if (dynamic_cast<const method_plugin*>(*function)) {
        result.push_back(name);
      }
    }
  }
  std::ranges::sort(result);
  return result;
}

void registry::add(std::string name, entity_def def) {
  auto inserted = defs_.emplace(std::move(name), def).second;
  TENZIR_ASSERT(inserted);
}
} // namespace tenzir
