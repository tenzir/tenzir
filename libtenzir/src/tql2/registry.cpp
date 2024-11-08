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
      // TODO: We prefixed some operators with "tql2." to prevent name clashes
      // with the legacy operators. We should get rid of this eventually.
      if (name.starts_with("tql2.")) {
        name.erase(0, 5);
      }
      reg.add(name, *op);
    }
    for (auto fn : plugins::get<function_plugin>()) {
      auto name = fn->function_name();
      // TODO: Same here.
      if (name.starts_with("tql2.")) {
        name.erase(0, 5);
      }
      reg.add(name, *fn);
    }
    return reg;
  });
  return reg;
}

auto registry::get(const ast::function_call& call) const
  -> const function_plugin& {
  auto def = get(call.fn.ref);
  auto fn = std::get_if<std::reference_wrapper<const function_plugin>>(&def);
  TENZIR_ASSERT(fn);
  return *fn;
}

auto registry::get(const ast::invocation& call) const
  -> const operator_factory_plugin& {
  auto def = get(call.op.ref);
  auto op
    = std::get_if<std::reference_wrapper<const operator_factory_plugin>>(&def);
  TENZIR_ASSERT(op);
  return *op;
}

auto registry::try_get(const entity_path& path) const
  -> std::optional<entity_def> {
  TENZIR_ASSERT(path.resolved());
  if (path.segments().size() != 1) {
    // TODO: We pretend here that only single-name paths exist.
    return std::nullopt;
  }
  auto it = defs_.find(path.segments()[0]);
  if (it == defs_.end()) {
    return std::nullopt;
  }
  auto& set = it->second;
  switch (path.ns()) {
    case entity_ns::fn:
      if (set.fn) {
        return *set.fn;
      }
      return std::nullopt;
    case entity_ns::op:
      if (set.op) {
        return *set.op;
      }
      return std::nullopt;
  }
  TENZIR_UNREACHABLE();
}

auto registry::get(const entity_path& path) const -> entity_def {
  auto result = try_get(path);
  TENZIR_ASSERT(result);
  return *result;
}

auto registry::operator_names() const -> std::vector<std::string_view> {
  // TODO: This cannot stay this way, but for now we use it in error messages.
  auto result = std::vector<std::string_view>{};
  for (auto& [name, def] : defs_) {
    if (def.op) {
      result.push_back(name);
    }
  }
  std::ranges::sort(result);
  return result;
}

auto registry::function_names() const -> std::vector<std::string_view> {
  auto result = std::vector<std::string_view>{};
  for (auto& [name, def] : defs_) {
    if (def.fn) {
      result.push_back(name);
    }
  }
  std::ranges::sort(result);
  return result;
}

void registry::add(std::string name, entity_def def) {
  auto& set = defs_[std::move(name)];
  def.match(
    [&](std::reference_wrapper<const function_plugin> plugin) {
      TENZIR_ASSERT(not set.fn);
      set.fn = &plugin.get();
    },
    [&](std::reference_wrapper<const operator_factory_plugin> plugin) {
      TENZIR_ASSERT(not set.op);
      set.op = &plugin.get();
    });
}

} // namespace tenzir
