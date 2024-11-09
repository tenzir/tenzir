//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/registry.hpp"

#include "tenzir/plugin.hpp"

#include <ranges>

namespace tenzir {

namespace {

void gather_names(const module_def& mod, entity_ns ns, std::string prefix,
                  std::vector<std::string>& out) {
  auto result = std::vector<std::string>{};
  for (auto& [name, def] : mod.defs) {
    if (ns == entity_ns::fn and def.fn) {
      out.push_back(prefix + name);
    } else if (ns == entity_ns::op and def.op) {
      out.push_back(prefix + name);
    }
    if (def.mod) {
      gather_names(*def.mod, ns, prefix + name + "::", out);
    }
  }
}

auto gather_names(const module_def& mod, entity_ns ns)
  -> std::vector<std::string> {
  auto result = std::vector<std::string>{};
  gather_names(mod, ns, "", result);
  std::ranges::sort(result);
  return result;
}

} // namespace

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
  -> variant<entity_def, error> {
  TENZIR_ASSERT(path.resolved());
  auto current = &root_;
  auto&& segments = path.segments();
  for (auto i = size_t{0}; i < segments.size(); ++i) {
    auto it = current->defs.find(segments[i]);
    if (it == current->defs.end()) {
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

auto registry::get(const entity_path& path) const -> entity_def {
  auto result = try_get(path);
  TENZIR_ASSERT(std::holds_alternative<entity_def>(result));
  return std::get<entity_def>(result);
}

auto registry::operator_names() const -> std::vector<std::string> {
  return gather_names(root_, entity_ns::op);
}

auto registry::function_names() const -> std::vector<std::string> {
  return gather_names(root_, entity_ns::fn);
}

void registry::add(std::string name, entity_def def) {
  TENZIR_ASSERT(not name.empty());
  auto path = detail::split(name, "::");
  TENZIR_ASSERT(not path.empty());
  // Find the correct module first.
  auto mod = &root_;
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
  def.match(
    [&](std::reference_wrapper<const function_plugin> plugin) {
      TENZIR_ASSERT(not set.fn);
      set.fn = &plugin.get();
    },
    [&](std::reference_wrapper<const operator_factory_plugin> plugin) {
      TENZIR_ASSERT(not set.op);
      set.op = &plugin.get();
    },
    [&](std::reference_wrapper<const module_def>) {
      // Does this make sense?
      TENZIR_TODO();
    });
}

} // namespace tenzir
