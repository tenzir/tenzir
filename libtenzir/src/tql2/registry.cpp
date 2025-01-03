//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/registry.hpp"

#include "tenzir/plugin.hpp"
#include "tenzir/tql2/exec.hpp"

#include <ranges>

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
    [&](const operator_factory_plugin& plugin) -> failure_or<operator_ptr> {
      return plugin.make(inv, ctx);
    });
}

thread_local const registry* g_thread_local_registry = nullptr;

auto thread_local_registry() -> const registry* {
  return g_thread_local_registry;
}

void set_thread_local_registry(const registry* reg) {
  g_thread_local_registry = reg;
}

auto global_registry_mut() -> registry& {
  static auto reg = std::invoke([] {
    auto reg = registry{};
    for (auto op : plugins::get<operator_factory_plugin>()) {
      auto name = op->name();
      // TODO: We prefixed some operators with "tql2." to prevent name clashes
      // with the legacy operators. We should get rid of this eventually.
      if (name.starts_with("tql2.")) {
        name.erase(0, 5);
      }
      reg.add(entity_pkg::std, name, *op);
    }
    for (auto fn : plugins::get<function_plugin>()) {
      auto name = fn->function_name();
      // TODO: Same here.
      if (name.starts_with("tql2.")) {
        name.erase(0, 5);
      }
      reg.add(entity_pkg::std, name, std::ref(*fn));
    }
    return reg;
  });
  return reg;
}

auto global_registry() -> const registry& {
  return global_registry_mut();
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
  // TODO: This does not really return the reachable entity names if the names
  // from `cfg_` shadow names from `std_`.
  auto result = std::vector<std::string>{};
  gather_names(std_, ns, "", result);
  gather_names(cfg_, ns, "", result);
  std::ranges::sort(result);
  return result;
}

void registry::add(entity_pkg package, std::string_view name, entity_def def) {
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
      TENZIR_ASSERT(not set.op);
      set.op = std::move(def);
    });
}

auto registry::root(entity_pkg package) -> module_def& {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
  return const_cast<module_def&>(std::as_const(*this).root(package));
}

auto registry::root(entity_pkg package) const -> const module_def& {
  return std::invoke([&]() -> const module_def& {
    switch (package) {
      case entity_pkg::std:
        return std_;
      case entity_pkg::cfg:
        return cfg_;
    }
    TENZIR_UNREACHABLE();
  });
}

} // namespace tenzir
