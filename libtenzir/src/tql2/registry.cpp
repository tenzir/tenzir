//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/registry.hpp"

#include "tenzir/concept/parseable/tenzir/yaml.hpp"
#include "tenzir/detail/similarity.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/exec.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/user_defined_operator.hpp"

#include <algorithm>
#include <cstdlib>
#include <limits>
#include <memory>
#include <ranges>
#include <shared_mutex>
#include <unordered_map>
#include <utility>
#include <vector>

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
      auto op_name = make_operator_name(inv.self);
      auto usage = make_usage_string(op_name, udo);
      const auto& docs = user_defined_operator_docs();
      auto parameter_note = make_parameter_note(udo);
      auto emit_failure = [&](diagnostic_builder d) {
        auto builder = std::move(d).usage(usage);
        if (parameter_note) {
          builder = std::move(builder).note(*parameter_note);
        }
        builder = std::move(builder).docs(docs);
        std::move(builder).emit(ctx);
      };
      auto fail = [&](diagnostic_builder d) -> failure_or<operator_ptr> {
        emit_failure(std::move(d));
        return failure::promise();
      };
      auto fail_ast = udo_failure_handler{
        [&](diagnostic_builder d) -> failure_or<ast::pipeline> {
          emit_failure(std::move(d));
          return failure::promise();
        }};

      // If there are no parameters defined, check that no arguments were provided
      if (udo.positional_params.empty() && udo.named_params.empty()) {
        if (! inv.args.empty()) {
          return fail(diagnostic::error(
                        "operator '{}' does not support arguments", op_name)
                        .primary(inv.self));
        }
        TRY(auto compiled, compile(ast::pipeline{udo.definition}, ctx));
        return std::make_unique<pipeline>(std::move(compiled));
      }

      auto instantiated
        = instantiate_user_defined_operator(udo, inv, ctx, fail_ast);
      if (! instantiated) {
        return failure::promise();
      }
      TRY(auto compiled, compile(std::move(*instantiated), ctx));
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

namespace {

auto global_registry_ref() -> std::shared_ptr<const registry>& {
  static auto* reg = std::invoke([&] -> std::shared_ptr<const registry>* {
    auto init = std::make_shared<registry>();
    for (const auto* op : plugins::get<operator_factory_plugin>()) {
      auto name = op->name();
      if (name.starts_with("tql2.")) {
        name.erase(0, 5);
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
      init->add(std::string{entity_pkg_std}, name, std::ref(*fn));
    }
    // Leak this on purpose to prevent static destruction order fiasco.
    return new std::shared_ptr<const registry>{std::move(init)}; // NOLINT
  });
  return *reg;
}

auto global_registry_mutex() -> std::shared_mutex& {
  // Leak this on purpose to prevent static destruction order fiasco.
  static auto* mtx = new std::shared_mutex{}; // NOLINT
  return *mtx;
}

} // namespace

auto global_registry() -> std::shared_ptr<const registry> {
  auto lock = std::shared_lock{global_registry_mutex()};
  auto& reg = global_registry_ref();
  return reg;
}

auto begin_registry_update() -> registry_update_guard {
  return registry_update_guard{std::unique_lock{global_registry_mutex()}};
}

auto registry_update_guard::current() const -> std::shared_ptr<const registry> {
  // Go through the private accessor because we already hold a lock.
  return global_registry_ref();
}

void registry_update_guard::publish(
  std::shared_ptr<const registry>&& next) const {
  global_registry_ref() = std::move(next);
}

auto registry::get(const ast::function_call& call) const
  -> const function_plugin& {
  auto def = get(call.fn.ref);
  auto* fn = std::get_if<std::reference_wrapper<const function_plugin>>(&def);
  TENZIR_ASSERT(fn);
  return *fn;
}

auto registry::get(const ast::invocation& inv) const -> const operator_def& {
  auto def = get(inv.op.ref);
  auto* op = std::get_if<std::reference_wrapper<const operator_def>>(&def);
  TENZIR_ASSERT(op);
  return *op;
}

auto registry::try_get(const entity_path& path) const
  -> variant<entity_ref, error> {
  TENZIR_ASSERT(path.resolved());
  const auto* current = &root(path.pkg());
  auto&& segments = path.segments();
  for (auto i = size_t{0}; i < segments.size(); ++i) {
    auto it = current->defs.find(segments[i]);
    if (it == current->defs.end()) {
      if (i == 0) {
        TENZIR_DEBUG("registry.try_get: root '{}' missing first segment '{}' "
                     "in ns {}. entries=[{}]",
                     path.pkg(), segments[i], path.ns(),
                     fmt::join(std::views::keys(current->defs), ", "));
      }
      // No such entity.
      return error{i, false};
    }
    const auto& set = it->second;
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
  auto* mod = &root(package);
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
      auto* existing = try_as<native_operator>(set.op->inner());
      TENZIR_ASSERT(existing);
      auto* incoming = try_as<native_operator>(def.inner());
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
