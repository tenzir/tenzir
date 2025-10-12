//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/registry.hpp"

#include "tenzir/concept/parseable/tenzir/yaml.hpp"
#include "tenzir/data.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/tql2/exec.hpp"
#include "tenzir/tql2/parser.hpp"

#include <cstdlib>
#include <memory>
#include <ranges>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

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

// AST visitor to substitute parameter references with argument expressions
class parameter_substituter : public ast::visitor<parameter_substituter> {
public:
  parameter_substituter(
    std::unordered_map<std::string, ast::expression> substitutions)
    : substitutions_(std::move(substitutions)) {
  }

  void visit(ast::expression& expr) {
    // Check if this is a dollar variable that should be substituted
    auto* dollar_var = std::get_if<ast::dollar_var>(&*expr.kind);
    if (dollar_var) {
      auto name = std::string{dollar_var->name_without_dollar()};
      // Only substitute if: 1) we have a substitution for it, 2) it's not
      // shadowed
      if (substitutions_.contains(name) && ! shadowed_.contains(name)) {
        // Replace the entire expression with the substitution
        expr = substitutions_[name];
        return;
      }
    }
    // Continue visiting children
    enter(expr);
  }

  void visit(ast::let_stmt& stmt) {
    // Visit the RHS expression first (before the variable is bound)
    visit(stmt.expr);

    // Check if this let statement shadows a parameter
    auto name = std::string{stmt.name_without_dollar()};
    if (substitutions_.contains(name)) {
      shadowed_.insert(name);
    }
  }

  void visit(ast::pipeline& pipe) {
    // Visit each statement, tracking scope
    for (auto& stmt : pipe.body) {
      visit(stmt);
    }
  }

  template <class T>
  void visit(T& x) {
    enter(x);
  }

private:
  std::unordered_map<std::string, ast::expression> substitutions_;
  std::unordered_set<std::string> shadowed_;
};

} // namespace

auto operator_def::make(operator_factory_plugin::invocation inv,
                        session ctx) const -> failure_or<operator_ptr> {
  return match(
    kind_,
    [&](const user_defined_operator& udo) -> failure_or<operator_ptr> {
      // If there are no parameters defined, check that no arguments were provided
      if (udo.positional_params.empty() && udo.named_params.empty()) {
        if (not inv.args.empty()) {
          diagnostic::error("user-defined operator does not support arguments")
            .primary(inv.self)
            .emit(ctx);
          return failure::promise();
        }
        TRY(auto compiled, compile(ast::pipeline{udo.definition}, ctx));
        return std::make_unique<pipeline>(std::move(compiled));
      }

      // Parse arguments using argument_parser2
      auto parser = argument_parser2::operator_("user_defined_operator");

      // Storage for parsed argument values
      std::vector<ast::expression> positional_values;
      positional_values.reserve(udo.positional_params.size());
      std::vector<std::pair<std::string, ast::expression>> named_values;

      // Register positional parameters
      for (size_t i = 0; i < udo.positional_params.size(); ++i) {
        positional_values.emplace_back();
        // Pass empty string for type - we use ast::expression which doesn't
        // const_eval The actual type from frontmatter is just for documentation
        parser.positional(udo.positional_params[i].name, positional_values[i],
                          "");
      }

      // Register named parameters
      // Storage for optional named parameter values
      std::vector<std::optional<ast::expression>> optional_values;
      optional_values.reserve(udo.named_params.size());

      for (size_t i = 0; i < udo.named_params.size(); ++i) {
        const auto& param = udo.named_params[i];
        if (param.required) {
          named_values.emplace_back(param.name, ast::expression{});
          // Use the type from frontmatter for documentation/display purposes
          parser.named(param.name, named_values.back().second, param.type);
          optional_values.emplace_back(std::nullopt); // Placeholder
        } else {
          optional_values.emplace_back(std::nullopt);
          // Use the type from frontmatter for documentation/display purposes
          parser.named(param.name, optional_values.back(), param.type);
        }
      }

      // Parse the invocation
      TRY(parser.parse(inv, ctx));

      // Process optional parameters - add provided values or use defaults
      std::vector<size_t> params_needing_defaults;
      for (size_t i = 0; i < udo.named_params.size(); ++i) {
        const auto& param = udo.named_params[i];
        if (param.required) {
          continue; // Already added to named_values above
        }
        auto& opt_value = optional_values[i];
        if (opt_value) {
          named_values.emplace_back(param.name, std::move(*opt_value));
        } else if (param.default_value) {
          // Mark this parameter as needing its default value parsed
          params_needing_defaults.push_back(i);
        }
      }

      // Parse default values for parameters that weren't provided
      for (size_t param_idx : params_needing_defaults) {
        const auto& param = udo.named_params[param_idx];
        // Parse the YAML default value string as data
        auto yaml_data = from_yaml(*param.default_value);
        if (not yaml_data) {
          diagnostic::error("failed to parse default value for parameter '{}'",
                            param.name)
            .note("default value: {}", *param.default_value)
            .note("type: {}", param.type)
            .note("error: {}", yaml_data.error())
            .primary(inv.self)
            .emit(ctx);
          return failure::promise();
        }
        // Convert data to constant expression
        auto constant_value = match(
          std::move(*yaml_data),
          [](auto x) -> ast::constant::kind {
            return x;
          },
          [](const pattern&) -> ast::constant::kind {
            TENZIR_UNREACHABLE();
          });
        auto default_expr = ast::expression{
          ast::constant{std::move(constant_value), location::unknown}};
        named_values.emplace_back(param.name, std::move(default_expr));
      }

      // Build substitution map: parameter name -> argument expression
      auto substitutions = std::unordered_map<std::string, ast::expression>{};

      // Add positional parameters
      for (size_t i = 0; i < positional_values.size(); ++i) {
        substitutions[udo.positional_params[i].name]
          = std::move(positional_values[i]);
      }

      // Add named parameters
      for (auto& [param_name, param_value] : named_values) {
        substitutions[param_name] = std::move(param_value);
      }

      // Substitute parameter references in the UDO pipeline
      auto modified_pipeline = udo.definition;
      auto substituter = parameter_substituter{std::move(substitutions)};
      substituter.visit(modified_pipeline);

      // Compile and return
      TRY(auto compiled, compile(std::move(modified_pipeline), ctx));
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
