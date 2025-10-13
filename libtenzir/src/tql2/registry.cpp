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
#include "tenzir/tql2/exec.hpp"
#include "tenzir/tql2/parser.hpp"

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

auto parameter_type_label(const user_defined_operator::parameter& param)
  -> std::string {
  if (! param.type_hint.empty()) {
    return param.type_hint;
  }
  switch (param.kind) {
    case user_defined_operator::parameter_kind::expression:
      return "any";
    case user_defined_operator::parameter_kind::field_path:
      return "field";
  }
  TENZIR_UNREACHABLE();
}

auto make_operator_name(const ast::entity& entity) -> std::string {
  auto parts = std::vector<std::string>{};
  parts.reserve(entity.path.size());
  for (const auto& segment : entity.path) {
    parts.push_back(segment.name);
  }
  if (parts.empty()) {
    return std::string{"<unknown operator>"};
  }
  return fmt::format("{}", fmt::join(parts, "::"));
}

auto parameter_default_string(const ast::expression& expr)
  -> std::optional<std::string> {
  if (const auto* constant = try_as<ast::constant>(expr)) {
    auto data_value = constant->as_data();
    if (auto yaml = to_yaml(data_value)) {
      return *yaml;
    }
  }
  return std::nullopt;
}

auto make_usage_string(std::string_view op_name,
                       const user_defined_operator& udo) -> std::string {
  auto usage = std::string{op_name};
  auto has_parameters
    = ! udo.positional_params.empty() || ! udo.named_params.empty();
  if (! has_parameters) {
    return usage;
  }
  usage += ' ';
  auto has_previous = false;
  auto in_brackets = false;
  auto append_positional = [&](const user_defined_operator::parameter& param) {
    if (std::exchange(has_previous, true)) {
      usage += ", ";
    }
    usage += fmt::format("{}:{}", param.name, parameter_type_label(param));
  };
  for (const auto& param : udo.positional_params) {
    append_positional(param);
  }
  auto append_named = [&](const user_defined_operator::parameter& param) {
    if (param.name.starts_with('_')) {
      return;
    }
    if (param.required && in_brackets) {
      usage += ']';
      in_brackets = false;
    }
    if (std::exchange(has_previous, true)) {
      usage += ", ";
    }
    if (! param.required && ! in_brackets) {
      usage += '[';
      in_brackets = true;
    }
    usage += fmt::format("{}={}", param.name, parameter_type_label(param));
  };
  for (const auto& param : udo.named_params) {
    if (param.required) {
      append_named(param);
    }
  }
  for (const auto& param : udo.named_params) {
    if (! param.required) {
      append_named(param);
    }
  }
  if (in_brackets) {
    usage += ']';
  }
  return usage;
}

auto make_parameter_note(const user_defined_operator& udo)
  -> std::optional<std::string> {
  struct row {
    std::string name;
    std::string type;
    std::string default_value;
    std::vector<std::string> description_lines;
  };
  auto rows = std::vector<row>{};
  rows.reserve(udo.positional_params.size() + udo.named_params.size());
  auto append_row = [&](const user_defined_operator::parameter& param) {
    auto default_value = std::string{"-"};
    if (param.default_value) {
      if (auto default_str = parameter_default_string(*param.default_value)) {
        default_value = *default_str;
      } else {
        default_value = "<expr>";
      }
    }
    auto description_lines = std::vector<std::string>{};
    if (param.description && ! param.description->empty()) {
      auto remaining = std::string_view{*param.description};
      while (true) {
        auto pos = remaining.find('\n');
        if (pos == std::string_view::npos) {
          description_lines.emplace_back(remaining);
          break;
        }
        description_lines.emplace_back(remaining.substr(0, pos));
        remaining.remove_prefix(pos + 1);
      }
    }
    if (description_lines.empty()) {
      description_lines.emplace_back("-");
    }
    rows.push_back(row{
      .name = param.name,
      .type = parameter_type_label(param),
      .default_value = std::move(default_value),
      .description_lines = std::move(description_lines),
    });
  };
  for (const auto& param : udo.positional_params) {
    append_row(param);
  }
  for (const auto& param : udo.named_params) {
    append_row(param);
  }
  if (rows.empty()) {
    return std::nullopt;
  }
  auto name_width = size_t{4};
  auto type_width = size_t{4};
  auto default_width = size_t{7};
  for (const auto& row : rows) {
    name_width = std::max(name_width, row.name.size());
    type_width = std::max(type_width, row.type.size());
    default_width = std::max(default_width, row.default_value.size());
  }
  auto note = std::string{"parameters:\n"};
  note += fmt::format("  {:{}}  {:{}}  {:{}}  {}\n", "name", name_width,
                      "type", type_width, "default", default_width,
                      "description");
  for (const auto& row : rows) {
    note += fmt::format("  {:{}}  {:{}}  {:{}}  {}\n", row.name, name_width,
                        row.type, type_width, row.default_value, default_width,
                        row.description_lines.front());
    for (size_t i = 1; i < row.description_lines.size(); ++i) {
      note += fmt::format("  {:{}}  {:{}}  {:{}}  {}\n", "", name_width, "",
                          type_width, "", default_width,
                          row.description_lines[i]);
    }
  }
  if (! note.empty() && note.back() == '\n') {
    note.pop_back();
  }
  return note;
}

auto user_defined_operator_docs() -> const std::string& {
  static const auto docs
    = std::string{"https://docs.tenzir.com/reference/operators/"
                  "user_defined_operator"};
  return docs;
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
      auto fail = [&](diagnostic_builder d) -> failure_or<operator_ptr> {
        auto builder = std::move(d).usage(usage);
        if (parameter_note) {
          builder = std::move(builder).note(*parameter_note);
        }
        builder = std::move(builder).docs(docs);
        std::move(builder).emit(ctx);
        return failure::promise();
      };

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

      auto positional_values = std::vector<ast::expression>{};
      positional_values.reserve(udo.positional_params.size());

      auto named_values
        = std::vector<std::optional<ast::expression>>(udo.named_params.size());
      auto named_value_locations
        = std::vector<std::optional<location>>(udo.named_params.size());
      auto assignment_locations
        = std::vector<std::optional<location>>(udo.named_params.size());

      auto name_to_index = std::unordered_map<std::string, size_t>{};
      name_to_index.reserve(udo.named_params.size());
      auto suggestion_names = std::vector<std::string>{};
      suggestion_names.reserve(udo.named_params.size());
      for (size_t i = 0; i < udo.named_params.size(); ++i) {
        name_to_index.emplace(udo.named_params[i].name, i);
        if (! udo.named_params[i].name.starts_with('_')) {
          suggestion_names.push_back(udo.named_params[i].name);
        }
      }

      auto next_arg = size_t{0};
      for (size_t i = 0; i < udo.positional_params.size(); ++i) {
        if (next_arg >= inv.args.size()
            || try_as<ast::assignment>(inv.args[next_arg])) {
          return fail(
            diagnostic::error("expected additional positional argument `{}`",
                              udo.positional_params[i].name)
              .primary(inv.self));
        }
        positional_values.push_back(std::move(inv.args[next_arg]));
        ++next_arg;
      }

      for (; next_arg < inv.args.size(); ++next_arg) {
        auto& arg = inv.args[next_arg];
        auto* assignment = try_as<ast::assignment>(arg);
        if (! assignment) {
          return fail(
            diagnostic::error("did not expect more positional arguments")
              .primary(arg));
        }
        auto* left = try_as<ast::field_path>(assignment->left);
        if (! left || left->has_this() || left->path().size() != 1
            || left->path()[0].has_question_mark) {
          return fail(diagnostic::error("invalid argument name")
                        .primary(assignment->left));
        }
        auto name = std::string{left->path()[0].id.name};
        auto it = name_to_index.find(name);
        if (it == name_to_index.end()) {
          auto builder
            = diagnostic::error("named argument `{}` does not exist", name)
                .primary(assignment->left);
          if (! suggestion_names.empty()) {
            auto best = std::string_view{};
            auto best_score = std::numeric_limits<int64_t>::min();
            for (const auto& candidate : suggestion_names) {
              auto score = detail::calculate_similarity(
                name, std::string_view{candidate});
              if (score > best_score) {
                best = candidate;
                best_score = score;
              }
            }
            if (best_score > -10) {
              builder = std::move(builder).hint("did you mean `{}`?", best);
            }
          }
          return fail(std::move(builder));
        }
        auto idx = it->second;
        if (assignment_locations[idx]) {
          return fail(diagnostic::error("duplicate named argument `{}`", name)
                        .primary(*assignment_locations[idx])
                        .primary(assignment->get_location()));
        }
        assignment_locations[idx] = assignment->get_location();
        named_value_locations[idx] = assignment->right.get_location();
        named_values[idx] = std::move(assignment->right);
      }

      for (size_t i = 0; i < udo.named_params.size(); ++i) {
        const auto& param = udo.named_params[i];
        if (! named_values[i]) {
          if (param.required) {
            return fail(diagnostic::error(
                          "required argument `{}` was not provided", param.name)
                          .primary(inv.self));
          }
          if (param.default_value) {
            named_values[i] = *param.default_value;
          }
        }
      }

      auto substitutions = std::unordered_map<std::string, ast::expression>{};
      substitutions.reserve(udo.positional_params.size()
                            + udo.named_params.size());

      for (size_t i = 0; i < udo.positional_params.size(); ++i) {
        auto& expr = positional_values[i];
        const auto& param = udo.positional_params[i];
        if (param.kind == user_defined_operator::parameter_kind::field_path) {
          auto copy = ast::expression{expr};
          if (! ast::field_path::try_from(std::move(copy))) {
            return fail(diagnostic::error("expected a selector").primary(expr));
          }
        }
        substitutions.emplace(param.name, std::move(expr));
      }

      for (size_t i = 0; i < udo.named_params.size(); ++i) {
        const auto& param = udo.named_params[i];
        if (! named_values[i]) {
          continue;
        }
        auto value = std::move(*named_values[i]);
        if (param.kind == user_defined_operator::parameter_kind::field_path) {
          auto copy = ast::expression{value};
          if (! ast::field_path::try_from(std::move(copy))) {
            if (named_value_locations[i]) {
              return fail(diagnostic::error("expected a selector")
                            .primary(*named_value_locations[i]));
            }
            return fail(
              diagnostic::error("expected a selector").primary(inv.self));
          }
        }
        substitutions.emplace(param.name, std::move(value));
      }

      // Substitute parameter references in the UDO pipeline
      auto modified_pipeline = udo.definition;
      ast::substitute_named_expressions(modified_pipeline, substitutions);

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
