//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/user_defined_operator.hpp"

#include "tenzir/detail/similarity.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval.hpp"

namespace tenzir {

auto parameter_type_label(const user_defined_operator::parameter& param)
  -> std::string {
  if (not param.type_hint.empty()) {
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
  if (entity.path.empty()) {
    return std::string{"<unknown operator>"};
  }
  auto names_range
    = entity.path | std::views::transform(&ast::identifier::name);
  return fmt::format("{}", fmt::join(names_range, "::"));
}

namespace {

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

} // namespace

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
  auto make_rows = [](const auto& params) {
    auto rows = std::vector<row>{};
    rows.reserve(params.size());
    for (const auto& param : params) {
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
    }
    return rows;
  };
  auto update_widths = [](const std::vector<row>& rows, size_t& name_width,
                          size_t& type_width, size_t& default_width) {
    for (const auto& row : rows) {
      name_width = std::max(name_width, row.name.size());
      type_width = std::max(type_width, row.type.size());
      default_width = std::max(default_width, row.default_value.size());
    }
  };
  auto format_section
    = [](std::string_view heading, const std::vector<row>& rows,
         size_t name_width, size_t type_width,
         size_t default_width) -> std::string {
    if (rows.empty()) {
      return {};
    }
    auto section = fmt::format("  {}:\n", heading);
    section += fmt::format("    {:{}}  {:{}}  {:{}}  {}\n", "name", name_width,
                           "type", type_width, "default", default_width,
                           "description");
    for (const auto& row : rows) {
      section
        += fmt::format("    {:{}}  {:{}}  {:{}}  {}\n", row.name, name_width,
                       row.type, type_width, row.default_value, default_width,
                       row.description_lines.front());
      for (size_t i = 1; i < row.description_lines.size(); ++i) {
        section += fmt::format("    {:{}}  {:{}}  {:{}}  {}\n", "", name_width,
                               "", type_width, "", default_width,
                               row.description_lines[i]);
      }
    }
    return section;
  };
  auto positional_rows = make_rows(udo.positional_params);
  auto named_rows = make_rows(udo.named_params);
  auto name_width = size_t{4};
  auto type_width = size_t{4};
  auto default_width = size_t{7};
  update_widths(positional_rows, name_width, type_width, default_width);
  update_widths(named_rows, name_width, type_width, default_width);
  if (positional_rows.empty() && named_rows.empty()) {
    return std::nullopt;
  }
  auto note = std::string{"parameters:\n"};
  note += format_section("positional", positional_rows, name_width, type_width,
                         default_width);
  note += format_section("named", named_rows, name_width, type_width,
                         default_width);
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

auto instantiate_user_defined_operator(const user_defined_operator& udo,
                                       operator_factory_plugin::invocation& inv,
                                       session ctx, udo_failure_handler& fail)
  -> failure_or<ast::pipeline> {
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
  auto append_positional_argument
    = [&](const user_defined_operator::parameter& param)
    -> std::optional<failure_or<ast::pipeline>> {
    const auto missing_argument
      = next_arg >= inv.args.size()
        || try_as<ast::assignment>(inv.args[next_arg]);
    if (! missing_argument) {
      positional_values.push_back(std::move(inv.args[next_arg]));
      ++next_arg;
      return std::nullopt;
    }
    if (! param.default_value) {
      return fail(
        diagnostic::error("expected additional positional argument `{}`",
                          param.name)
          .primary(inv.self));
    }
    positional_values.push_back(*param.default_value);
    return std::nullopt;
  };
  for (const auto& positional_param : udo.positional_params) {
    if (auto error = append_positional_argument(positional_param)) {
      return *error;
    }
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
    if ((left == nullptr) || left->has_this() || left->path().size() != 1
        || left->path()[0].has_question_mark) {
      return fail(
        diagnostic::error("invalid argument name").primary(assignment->left));
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
          auto score
            = detail::calculate_similarity(name, std::string_view{candidate});
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
        return fail(diagnostic::error("required argument `{}` was not provided",
                                      param.name)
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

  auto validate_type = [&](const user_defined_operator::parameter& param,
                           const ast::expression& expr,
                           std::optional<location> explicit_location)
    // TODO:
    //-> failure_or<void>
    -> std::optional<failure_or<ast::pipeline>> {
    if (! param.value_type) {
      return std::nullopt;
    }
    // FIXME: Add a test that checks that `null` will be accepted by the type
    // check, no matter the type in the operator signature.
    auto diag_loc = explicit_location.value_or(expr.get_location());
    if (auto value = try_const_eval(expr, ctx)) {
      if (type_check(*param.value_type, *value)) {
        return std::nullopt;
      }
      auto actual_type = type::infer(*value);
      auto actual_str
        = actual_type ? fmt::format("{}", *actual_type) : std::string{"unknown"};
      return fail(diagnostic::error(
                    "argument `{}` must be of type `{}` (got `{}`)", param.name,
                    fmt::format("{}", *param.value_type), actual_str)
                    .primary(diag_loc));
    }
    return {};
  };

  // FIXME: Add tests for named before positional, should behave exactly like
  // builtins
  for (size_t i = 0; i < udo.positional_params.size(); ++i) {
    auto& expr = positional_values[i];
    const auto& param = udo.positional_params[i];
    if (param.kind == user_defined_operator::parameter_kind::field_path) {
      auto copy = ast::expression{expr};
      if (! ast::field_path::try_from(std::move(copy))) {
        return fail(diagnostic::error("expected a selector").primary(expr));
      }
    }
    if (auto result = validate_type(param, expr, std::nullopt)) {
      return *result;
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
      if (! ast::selector::try_from(std::move(copy))) {
        if (named_value_locations[i]) {
          return fail(diagnostic::error("expected a selector")
                        .primary(*named_value_locations[i]));
        }
        return fail(diagnostic::error("expected a selector").primary(inv.self));
      }
    }
    if (auto result = validate_type(param, value, named_value_locations[i])) {
      return *result;
    }
    substitutions.emplace(param.name, std::move(value));
  }
  // We intentionally don't support pipelines that are passed to udos yet.

  auto modified_pipeline = udo.definition;
  return ast::substitute_named_expressions(std::move(modified_pipeline),
                                           substitutions, dh);
}

} // namespace tenzir
