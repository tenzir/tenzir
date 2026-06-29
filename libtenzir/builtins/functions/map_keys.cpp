//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_memory_pool.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/series.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/type.hpp>

#include <arrow/api.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir::plugins::map_keys_function {

namespace {

auto map_names(record_type const& r, ast::lambda_expr const& fn, session ctx)
  -> std::optional<std::vector<std::string>> {
  auto keys_builder = arrow::StringBuilder{tenzir::arrow_memory_pool()};
  check(keys_builder.Reserve(detail::narrow<int64_t>(r.num_fields())));
  auto names = std::vector<std::string>{};
  auto invalid = std::vector<std::string_view>{};
  auto conflicts = std::vector<std::string_view>{};
  for (auto const& field : r.fields()) {
    check(keys_builder.Append(field.name));
    names.emplace_back(field.name);
  }
  auto mapped = tenzir::eval(
    fn, multi_series{series{string_type{}, finish(keys_builder)}}, ctx);
  TENZIR_ASSERT_EQ(mapped.length(), detail::narrow<int64_t>(r.num_fields()));
  auto i = size_t{0};
  for (auto const& part : mapped) {
    auto strings = part.as<string_type>();
    auto* array = strings ? strings->array.get() : nullptr;
    for (auto j = int64_t{0}; j < part.length(); ++j, ++i) {
      if (array and not array->IsNull(j)) {
        names[i] = array->GetView(j);
      } else {
        invalid.emplace_back(r.field(i).name);
      }
    }
  }
  for (auto i = size_t{0}; i < names.size(); ++i) {
    if (std::ranges::count(names, names[i]) > 1) {
      conflicts.emplace_back(r.field(i).name);
    }
  }
  if (not invalid.empty()) {
    diagnostic::warning("kept fields with non-string results")
      .primary(fn.body)
      .note("kept fields: `{}`", fmt::join(invalid, "`, `"))
      .emit(ctx);
  }
  if (not conflicts.empty()) {
    diagnostic::warning("skipped record with conflicting field names")
      .primary(fn.body)
      .note("conflicting fields: `{}`", fmt::join(conflicts, "`, `"))
      .emit(ctx);
    return std::nullopt;
  }
  return names;
}

class map_keys final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "map_keys";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto function = ast::lambda_expr{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "record")
          .positional("function", function, "string => string")
          .parse(inv, ctx));
    if (not function.is_unary()) {
      diagnostic::error("expected unary lambda")
        .primary(function)
        .hint("provide `key => ...`")
        .emit(ctx);
      return failure::promise();
    }
    return function_use::make(
      [expr = std::move(expr), function = std::move(function)](
        evaluator eval, session ctx) -> multi_series {
        return map_series(eval(expr), [&](series subject) -> series {
          if (is<null_type>(subject.type)) {
            return subject;
          }
          const auto* input_type = try_as<record_type>(&subject.type);
          if (not input_type) {
            diagnostic::warning("expected `record`, got `{}`",
                                subject.type.kind())
              .primary(expr)
              .emit(ctx);
            return series::null(null_type{}, subject.length());
          }
          auto names = map_names(*input_type, function, ctx);
          if (not names) {
            return subject;
          }
          auto fields = std::vector<record_type::field_view>{};
          fields.reserve(input_type->num_fields());
          for (auto [name, field] :
               std::views::zip(*names, input_type->fields())) {
            fields.emplace_back(name, field.type);
          }
          auto result_type = type{record_type{fields}};
          auto result_data = subject.array->data()->Copy();
          result_data->type = result_type.to_arrow_type();
          auto result_array = arrow::MakeArray(std::move(result_data));
          return series{std::move(result_type), std::move(result_array)};
        });
      });
  }
};

} // namespace

} // namespace tenzir::plugins::map_keys_function

TENZIR_REGISTER_PLUGIN(tenzir::plugins::map_keys_function::map_keys)
