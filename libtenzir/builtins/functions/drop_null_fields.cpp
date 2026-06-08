//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/drop_null_fields.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/series.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/type.hpp>

#include <arrow/api.h>

namespace tenzir::plugins::drop_null_fields_function {

namespace {

/// Builds a `field_path` that names the wrapper field plus the original path.
auto prefix_with_wrapper(const ast::field_path& original,
                         std::string_view wrapper_name)
  -> std::optional<ast::field_path> {
  ast::expression expr = ast::root_field{
    ast::identifier{std::string{wrapper_name}, location::unknown}, false};
  for (const auto& seg : original.path()) {
    expr = ast::expression{ast::field_access{std::move(expr), location::unknown,
                                             seg.has_question_mark, seg.id}};
  }
  return ast::field_path::try_from(std::move(expr));
}

/// Wraps a record-valued series as a single-column table_slice so the shared
/// `drop_null_fields` implementation can operate on it.
auto wrap_as_slice(series value, std::string_view wrapper_name) -> table_slice {
  auto wrapper_field_type = type{value.type};
  auto wrapper_record_type
    = record_type{{std::string{wrapper_name}, wrapper_field_type}};
  auto wrapper_type = type{wrapper_record_type};
  auto arrow_schema = arrow::schema(
    {wrapper_field_type.to_arrow_field(std::string{wrapper_name})});
  // Capture the row count before moving `value.array`, because argument
  // evaluation order is unspecified and `series::length()` returns 0 once
  // the array shared_ptr has been moved from.
  const auto length = value.length();
  auto batch = arrow::RecordBatch::Make(
    std::move(arrow_schema), length,
    std::vector<std::shared_ptr<arrow::Array>>{std::move(value.array)});
  return table_slice{std::move(batch), std::move(wrapper_type)};
}

/// Projects the wrapper field out of a slice produced by `drop_null_fields`.
/// Returns null series with the original record type when the wrapper itself
/// was dropped (the input record was null in the corresponding rows).
auto unwrap_slice(const table_slice& slice, const type& original_record_type)
  -> series {
  const auto& schema = slice.schema();
  const auto* record = try_as<record_type>(schema);
  if (not record or record->num_fields() == 0) {
    return series::null(original_record_type, slice.rows());
  }
  auto batch = to_record_batch(slice);
  auto column = batch->column(0);
  auto column_type = record->field(0).type;
  return series{std::move(column_type), std::move(column)};
}

class drop_null_fields_function final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "drop_null_fields";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    const auto& args = inv.call.args;
    if (args.empty()) {
      diagnostic::error("`drop_null_fields` expects at least one argument")
        .primary(inv.call.fn)
        .emit(ctx);
      return failure::promise();
    }
    auto record_expr = args[0];
    auto selectors = std::vector<ast::field_path>{};
    selectors.reserve(args.size() - 1);
    for (auto i = 1uz; i < args.size(); ++i) {
      auto selector = ast::field_path::try_from(args[i]);
      if (not selector) {
        diagnostic::error("expected simple selector").primary(args[i]).emit(ctx);
        return failure::promise();
      }
      if (selector->has_this()) {
        diagnostic::error("cannot drop `this`").primary(*selector).emit(ctx);
        return failure::promise();
      }
      selectors.push_back(std::move(*selector));
    }
    return function_use::make([record_expr = std::move(record_expr),
                               selectors = std::move(selectors)](
                                evaluator eval, session ctx) -> multi_series {
      return map_series(eval(record_expr), [&](series value) -> multi_series {
        if (value.length() == 0) {
          return multi_series{std::move(value)};
        }
        if (is<null_type>(value.type)) {
          return multi_series{std::move(value)};
        }
        if (not is<record_type>(value.type)) {
          diagnostic::warning("expected `record`, got `{}`", value.type.kind())
            .primary(record_expr)
            .emit(ctx);
          return multi_series{series::null(null_type{}, value.length())};
        }
        constexpr auto wrapper_name = std::string_view{"_"};
        auto original_record_type = value.type;
        auto prefixed_selectors = std::vector<ast::field_path>{};
        prefixed_selectors.reserve(selectors.size());
        for (const auto& sel : selectors) {
          if (auto fp = prefix_with_wrapper(sel, wrapper_name)) {
            prefixed_selectors.push_back(std::move(*fp));
          }
        }
        auto slice = wrap_as_slice(std::move(value), wrapper_name);
        auto parts = tenzir::drop_null_fields(
          std::move(slice), prefixed_selectors, event_order::ordered, ctx.dh());
        auto result = multi_series{};
        for (const auto& part : parts) {
          if (part.rows() == 0) {
            continue;
          }
          result.append(unwrap_slice(part, original_record_type));
        }
        return result;
      });
    });
  }
};

} // namespace

} // namespace tenzir::plugins::drop_null_fields_function

TENZIR_REGISTER_PLUGIN(
  tenzir::plugins::drop_null_fields_function::drop_null_fields_function)
