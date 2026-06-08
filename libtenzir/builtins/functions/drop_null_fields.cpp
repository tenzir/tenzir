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

/// Treats a record-valued series as a table_slice whose top-level columns are
/// the record's fields, so the shared `drop_null_fields` implementation can
/// operate on it directly without an extra wrapper field.
auto record_series_as_slice(series value) -> table_slice {
  auto input_type = value.type;
  const auto length = value.length();
  auto struct_array
    = std::static_pointer_cast<arrow::StructArray>(std::move(value.array));
  auto arrow_schema = arrow::schema(struct_array->type()->fields());
  auto batch = arrow::RecordBatch::Make(std::move(arrow_schema), length,
                                        struct_array->fields());
  return table_slice{std::move(batch), std::move(input_type)};
}

/// Reconstructs a record-valued series from a slice produced by
/// `drop_null_fields`. Empty-schema slices map to a null series carrying the
/// original record type so the caller's row count is preserved.
auto slice_as_record_series(const table_slice& slice,
                            const type& original_record_type) -> series {
  if (slice.rows() == 0) {
    return series::null(original_record_type, 0);
  }
  const auto* record = try_as<record_type>(slice.schema());
  if (not record or record->num_fields() == 0) {
    return series::null(original_record_type, slice.rows());
  }
  auto batch = to_record_batch(slice);
  auto struct_array = check(batch->ToStructArray());
  return series{slice.schema(), std::move(struct_array)};
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
        const auto input_length = value.length();
        if (input_length == 0) {
          return multi_series{std::move(value)};
        }
        if (is<null_type>(value.type)) {
          return multi_series{std::move(value)};
        }
        if (not is<record_type>(value.type)) {
          diagnostic::warning("expected `record`, got `{}`", value.type.kind())
            .primary(record_expr)
            .emit(ctx);
          return multi_series{series::null(null_type{}, input_length)};
        }
        auto input_type = value.type;
        auto slice = record_series_as_slice(std::move(value));
        auto parts = tenzir::drop_null_fields(std::move(slice), selectors,
                                              event_order::ordered, ctx.dh());
        auto result = multi_series{};
        auto produced = int64_t{0};
        for (const auto& part : parts) {
          auto produced_part = slice_as_record_series(part, input_type);
          produced += produced_part.length();
          result.append(std::move(produced_part));
        }
        // The shared implementation can return zero rows when every field of
        // a row gets dropped (the empty-record case). Pad with null rows of
        // the original record type so the function preserves the input row
        // count, as required by `map_series`.
        if (produced < input_length) {
          result.append(series::null(input_type, input_length - produced));
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
