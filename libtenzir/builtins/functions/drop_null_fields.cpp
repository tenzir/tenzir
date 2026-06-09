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

/// Wraps a non-null run of a record-valued struct array as a table_slice. The
/// caller has already ensured `array` has no null rows, so we can hand the
/// child arrays straight to `RecordBatch::Make` without losing the parent
/// null bitmap.
auto non_null_run_as_slice(std::shared_ptr<arrow::StructArray> array)
  -> table_slice {
  const auto length = array->length();
  auto arrow_schema = arrow::schema(array->type()->fields());
  // Use `StructArray::field(i)` rather than `fields()`. `Slice()` only
  // applies the offset to the parent struct, leaving `fields()` pointing at
  // the unsliced child vector — so when this function receives a sliced run
  // from the null-parent walk below, the raw children would be longer than
  // `length` and the resulting RecordBatch would read the wrong rows.
  // `field(i)` slices each child to match the parent's offset and length.
  auto columns = arrow::ArrayVector{};
  columns.reserve(array->num_fields());
  for (auto i = 0; i < array->num_fields(); ++i) {
    columns.push_back(array->field(i));
  }
  auto batch = arrow::RecordBatch::Make(std::move(arrow_schema), length,
                                        std::move(columns));
  // Pass no explicit schema so the table_slice derives an anonymous
  // record_type from the Arrow schema. Passing the input series' named type
  // here would trip the table_slice constructor's schema/arrow-schema
  // consistency check, because the wrapping operation strips metadata.
  return table_slice{std::move(batch)};
}

/// Converts a slice produced by `drop_null_fields` back into a record-valued
/// series. Empty-schema slices become anonymous empty-record series, matching
/// the operator's behavior of emitting `{}` when every field is dropped.
auto slice_as_record_series(const table_slice& slice) -> series {
  const auto* record = try_as<record_type>(slice.schema());
  if (not record) {
    return series::null(null_type{}, slice.rows());
  }
  if (record->num_fields() == 0) {
    auto empty_struct = make_struct_array(
      slice.rows(), nullptr, arrow::FieldVector{}, arrow::ArrayVector{});
    return series{slice.schema(), std::move(empty_struct)};
  }
  if (slice.rows() == 0) {
    return series::null(slice.schema(), 0);
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
        auto struct_array = std::static_pointer_cast<arrow::StructArray>(
          std::move(value.array));
        // Apply drop_null_fields to a contiguous run of non-null parent rows
        // and append the result to `out`.
        auto process_run = [&](std::shared_ptr<arrow::StructArray> sub,
                               multi_series& out) {
          const auto run_length = sub->length();
          auto slice = non_null_run_as_slice(std::move(sub));
          auto parts = tenzir::drop_null_fields(std::move(slice), selectors,
                                                event_order::ordered, ctx.dh());
          auto produced = int64_t{0};
          for (const auto& part : parts) {
            auto part_series = slice_as_record_series(part);
            produced += part_series.length();
            out.append(std::move(part_series));
          }
          TENZIR_ASSERT(produced == run_length);
        };
        auto result = multi_series{};
        if (struct_array->null_count() == 0) {
          // Fast path: every input row carries a valid record.
          process_run(std::move(struct_array), result);
          return result;
        }
        // Some parent rows are null. Walk runs so we can pass each non-null
        // run through the shared implementation while emitting null rows for
        // the null runs. This preserves the row-level null semantics that
        // child arrays under a null parent would otherwise lose if we handed
        // the unflattened struct to `RecordBatch::Make`.
        auto row = int64_t{0};
        while (row < input_length) {
          const auto run_is_null = struct_array->IsNull(row);
          auto run_end = row + 1;
          while (run_end < input_length
                 and struct_array->IsNull(run_end) == run_is_null) {
            ++run_end;
          }
          const auto run_length = run_end - row;
          if (run_is_null) {
            result.append(series::null(input_type, run_length));
          } else {
            auto sub = std::static_pointer_cast<arrow::StructArray>(
              struct_array->Slice(row, run_length));
            process_run(std::move(sub), result);
          }
          row = run_end;
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
