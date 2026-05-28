//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/series.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>
#include <tenzir/type.hpp>

#include <arrow/record_batch.h>
#include <arrow/util/bitmap_ops.h>

#include <algorithm>
#include <optional>
#include <ranges>
#include <string_view>

namespace tenzir::plugins::sort_function {

namespace {

struct comparator_warning_state {
  bool invalid_result_length = false;
  bool null_result = false;
  bool non_boolean_result = false;
  diagnostic_deduplicator eval_diagnostics = {};
};

auto empty_scope_slice() -> table_slice {
  auto schema = type{"cmp", record_type{}};
  return table_slice{
    arrow::RecordBatch::Make(schema.to_arrow_schema(), int64_t{1},
                             arrow::ArrayVector{}),
    std::move(schema),
  };
}

class deduplicating_diagnostic_handler final : public diagnostic_handler {
public:
  deduplicating_diagnostic_handler(diagnostic_handler& inner,
                                   diagnostic_deduplicator& deduplicator)
    : inner_{inner}, deduplicator_{deduplicator} {
  }

  void emit(diagnostic diag) override {
    if (deduplicator_.insert(diag)) {
      inner_.emit(std::move(diag));
    }
  }

private:
  diagnostic_handler& inner_;
  diagnostic_deduplicator& deduplicator_;
};

auto eval_sort_predicate(const ast::lambda_expr& cmp, const data& lhs_value,
                         const data& rhs_value, const table_slice& scope,
                         comparator_warning_state& warning_state, session ctx)
  -> bool {
  TENZIR_ASSERT(cmp.is_binary());
  auto lhs_series = data_to_series(lhs_value, int64_t{1});
  auto rhs_series = data_to_series(rhs_value, int64_t{1});
  auto lhs_param_path
    = check(ast::field_path::try_from(ast::root_field{cmp.param(0), false}));
  auto rhs_param_path
    = check(ast::field_path::try_from(ast::root_field{cmp.param(1), false}));
  auto slice = assign(lhs_param_path, std::move(lhs_series), scope, ctx.dh());
  slice = assign(rhs_param_path, std::move(rhs_series), slice, ctx.dh());
  auto cmp_dh = deduplicating_diagnostic_handler{
    ctx.dh(), warning_state.eval_diagnostics};
  auto result = eval(cmp.body, slice, cmp_dh);
  if (result.length() != 1) {
    if (not warning_state.invalid_result_length) {
      diagnostic::warning("`cmp` lambda must return exactly one value")
        .primary(cmp.body)
        .emit(ctx);
      warning_state.invalid_result_length = true;
    }
    return false;
  }
  auto value = materialize(result.view3_at(0));
  if (auto* boolean = try_as<bool>(&value)) {
    return *boolean;
  }
  if (is<caf::none_t>(value)) {
    if (not warning_state.null_result) {
      diagnostic::warning("`cmp` lambda must return `bool`, got `null`")
        .primary(cmp.body)
        .emit(ctx);
      warning_state.null_result = true;
    }
    return false;
  }
  if (not warning_state.non_boolean_result) {
    diagnostic::warning("`cmp` lambda must return `bool`")
      .primary(cmp.body)
      .emit(ctx);
    warning_state.non_boolean_result = true;
  }
  return false;
}

auto sort_array_recursive(std::shared_ptr<arrow::Array> array)
  -> std::shared_ptr<arrow::Array>;

auto sort_list(const series& input, const std::optional<table_slice>& scope,
               bool descending, const std::optional<ast::lambda_expr>& cmp,
               session ctx) -> series {
  auto input_array = sort_array_recursive(input.array);
  auto input_type = input.type;
  if (input_array->type() != input.array->type()) {
    input_type = type::from_arrow(*input_array->type());
  }
  auto builder = series_builder{input_type};
  TENZIR_ASSERT(
    not scope or scope->rows() == detail::narrow_cast<size_t>(input.length()));
  auto warning_state = comparator_warning_state{};
  auto fallback_scope = empty_scope_slice();
  auto row = int64_t{0};
  for (const auto& value : values(type{as<list_type>(input_type)},
                                  as<arrow::ListArray>(*input_array))) {
    auto row_scope = scope ? subslice(*scope, row, row + 1) : fallback_scope;
    ++row;
    if (is<caf::none_t>(value)) {
      builder.null();
      continue;
    }
    const auto* list_view = try_as<view<list>>(&value);
    TENZIR_ASSERT(list_view);
    auto materialized = materialize(*list_view);
    if (cmp) {
      std::stable_sort(materialized.begin(), materialized.end(),
                       [&](const data& lhs, const data& rhs) {
                         const auto& cmp_lhs = descending ? rhs : lhs;
                         const auto& cmp_rhs = descending ? lhs : rhs;
                         return eval_sort_predicate(*cmp, cmp_lhs, cmp_rhs,
                                                    row_scope, warning_state,
                                                    ctx);
                       });
    } else if (descending) {
      std::stable_sort(materialized.begin(), materialized.end(),
                       [](const data& lhs, const data& rhs) {
                         return rhs < lhs;
                       });
    } else {
      std::stable_sort(materialized.begin(), materialized.end(),
                       [](const data& lhs, const data& rhs) {
                         return lhs < rhs;
                       });
    }
    builder.data(materialized);
  }
  return builder.finish_assert_one_array();
}

auto sort_record(const arrow::StructArray& array)
  -> std::shared_ptr<arrow::StructArray> {
  auto fields = array.struct_type()->fields();
  auto arrays = arrow::ArrayVector{};
  arrays.reserve(array.num_fields());
  for (auto i = 0; i < array.num_fields(); ++i) {
    arrays.push_back(array.field(i));
  }
  struct kv_pair {
    std::shared_ptr<arrow::Field> key;
    std::shared_ptr<arrow::Array> value;

    auto name() const -> std::string_view {
      return key->name();
    }
  };
  auto data = std::vector<kv_pair>(fields.size());
  for (size_t i = 0; i < data.size(); ++i) {
    auto sorted_array = sort_array_recursive(arrays[i]);
    if (sorted_array->type() != fields[i]->type()) {
      fields[i] = fields[i]->WithType(sorted_array->type());
    }
    arrays[i] = std::move(sorted_array);
    data[i] = {std::move(fields[i]), std::move(arrays[i])};
  }
  std::ranges::sort(data, std::less<>{}, &kv_pair::name);
  for (size_t i = 0; i < data.size(); ++i) {
    fields[i] = std::move(data[i].key);
    arrays[i] = std::move(data[i].value);
  }
  auto null_bitmap = array.null_bitmap();
  if (array.offset() != 0 and array.null_bitmap_data()) {
    null_bitmap = check(
      arrow::internal::CopyBitmap(arrow_memory_pool(), array.null_bitmap_data(),
                                  array.offset(), array.length()));
  }
  return std::make_shared<arrow::StructArray>(arrow::struct_(fields),
                                              array.length(), std::move(arrays),
                                              std::move(null_bitmap),
                                              array.null_count(), 0);
}

auto sort_array_recursive(std::shared_ptr<arrow::Array> array)
  -> std::shared_ptr<arrow::Array> {
  if (auto* record = try_as<arrow::StructArray>(*array)) {
    return sort_record(*record);
  }
  if (auto* list = try_as<arrow::ListArray>(*array)) {
    const auto value_offset = list->value_offset(0);
    const auto value_length = list->value_offset(list->length()) - value_offset;
    auto values = list->values()->Slice(value_offset, value_length);
    auto sorted_values = sort_array_recursive(values);
    if (sorted_values == values) {
      return array;
    }
    auto buffers = rebase_list_array_buffers(*list);
    return std::make_shared<arrow::ListArray>(
      arrow::list(sorted_values->type()), buffers.length,
      std::move(buffers.offsets), std::move(sorted_values),
      std::move(buffers.null_bitmap), buffers.null_count, 0);
  }
  return array;
}

auto sort_record(const series& input) -> series {
  auto array = sort_record(as<arrow::StructArray>(*input.array));
  auto type = type::from_arrow(*array->struct_type());
  return series{
    std::move(type),
    std::move(array),
  };
}

class plugin final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "sort-function";
  }

  auto function_name() const -> std::string override {
    return "sort";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto descending = false;
    auto cmp = std::optional<ast::lambda_expr>{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "list|record")
          .named_optional("desc", descending, "bool")
          .named("cmp", cmp, "(a, b) => bool")
          .parse(inv, ctx));
    if (cmp and not cmp->is_binary()) {
      diagnostic::error("`cmp` must be a binary lambda")
        .primary(*cmp)
        .hint("provide `cmp=(a, b) => ...`")
        .emit(ctx);
      return failure::promise();
    }
    return function_use::make([call = inv.call, expr = std::move(expr),
                               descending,
                               cmp = std::move(cmp)](auto eval, session ctx) {
      auto input = eval.get_input();
      return map_series(eval(expr), [call, descending, cmp, ctx,
                                     input = std::move(input),
                                     offset = int64_t{0}](series arg) mutable {
        auto scope = std::optional<table_slice>{};
        if (input) {
          scope = subslice(*input, offset, offset + arg.length());
        }
        offset += arg.length();
        auto f = detail::overload{
          [&](const arrow::NullArray&) {
            return arg;
          },
          [&](const arrow::ListArray&) {
            return sort_list(arg, scope, descending, cmp, ctx);
          },
          [&](const arrow::StructArray&) {
            if (descending or cmp) {
              diagnostic::warning(
                "`desc` and `cmp` are only applied when sorting lists")
                .primary(call)
                .note("record fields are always sorted ascending by key")
                .emit(ctx);
            }
            return sort_record(arg);
          },
          [&](const auto&) {
            diagnostic::warning("`sort` expected `record` or `list`, got `{}`",
                                arg.type.kind())
              .primary(call)
              .emit(ctx);
            return series::null(null_type{}, arg.length());
          },
        };
        return match(*arg.array, f);
      });
    });
  }
};

} // namespace

} // namespace tenzir::plugins::sort_function

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sort_function::plugin)
