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

#include <arrow/compute/api_vector.h>
#include <arrow/record_batch.h>
#include <arrow/util/bitmap_ops.h>

#include <algorithm>
#include <numeric>
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

auto eval_sort_predicate(const ast::lambda_expr& cmp, series lhs_series,
                         series rhs_series, const table_slice& scope,
                         comparator_warning_state& warning_state, session ctx)
  -> bool {
  TENZIR_ASSERT(cmp.is_binary());
  TENZIR_ASSERT(lhs_series.length() == 1);
  TENZIR_ASSERT(rhs_series.length() == 1);
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
  auto value = result.view3_at(0);
  return match(
    value,
    [](bool boolean) {
      return boolean;
    },
    [&](caf::none_t) {
      if (not warning_state.null_result) {
        diagnostic::warning("`cmp` lambda must return `bool`, got `null`")
          .primary(cmp.body)
          .emit(ctx);
        warning_state.null_result = true;
      }
      return false;
    },
    [&](const auto&) {
      if (not warning_state.non_boolean_result) {
        diagnostic::warning("`cmp` lambda must return `bool`")
          .primary(cmp.body)
          .emit(ctx);
        warning_state.non_boolean_result = true;
      }
      return false;
    });
}

auto sort_records_recursive(std::shared_ptr<arrow::Array> array)
  -> std::shared_ptr<arrow::Array>;

auto sort_list(const series& input, const std::optional<table_slice>& scope,
               bool descending, const std::optional<ast::lambda_expr>& cmp,
               session ctx) -> series {
  auto& list_type = as<tenzir::list_type>(input.type);
  auto& list_array = as<arrow::ListArray>(*input.array);
  auto values = list_array.values();
  auto offsets_builder
    = arrow::TypedBufferBuilder<int32_t>{tenzir::arrow_memory_pool()};
  check(offsets_builder.Reserve(input.length() + 1));
  offsets_builder.UnsafeAppend(0);
  auto null_bitmap = std::shared_ptr<arrow::Buffer>{};
  const auto has_null_bitmap = list_array.null_bitmap() != nullptr;
  auto null_builder
    = arrow::TypedBufferBuilder<bool>{tenzir::arrow_memory_pool()};
  if (has_null_bitmap) {
    check(null_builder.Reserve(input.length()));
  }
  auto indices_builder = arrow::Int64Builder{tenzir::arrow_memory_pool()};
  check(indices_builder.Reserve(values->length()));
  auto indices = std::vector<int64_t>{};
  auto sorted_values_length = int64_t{0};
  auto null_count = int64_t{0};
  TENZIR_ASSERT(
    not scope or scope->rows() == detail::narrow_cast<size_t>(input.length()));
  auto warning_state = comparator_warning_state{};
  auto fallback_scope = empty_scope_slice();
  for (auto row = int64_t{0}; row < input.length(); ++row) {
    auto row_scope = scope ? subslice(*scope, row, row + 1) : fallback_scope;
    if (list_array.IsNull(row)) {
      if (has_null_bitmap) {
        null_builder.UnsafeAppend(false);
      }
      offsets_builder.UnsafeAppend(
        detail::narrow<int32_t>(sorted_values_length));
      ++null_count;
      continue;
    }
    if (has_null_bitmap) {
      null_builder.UnsafeAppend(true);
    }
    const auto begin = list_array.value_offset(row);
    const auto end = list_array.value_offset(row + 1);
    indices.resize(end - begin);
    std::iota(indices.begin(), indices.end(), begin);
    if (cmp) {
      std::stable_sort(
        indices.begin(), indices.end(), [&](int64_t lhs, int64_t rhs) {
          if (descending) {
            std::swap(lhs, rhs);
          }
          return eval_sort_predicate(
            *cmp, series{list_type.value_type(), values->Slice(lhs, 1)},
            series{list_type.value_type(), values->Slice(rhs, 1)}, row_scope,
            warning_state, ctx);
        });
    } else if (descending) {
      std::stable_sort(
        indices.begin(), indices.end(), [&](int64_t lhs, int64_t rhs) {
          return weak_order(view_at(*values, lhs), view_at(*values, rhs))
                 == std::weak_ordering::greater;
        });
    } else {
      std::stable_sort(
        indices.begin(), indices.end(), [&](int64_t lhs, int64_t rhs) {
          return weak_order(view_at(*values, lhs), view_at(*values, rhs))
                 == std::weak_ordering::less;
        });
    }
    for (const auto index : indices) {
      indices_builder.UnsafeAppend(index);
    }
    sorted_values_length += detail::narrow<int64_t>(indices.size());
    offsets_builder.UnsafeAppend(detail::narrow<int32_t>(sorted_values_length));
  }
  if (has_null_bitmap) {
    null_bitmap = check(null_builder.FinishWithLength(input.length()));
  }
  auto value_indices = finish(indices_builder);
  auto sorted_values = check(arrow::compute::Take(*values, *value_indices));
  return {
    input.type,
    std::make_shared<arrow::ListArray>(
      list_array.type(), input.length(), check(offsets_builder.Finish()),
      std::move(sorted_values), std::move(null_bitmap), null_count, 0),
  };
}

auto sort_record(std::shared_ptr<arrow::StructArray> array)
  -> std::shared_ptr<arrow::StructArray> {
  const auto& fields = array->struct_type()->fields();
  struct kv_pair {
    std::shared_ptr<arrow::Field> key;
    std::shared_ptr<arrow::Array> value;

    auto name() const -> std::string_view {
      return key->name();
    }
  };
  auto data = std::vector<kv_pair>(fields.size());
  auto children_changed = false;
  auto fields_sorted = true;
  for (size_t i = 0; i < data.size(); ++i) {
    if (i > 0 and fields[i - 1]->name() > fields[i]->name()) {
      fields_sorted = false;
    }
    auto child = array->field(detail::narrow<int>(i));
    auto sorted_child = sort_records_recursive(child);
    children_changed |= sorted_child != child;
    auto field = fields[i];
    if (sorted_child->type() != field->type()) {
      field = field->WithType(sorted_child->type());
    }
    data[i] = {std::move(field), std::move(sorted_child)};
  }
  if (not children_changed and fields_sorted) {
    return array;
  }
  if (not fields_sorted) {
    std::ranges::sort(data, std::less<>{}, &kv_pair::name);
  }
  auto sorted_fields = arrow::FieldVector{};
  auto sorted_arrays = arrow::ArrayVector{};
  sorted_fields.reserve(data.size());
  sorted_arrays.reserve(data.size());
  for (size_t i = 0; i < data.size(); ++i) {
    sorted_fields.push_back(std::move(data[i].key));
    sorted_arrays.push_back(std::move(data[i].value));
  }
  auto null_bitmap = array->null_bitmap();
  if (array->offset() != 0 and array->null_bitmap_data()) {
    null_bitmap
      = check(arrow::internal::CopyBitmap(arrow_memory_pool(),
                                          array->null_bitmap_data(),
                                          array->offset(), array->length()));
  }
  return std::make_shared<arrow::StructArray>(
    arrow::struct_(sorted_fields), array->length(), std::move(sorted_arrays),
    std::move(null_bitmap), array->null_count(), 0);
}

auto sort_records_recursive(std::shared_ptr<arrow::Array> array)
  -> std::shared_ptr<arrow::Array> {
  return match(
    *array,
    [&](const arrow::StructArray&) -> std::shared_ptr<arrow::Array> {
      return sort_record(
        std::static_pointer_cast<arrow::StructArray>(std::move(array)));
    },
    [&](const arrow::ListArray& list) -> std::shared_ptr<arrow::Array> {
      const auto values_contain_records = match(
        *list.values(),
        [](const arrow::StructArray&) {
          return true;
        },
        [](const arrow::ListArray&) {
          return true;
        },
        [](const auto&) {
          return false;
        });
      if (not values_contain_records) {
        return array;
      }
      const auto value_offset = list.value_offset(0);
      const auto value_length = list.value_offset(list.length()) - value_offset;
      auto values = list.values()->Slice(value_offset, value_length);
      auto sorted_values = sort_records_recursive(values);
      if (sorted_values == values) {
        return array;
      }
      auto buffers = rebase_list_array_buffers(list);
      auto value_field = std::static_pointer_cast<arrow::ListType>(list.type())
                           ->value_field()
                           ->WithType(sorted_values->type());
      return std::make_shared<arrow::ListArray>(
        arrow::list(std::move(value_field)), buffers.length,
        std::move(buffers.offsets), std::move(sorted_values),
        std::move(buffers.null_bitmap), buffers.null_count, 0);
    },
    [&](const auto&) -> std::shared_ptr<arrow::Array> {
      return array;
    });
}

auto sort_record(const series& input) -> series {
  TENZIR_ASSERT(try_as<arrow::StructArray>(*input.array));
  auto array
    = sort_record(std::static_pointer_cast<arrow::StructArray>(input.array));
  if (array == input.array) {
    return input;
  }
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
