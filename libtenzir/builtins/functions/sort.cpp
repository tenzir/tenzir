//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/comparison.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/function_plugin.hpp"
#include "tenzir/option.hpp"

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
#include <ranges>
#include <string_view>
#include <vector>

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

auto sort_list(const series& input, const Option<table_slice>& scope,
               bool descending, const Option<ast::lambda_expr>& cmp,
               session ctx) -> series {
  auto builder = series_builder{input.type};
  TENZIR_ASSERT(
    not scope or scope->rows() == detail::narrow_cast<size_t>(input.length()));
  auto warning_state = comparator_warning_state{};
  auto fallback_scope = empty_scope_slice();
  auto row = int64_t{0};
  for (const auto& value : values(type{as<list_type>(input.type)},
                                  as<arrow::ListArray>(*input.array))) {
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
  if (try_as<arrow::StructArray>(*array)) {
    return sort_record(
      std::static_pointer_cast<arrow::StructArray>(std::move(array)));
  }
  if (auto* list = try_as<arrow::ListArray>(*array)) {
    if (not try_as<arrow::StructArray>(*list->values())
        and not try_as<arrow::ListArray>(*list->values())) {
      return array;
    }
    const auto value_offset = list->value_offset(0);
    const auto value_length = list->value_offset(list->length()) - value_offset;
    auto sliced_values = list->values()->Slice(value_offset, value_length);
    auto sorted_values = sort_records_recursive(sliced_values);
    if (sorted_values == sliced_values) {
      return array;
    }
    auto buffers = rebase_list_array_buffers(*list);
    auto value_field = std::static_pointer_cast<arrow::ListType>(list->type())
                         ->value_field()
                         ->WithType(sorted_values->type());
    auto values = series{
      type::from_arrow(*value_field),
      std::move(sorted_values),
    };
    return make_list_series_with_offsets(values, std::move(buffers)).array;
  }
  return array;
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

struct SortArgs {
  nova::ValueArgument x;
  bool descending = false;
  nova::BinaryLambdaArgument cmp;
  location call;
};

/// Deduplicates the warnings a `cmp` lambda can raise per call. The lambda
/// body runs once per comparison, so both the diagnostics it emits itself and
/// the ones we raise about its result need to collapse to one each.
struct nova_comparator_warning_state {
  bool null_result = false;
  bool non_boolean_result = false;
  diagnostic_deduplicator eval_diagnostics = {};
};

/// Evaluates `cmp` for a single pair of elements, with the enclosing row as
/// input so that captures resolve against the event the list belongs to.
auto eval_nova_sort_predicate(nova::BinaryLambdaArgument const& cmp,
                              nova::RowView<nova::Data> lhs,
                              nova::RowView<nova::Data> rhs,
                              nova::Events const& scope,
                              nova::EvalFrame const& frame,
                              nova_comparator_warning_state& warnings) -> bool {
  using namespace nova;
  auto lhs_builder = ArrayBuilder<Data>{};
  append_row(lhs_builder, lhs);
  auto rhs_builder = ArrayBuilder<Data>{};
  append_row(rhs_builder, rhs);
  auto const present = storage::BitMap{1, true};
  auto cmp_dh
    = deduplicating_diagnostic_handler{frame.dh(), warnings.eval_diagnostics};
  auto result
    = frame.eval(cmp, MaskedArray<Array<Data>>{lhs_builder.finish(), present},
                 MaskedArray<Array<Data>>{rhs_builder.finish(), present}, scope,
                 cmp_dh);
  return match(
    result.get(storage::Index{0}),
    [](RowView<Bool> value) {
      return *value;
    },
    [&](RowView<Null>) {
      if (not warnings.null_result) {
        diagnostic::warning("`cmp` lambda must return `bool`, got `null`")
          .primary(cmp.location())
          .emit(frame);
        warnings.null_result = true;
      }
      return false;
    },
    [&](auto const&) {
      if (not warnings.non_boolean_result) {
        diagnostic::warning("`cmp` lambda must return `bool`")
          .primary(cmp.location())
          .emit(frame);
        warnings.non_boolean_result = true;
      }
      return false;
    });
}

template <class Builder>
auto append_sorted_records(Builder&& builder, nova::RowView<nova::Data> value)
  -> void {
  using namespace nova;
  match(value, [&]<class T>(nova::RowView<T> value) {
    if constexpr (std::same_as<T, Null>) {
      builder.null();
    } else if constexpr (std::same_as<T, List>) {
      auto output = builder.list();
      for (auto element : value) {
        append_sorted_records(output, element);
      }
    } else if constexpr (std::same_as<T, Record>) {
      auto fields = std::vector<std::pair<std::string_view, RowView<Data>>>{};
      for (auto field : value) {
        fields.push_back(field);
      }
      std::ranges::sort(fields, std::less<>{},
                        &decltype(fields)::value_type::first);
      auto output = builder.record();
      for (auto const& [name, field] : fields) {
        append_sorted_records(output.field(name), field);
      }
    } else {
      builder.data(*value);
    }
  });
}

template <class Builder>
auto append_sorted_list(Builder&& builder, nova::RowView<nova::List> value,
                        bool descending, nova::BinaryLambdaArgument const& cmp,
                        nova::Events const* scope, nova::EvalFrame const& frame,
                        nova_comparator_warning_state& warnings) -> void {
  using namespace nova;
  auto elements = std::vector<RowView<Data>>{};
  for (auto element : value) {
    elements.push_back(element);
  }
  if (cmp) {
    TENZIR_ASSERT(scope);
    // `desc` reverses the comparator instead of the result, so that it stays
    // a strict weak ordering and `stable_sort` keeps ties in input order.
    std::stable_sort(elements.begin(), elements.end(), [&](auto lhs, auto rhs) {
      return descending ? eval_nova_sort_predicate(cmp, rhs, lhs, *scope, frame,
                                                   warnings)
                        : eval_nova_sort_predicate(cmp, lhs, rhs, *scope, frame,
                                                   warnings);
    });
    auto output = builder.list();
    for (auto element : elements) {
      append_row(output, element);
    }
    return;
  }
  auto const order = descending ? Order::descending : Order::ascending;
  std::stable_sort(
    elements.begin(), elements.end(), [order](auto lhs, auto rhs) {
      return nova::weak_order(lhs, rhs, order) == std::weak_ordering::less;
    });
  auto output = builder.list();
  for (auto element : elements) {
    append_row(output, element);
  }
}

class SortFunction final {
public:
  static auto eval(SortArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    auto builder = ArrayBuilder<Data>{};
    auto invalid = false;
    auto record_descending = false;
    auto warnings = nova_comparator_warning_state{};
    auto const* input = frame.input();
    for (auto row = storage::Index{0}; row < frame.length(); ++row) {
      if (not frame.mask().get(row)) {
        builder.skip();
        continue;
      }
      auto value = args.x.data.get(row);
      auto scope = Option<Events>{};
      if (args.cmp) {
        // The comparator sees its enclosing event, so that captures such as
        // `pivot` resolve per row.
        scope = input ? subslice(*input, row, row + 1)
                      : Events{Array<Record>::make_empty(1),
                               storage::BitMap{1, true},
                               Events::Meta::make_empty(1)};
      }
      match(value, [&]<class T>(RowView<T> value) {
        if constexpr (std::same_as<T, Null>) {
          builder.null();
        } else if constexpr (std::same_as<T, List>) {
          append_sorted_list(builder, value, args.descending, args.cmp,
                             scope ? std::addressof(*scope) : nullptr, frame,
                             warnings);
        } else if constexpr (std::same_as<T, Record>) {
          record_descending |= args.descending or bool{args.cmp};
          append_sorted_records(builder, RowView<Data>{value});
        } else {
          invalid = true;
          builder.null();
        }
      });
    }
    if (invalid) {
      diagnostic::warning("`sort` expected `record` or `list`, got a different "
                          "type")
        .primary(args.x.source)
        .emit(frame);
    }
    if (record_descending) {
      diagnostic::warning(
        "`desc` and `cmp` are only applied when sorting lists")
        .primary(args.call)
        .note("record fields are always sorted ascending by key")
        .emit(frame);
    }
    return builder.finish();
  }
};

class plugin final : public virtual function_plugin,
                     public virtual nova::FunctionPlugin {
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

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<SortArgs, SortFunction>{};
    d.positional("x", &SortArgs::x, "list|record");
    d.named_optional("desc", &SortArgs::descending, "bool");
    d.named_optional_binary("cmp", &SortArgs::cmp, "(a, b) => bool");
    d.call_location(&SortArgs::call);
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto descending = false;
    auto cmp = Option<ast::lambda_expr>{};
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
        auto scope = Option<table_slice>{};
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
