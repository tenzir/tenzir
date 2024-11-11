//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/numeric/integral.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/type.hpp>

#include <arrow/compute/api_vector.h>
#include <arrow/record_batch.h>

#include <ranges>

namespace tenzir::plugins::sort {

namespace {

auto sort_list(const series& input) -> series {
  auto builder = series_builder{input.type};
  for (const auto& value :
       values(as<list_type>(input.type), as<arrow::ListArray>(*input.array))) {
    if (not value) {
      builder.null();
      continue;
    }
    auto materialized = materialize(*value);
    std::ranges::sort(materialized);
    builder.data(materialized);
  }
  return builder.finish_assert_one_array();
}

auto sort_record(const arrow::StructArray& array)
  -> std::shared_ptr<arrow::StructArray> {
  auto fields = array.struct_type()->fields();
  auto arrays = array.fields();
  struct kv_pair {
    std::shared_ptr<arrow::Field> key;
    std::shared_ptr<arrow::Array> value;

    auto name() const -> std::string_view {
      return key->name();
    }
  };
  auto data = std::vector<kv_pair>(fields.size());
  for (size_t i = 0; i < data.size(); ++i) {
    data[i] = {std::move(fields[i]), std::move(arrays[i])};
  }
  std::ranges::sort(data, std::less<>{}, &kv_pair::name);
  for (size_t i = 0; i < data.size(); ++i) {
    fields[i] = std::move(data[i].key);
    arrays[i] = std::move(data[i].value);
  }
  return std::make_shared<arrow::StructArray>(
    arrow::struct_(fields), array.length(), std::move(arrays),
    array.null_bitmap(), array.null_count(), array.offset());
}

auto sort_record(const series& input) -> series {
  auto array = sort_record(as<arrow::StructArray>(*input.array));
  auto type = type::from_arrow(*array->struct_type());
  return series{
    std::move(type),
    std::move(array),
  };
}

class sort_state {
public:
  sort_state(const std::string& key,
             const arrow::compute::ArraySortOptions& sort_options)
    : key_{key}, sort_options_{sort_options} {
  }

  auto try_add(table_slice slice, operator_control_plane& ctrl) -> table_slice {
    if (slice.rows() == 0) {
      return slice;
    }
    const auto& path = find_or_create_path(slice.schema(), ctrl);
    if (not path) {
      return {};
    }
    auto batch = to_record_batch(slice);
    TENZIR_ASSERT(batch);
    auto array = path->get(*batch);
    // TODO: Sorting in Arrow using arrow::compute::SortIndices is not
    // supported for extension types, so eventually we'll have to roll our
    // own implementation. In the meantime, we sort the underlying storage
    // array, which at least sorts in some stable way.
    if (auto ext_array
        = std::dynamic_pointer_cast<arrow::ExtensionArray>(array)) {
      sort_keys_.push_back(ext_array->storage());
    } else {
      sort_keys_.push_back(std::move(array));
    }
    offset_table_.push_back(offset_table_.back()
                            + detail::narrow_cast<int64_t>(slice.rows()));
    cache_.push_back(std::move(slice));
    return {};
  }

  auto sorted() && -> generator<table_slice> {
    // If there is nothing to sort, then we can just return early.
    if (cache_.empty()) {
      co_return;
    }
    // Arrow's sort function returns us an Int64Array of indices, which are
    // guaranteed not to be null. We map these in a two-step process onto our
    // cached table slices, and yield slices of size 1 for each returned row.
    // The algorithm below uses an offset table that has an additional 0 value
    // at the start, and uses std::upper_bound to find the entry in the cache
    // using the offset table.
    const auto chunked_key
      = arrow::ChunkedArray::Make(std::move(sort_keys_)).ValueOrDie();
    const auto indices
      = arrow::compute::SortIndices(*chunked_key, sort_options_);
    if (not indices.ok()) {
      diagnostic::error("{}", indices.status().ToString())
        .note("failed to sort `{}`", key_)
        .throw_();
    }
    auto result_buffer = std::vector<table_slice>{};
    for (const auto& index :
         static_cast<const arrow::Int64Array&>(*indices.ValueUnsafe())) {
      TENZIR_ASSERT(index.has_value());
      const auto offset = std::prev(
        std::upper_bound(offset_table_.begin(), offset_table_.end(), *index));
      const auto cache_index = std::distance(offset_table_.begin(), offset);
      const auto row = *index - *offset;
      const auto& slice = cache_[cache_index];
      auto result = subslice(slice, row, row + 1);
      TENZIR_ASSERT(result.rows() == 1);
      co_yield std::move(result);
    }
  }

private:
  auto find_or_create_path(const type& schema, operator_control_plane& ctrl)
    -> const std::optional<offset>& {
    auto key_path = key_field_path_.find(schema);
    if (key_path != key_field_path_.end()) {
      return key_path->second;
    }
    // Set up the sorting and emit warnings at most once per schema.
    key_path = key_field_path_.emplace_hint(
      key_field_path_.end(), schema, schema.resolve_key_or_concept_once(key_));
    if (not key_path->second.has_value()) {
      diagnostic::warning("sort key `{}` does not apply to schema `{}`", key_,
                          schema)
        .note("events of this schema will be discarded")
        .note("from `sort`")
        .emit(ctrl.diagnostics());
      return key_path->second;
    }
    auto current_key_type
      = as<record_type>(schema).field(*key_path->second).type.prune();
    if (is<subnet_type>(current_key_type)) {
      // TODO: Sorting in Arrow using arrow::compute::SortIndices is not
      // supported for extension types. We can fall back to the storage array
      // for all types but subnet, which has a nested extension type.
      diagnostic::warning("sort key `{}` resolves to unsupported type `subnet` "
                          "for schema `{}`",
                          key_, schema)
        .note("events of this schema will be discarded")
        .note("from `sort`")
        .emit(ctrl.diagnostics());
      key_path->second = std::nullopt;
      return key_path->second;
    }
    if (not key_type_) {
      key_type_ = current_key_type;
    } else if (key_type_ != current_key_type) {
      diagnostic::warning("sort key `{}` resolves to type `{}` "
                          "for schema `{}`, but to `{}` for a previous schema",
                          key_, current_key_type, schema, *key_type_)
        .note("events of this schema will be discarded")
        .note("from `sort`")
        .emit(ctrl.diagnostics());
      key_path->second = std::nullopt;
    }
    return key_path->second;
  }

  /// The sort field key, as passed to the operator.
  const std::string& key_;

  /// The sort options, as passed to the operator.
  const arrow::compute::ArraySortOptions& sort_options_;

  /// The slices that we want to sort.
  std::vector<table_slice> cache_ = {};

  /// An offset table into the cached slices. The first entry of this is always
  /// zero, and for every slice we append to the cache we append the total
  /// number of rows in the cache to this table. This allows for using
  /// std::upper_bound to identify the index of the cache entry quickly.
  std::vector<int64_t> offset_table_ = {0};

  /// The arrays that we sort by, in the same order as the offset table.
  std::vector<std::shared_ptr<arrow::Array>> sort_keys_ = {};

  /// The cached field paths for the sorted-by field per schema. A nullopt value
  /// indicates that sorting is not possible for this schema.
  std::unordered_map<type, std::optional<offset>> key_field_path_ = {};

  /// The type of the sorted-by field.
  std::optional<type> key_type_ = {};
};

class sort_operator final : public crtp_operator<sort_operator> {
public:
  sort_operator() = default;

  sort_operator(std::string key, bool stable, bool descending, bool nulls_first)
    : key_{std::move(key)},
      stable_{stable},
      descending_{descending},
      nulls_first_{nulls_first} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto options = arrow::compute::ArraySortOptions::Defaults();
    options.order = descending_ ? arrow::compute::SortOrder::Descending
                                : arrow::compute::SortOrder::Ascending;
    options.null_placement = nulls_first_
                               ? arrow::compute::NullPlacement::AtStart
                               : arrow::compute::NullPlacement::AtEnd;
    auto state = sort_state{key_, options};
    co_yield {};
    for (auto&& slice : input) {
      co_yield state.try_add(std::move(slice), ctrl);
    }
    // The sorted slices are very like to have size 1 each, so we rebatch them
    // first to avoid inefficiencies in downstream operators.
    auto buffer = std::vector<table_slice>{};
    auto num_buffered = uint64_t{0};
    for (auto&& slice : std::move(state).sorted()) {
      if (not buffer.empty() and buffer.back().schema() != slice.schema()) {
        while (not buffer.empty()) {
          auto [lhs, rhs] = split(buffer, defaults::import::table_slice_size);
          auto result = concatenate(std::move(lhs));
          num_buffered -= result.rows();
          co_yield std::move(result);
          buffer = std::move(rhs);
        }
      }
      num_buffered += slice.rows();
      buffer.push_back(std::move(slice));
      while (num_buffered >= defaults::import::table_slice_size) {
        auto [lhs, rhs] = split(buffer, defaults::import::table_slice_size);
        auto result = concatenate(std::move(lhs));
        num_buffered -= result.rows();
        co_yield std::move(result);
        buffer = std::move(rhs);
      }
    }
    if (not buffer.empty()) {
      co_yield concatenate(std::move(buffer));
    }
  }

  auto name() const -> std::string override {
    return "sort";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    return optimize_result{filter, stable_ ? order : event_order::unordered,
                           copy()};
  }

  friend auto inspect(auto& f, sort_operator& x) -> bool {
    return f.object(x).fields(f.field("key", x.key_),
                              f.field("stable", x.stable_),
                              f.field("descending", x.descending_),
                              f.field("nulls_first", x.nulls_first_));
  }

private:
  std::string key_ = {};
  bool stable_ = {};
  bool descending_ = {};
  bool nulls_first_ = {};
};

class plugin final : public virtual operator_plugin<sort_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::optional_ws_or_comment, parsers::required_ws_or_comment,
      parsers::end_of_pipeline_operator, parsers::extractor, parsers::str;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p
      = required_ws_or_comment
        >> -(str{"--stable"}.then([&](std::string) -> bool {
            return true;
          }) >> required_ws_or_comment)
        >> ((extractor
             >> (-(required_ws_or_comment >> (str{"asc"} | str{"desc"})))
                  .then([&](caf::optional<std::string> sort_order) {
                    return sort_order.value_or("asc") == "desc";
                  })
             >> (-(required_ws_or_comment
                   >> (str{"nulls-first"} | str{"nulls-last"})))
                  .then([&](caf::optional<std::string> null_placement) {
                    return null_placement.value_or("nulls-last")
                           == "nulls-first";
                  })
             >> optional_ws_or_comment)
            % (',' >> optional_ws_or_comment))
        >> end_of_pipeline_operator;
    auto sort_args
      = std::vector<std::tuple<std::string /*key*/, bool /*descending*/,
                               bool /*nulls_first*/>>{};
    bool stable = false;
    if (!p(f, l, stable, sort_args)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse "
                                                      "sort operator: '{}'",
                                                      pipeline)),
      };
    }
    if (sort_args.empty()) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, "sort operator requires at least one "
                                          "sort key"),
      };
    }
    auto result = std::make_unique<tenzir::pipeline>();
    bool first = true;
    for (auto& [key, descending, nulls_first] :
         sort_args | std::ranges::views::reverse) {
      result->append(std::make_unique<sort_operator>(
        std::move(key), first ? stable : true, descending, nulls_first));
      first = false;
    }
    return {
      std::string_view{f, l},
      std::move(result),
    };
  }
};

// -- TQL2 implementation below ------------------------------------------------

struct sort_expression {
  ast::expression expr = {};
  bool reverse = {};

  friend auto inspect(auto& f, sort_expression& x) -> bool {
    return f.object(x).fields(f.field("expr", x.expr),
                              f.field("reverse", x.reverse));
  }
};

struct sort_key {
  std::vector<series> chunks = {};
  bool reverse = {};
};

struct sort_index {
  size_t slice = {};
  int64_t event = {};
};

class sort_operator2 final : public crtp_operator<sort_operator2> {
public:
  sort_operator2() = default;

  explicit sort_operator2(std::vector<sort_expression> sort_exprs)
    : sort_exprs_{std::move(sort_exprs)} {
  }

  auto name() const -> std::string override {
    return "tql2.sort";
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto events = std::vector<table_slice>{};
    auto indices = std::vector<sort_index>{};
    auto sort_keys = std::vector<sort_key>{};
    sort_keys.reserve(sort_exprs_.size());
    for (const auto& sort_expr : sort_exprs_) {
      sort_keys.emplace_back().reverse = sort_expr.reverse;
    }
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      const auto length = detail::narrow<int64_t>(slice.rows());
      indices.reserve(indices.size() + slice.rows());
      for (int64_t i = 0; i < length; ++i) {
        indices.push_back({
          .slice = events.size(),
          .event = i,
        });
      }
      for (size_t i = 0; i < sort_exprs_.size(); ++i) {
        sort_keys[i].chunks.push_back(
          eval(sort_exprs_[i].expr, slice, ctrl.diagnostics()));
      }
      events.push_back(std::move(slice));
    }
    if (indices.empty()) {
      co_return;
    }
    // TODO: If all chunks for a sort key evaluate to the same type, then we can
    // choose a faster path where we do not need to evaluate the sort key's type
    // for each row individually. That may be significantly faster.
    std::ranges::sort(indices, [&](const sort_index& lhs,
                                   const sort_index& rhs) {
      for (const auto& sort_key : sort_keys) {
        const auto& lhs_key = sort_key.chunks[lhs.slice];
        const auto& rhs_key = sort_key.chunks[rhs.slice];
        const auto lhs_null = lhs_key.array->IsNull(lhs.event);
        const auto rhs_null = rhs_key.array->IsNull(rhs.event);
        if (lhs_null and rhs_null) {
          continue;
        }
        if (lhs_null or rhs_null) {
          // Nulls last, independent of sort order.
          return rhs_null;
        }
        const auto& lhs_value
          = value_at(lhs_key.type, *lhs_key.array, lhs.event);
        const auto& rhs_value
          = value_at(rhs_key.type, *rhs_key.array, rhs.event);
        // TODO: Implement this directly on data and data_view. That is
        // non-trivial however.
        // TODO: This does not do the correct recursive application of the
        // comparator to nested structural types. It turns out that is a
        // non-trivial task as well.
        const auto cmp = detail::overload{
          [](const concepts::integer auto& l, const concepts::integer auto& r) {
            return std::cmp_less(l, r);
          },
          []<concepts::number L, concepts::number R>(const L& l, const R& r) {
            if constexpr (std::same_as<L, double>) {
              if (std::isnan(l)) {
                return false;
              }
            }
            if constexpr (std::same_as<R, double>) {
              if (std::isnan(r)) {
                return true;
              }
            }
            return l < r;
          },
          [&](const auto& l, const auto& r) {
            if constexpr (std::same_as<decltype(l), decltype(r)>) {
              return l < r;
            }
            return lhs_value.index() < rhs_value.index();
          },
        };
        if (caf::visit(cmp, lhs_value, rhs_value)) {
          return not sort_key.reverse;
        }
        if (caf::visit(cmp, rhs_value, lhs_value)) {
          return sort_key.reverse;
        }
      }
      // If we're here then it's a tie.
      return false;
    });
    // Lastly, assemble the result by fetching the rows in their sorted order.
    auto batch = std::vector<table_slice>{};
    for (const auto& index : indices) {
      if (not batch.empty()
          and batch.back().schema() != events[index.slice].schema()) {
        co_yield concatenate(std::exchange(batch, {}));
      }
      if (batch.size() >= defaults::import::table_slice_size) {
        co_yield concatenate(std::exchange(batch, {}));
      }
      batch.push_back(
        subslice(events[index.slice], index.event, index.event + 1));
    }
    if (not batch.empty()) {
      co_yield concatenate(std::exchange(batch, {}));
    }
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    // Our upstream can always be unordered. If our downstream did already not
    // care about ordering, we can skip sorting entirely.
    return optimize_result{
      filter,
      event_order::unordered,
      order == event_order::unordered ? nullptr : copy(),
    };
  }

  friend auto inspect(auto& f, sort_operator2& x) -> bool {
    return f.apply(x.sort_exprs_);
  }

private:
  std::vector<sort_expression> sort_exprs_ = {};
};

class plugin2 final : public virtual operator_plugin2<sort_operator2>,
                      public virtual function_plugin {
public:
  auto make(operator_factory_plugin::invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TENZIR_UNUSED(ctx);
    if (inv.args.empty()) {
      return std::make_unique<sort_operator2>(std::vector<sort_expression>{{
        .expr = ast::this_{inv.self.get_location()},
        .reverse = false,
      }});
    }
    auto sort_exprs = std::vector<sort_expression>{};
    sort_exprs.reserve(inv.args.size());
    const auto make_sort_key = [&](const auto& arg) {
      return arg.match(
        [&](const ast::unary_expr& unary) -> sort_expression {
          if (unary.op.inner == ast::unary_op::neg) {
            return {unary.expr, true};
          }
          return {unary, false};
        },
        [&](const auto& other) -> sort_expression {
          return {other, false};
        });
    };
    std::ranges::transform(inv.args, std::back_inserter(sort_exprs),
                           make_sort_key);
    return std::make_unique<sort_operator2>(std::move(sort_exprs));
  }

  auto make_function(function_plugin::invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name()).add(expr, "<expr>").parse(inv, ctx));
    return function_use::make(
      [call = inv.call, expr = std::move(expr)](auto eval, session ctx) {
        auto arg = eval(expr);
        auto f = detail::overload{
          [&](const arrow::NullArray&) {
            return arg;
          },
          [&](const arrow::ListArray&) {
            return sort_list(arg);
          },
          [&](const arrow::StructArray&) {
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
        return caf::visit(f, *arg.array);
      });
  }
};

} // namespace

} // namespace tenzir::plugins::sort

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sort::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::sort::plugin2)
