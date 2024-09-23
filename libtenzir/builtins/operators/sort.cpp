//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/concept/parseable/numeric/integral.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>

#include <arrow/compute/api_vector.h>
#include <arrow/record_batch.h>

#include <ranges>

namespace tenzir::plugins::sort {

namespace {

class sort_state {
public:
  sort_state(const std::string& key,
             const arrow::compute::ArraySortOptions& sort_options)
    : key_{key}, sort_options_{sort_options} {
  }

  auto try_add(table_slice slice, exec_ctx ctx) -> table_slice {
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
  auto find_or_create_path(const type& schema, exec_ctx ctx)
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
      = caf::get<record_type>(schema).field(*key_path->second).type.prune();
    if (caf::holds_alternative<subnet_type>(current_key_type)) {
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

  auto operator()(generator<table_slice> input, exec_ctx ctx) const
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

} // namespace

} // namespace tenzir::plugins::sort

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sort::plugin)
