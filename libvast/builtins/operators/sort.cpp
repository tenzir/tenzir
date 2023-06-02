//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/arrow_table_slice.hpp>
#include <vast/concept/parseable/numeric/integral.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice.hpp>

#include <arrow/compute/api_vector.h>
#include <arrow/record_batch.h>

namespace vast::plugins::sort {

namespace {

auto is_extension_type(const type& type) -> bool {
  VAST_ASSERT(type);
  const auto f = []<concrete_type Type>(const Type&) {
    return not std::is_same_v<type_to_arrow_array_t<Type>,
                              type_to_arrow_array_storage_t<Type>>;
  };
  return caf::visit(f, type);
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
    VAST_ASSERT(batch);
    sort_keys_.push_back(path->get(*batch));
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
      = arrow::compute::SortIndices(*chunked_key, sort_options_).ValueOrDie();
    auto result_buffer = std::vector<table_slice>{};
    for (const auto& index : static_cast<const arrow::Int64Array&>(*indices)) {
      VAST_ASSERT(index.has_value());
      const auto offset = std::prev(
        std::upper_bound(offset_table_.begin(), offset_table_.end(), *index));
      const auto cache_index = std::distance(offset_table_.begin(), offset);
      const auto row = *index - *offset;
      const auto& slice = cache_[cache_index];
      auto result = subslice(slice, row, row + 1);
      VAST_ASSERT(result.rows() == 1);
      // TODO: Yielding slices with 1 row is very inefficient, and we probably
      // want to batch them to larger slices here before yielding (or implicitly
      // run the results through the rebatch operator).
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
      key_field_path_.end(), schema,
      caf::get<record_type>(schema).resolve_key(key_));
    if (not key_path->second.has_value()) {
      ctrl.warn(caf::make_error(ec::invalid_configuration,
                                fmt::format("sort key {} does not apply to "
                                            "schema {}; events of this "
                                            "schema will not be sorted",
                                            key_, schema)));
    }
    auto current_key_type
      = key_path->second
          ? caf::get<record_type>(schema).field(*key_path->second).type
          : type{};
    if (not key_type_ && current_key_type) {
      // TODO: Sorting in Arrow using arrow::compute::SortIndices is not
      // supported for extension types, so eventually we'll have to roll our
      // own imlementation. In the meantime, we ignore extension types for
      // sorting and warn about them.
      if (is_extension_type(current_key_type)) {
        ctrl.warn(caf::make_error(
          ec::invalid_configuration,
          fmt::format("sort key {} resolved to type {} for schema {}, for "
                      "which sorting is not yet implemented; this schema "
                      "will not be sorted",
                      key_, current_key_type, schema)));
        key_path->second = std::nullopt;
      } else {
        key_type_ = current_key_type;
      }
    } else if (key_type_ != current_key_type) {
      ctrl.warn(caf::make_error(
        ec::invalid_configuration,
        fmt::format("sort key {} resolved to type {} for schema {}, but "
                    "resolved to {} for a previous schema; events of this "
                    "schema will not be sorted",
                    key_, current_key_type, schema, key_type_)));
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
  type key_type_ = {};
};

class sort_operator final : public crtp_operator<sort_operator> {
public:
  sort_operator() = default;

  sort_operator(std::string key, bool descending, bool nulls_first)
    : key_{std::move(key)}, descending_{descending}, nulls_first_{nulls_first} {
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
    for (auto&& slice : input) {
      co_yield state.try_add(std::move(slice), ctrl);
    }
    for (auto&& slice : std::move(state).sorted()) {
      co_yield std::move(slice);
    }
  }

  auto name() const -> std::string override {
    return "sort";
  }

  friend auto inspect(auto& f, sort_operator& x) -> bool {
    return f.object(x).fields(f.field("descending", x.descending_),
                              f.field("nulls_first", x.nulls_first_));
  }

private:
  std::string key_ = {};
  bool descending_{};
  bool nulls_first_{};
};

class plugin final : public virtual operator_plugin<sort_operator> {
public:
  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::optional_ws_or_comment, parsers::required_ws_or_comment,
      parsers::end_of_pipeline_operator, parsers::extractor, parsers::str;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    auto key = std::string{};
    const auto p
      = required_ws_or_comment >> extractor
        >> -(required_ws_or_comment >> (str{"asc"} | str{"desc"}))
              .then([&](std::string sort_order) {
                return !(sort_order.empty() || sort_order == "asc");
              })
        >> -(required_ws_or_comment >> (str{"nulls-first"} | str{"nulls-last"}))
              .then([&](std::string null_placement) {
                return !(null_placement.empty()
                         || null_placement == "nulls-last");
              })
        >> optional_ws_or_comment >> end_of_pipeline_operator;
    bool descending = false;
    bool nulls_first = false;
    if (!p(f, l, key, descending, nulls_first)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse "
                                                      "sort operator: '{}'",
                                                      pipeline)),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<sort_operator>(std::move(key), descending, nulls_first),
    };
  }
};

} // namespace

} // namespace vast::plugins::sort

VAST_REGISTER_PLUGIN(vast::plugins::sort::plugin)
