//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/collect.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql/parser.hpp>

#include <ranges>

namespace tenzir::plugins::deduplicate {
namespace {

/// Returns `true` if
///  - `rec` is a flat record (depth of 0 or 1), and
///  - the keys of `rec` are sorted.
/// Requires `d` to be a record.
auto is_sorted_and_flattened(const record& rec) -> bool {
  if (depth(rec) > 1) {
    return false;
  }
  return std::ranges::is_sorted(rec | std::views::keys);
}

/// Flattens `rec`, and sorts it keys.
/// Postcondition: `is_sorted_and_flattened(rec)` is `true`.
void make_sorted_and_flattened(record& rec) {
  if (depth(rec) > 1) {
    rec = flatten(rec);
  }
  std::ranges::sort(rec, std::ranges::less{}, &record::value_type::first);
}

/// A wrapper for `record`,
/// where `is_sorted_and_flattened(get())` is always `true`.
///
/// Used as the key in the `matches` hashmap in `deduplicate`,
/// to allow for transparent comparison irrespective of field ordering.
class sorted_flat_record {
public:
  sorted_flat_record() = default;

  sorted_flat_record(const record& x) : inner_(construct(x)) {
  }

  sorted_flat_record(record&& x) : inner_(construct(std::move(x))) {
  }

  auto get() -> record& {
    return inner_;
  }
  auto get() const -> const record& {
    return inner_;
  }

  auto operator==(const sorted_flat_record& other) const -> bool {
    return inner_ == other.inner_;
  }

private:
  static auto construct(const record& x) -> record {
    if (is_sorted_and_flattened(x)) {
      return x;
    }
    auto y = x;
    make_sorted_and_flattened(y);
    return y;
  }

  static auto construct(record&& x) -> record {
    if (not is_sorted_and_flattened(x)) {
      make_sorted_and_flattened(x);
    }
    return x;
  }

  record inner_{};
};
} // namespace
} // namespace tenzir::plugins::deduplicate

namespace std {
template <>
struct hash<tenzir::plugins::deduplicate::sorted_flat_record> {
  auto
  operator()(const tenzir::plugins::deduplicate::sorted_flat_record& x) const
    -> size_t {
    return tenzir::hash(x.get());
  }
};
} // namespace std

namespace tenzir::plugins::deduplicate {
namespace {

using std::chrono::steady_clock;

struct configuration {
  friend auto inspect(auto& f, configuration& x) -> bool {
    return f.object(x).fields(f.field("fields", x.fields),
                              f.field("limit", x.limit),
                              f.field("distance", x.distance),
                              f.field("timeout", x.timeout),
                              f.field("project_only", x.project_only));
  }

  std::vector<std::string> fields{};
  int64_t limit{};
  int64_t distance{};
  steady_clock::duration timeout{};
  bool project_only{false};
};

class deduplicate_operator final : public crtp_operator<deduplicate_operator> {
public:
  deduplicate_operator() = default;

  deduplicate_operator(configuration cfg) : cfg_(std::move(cfg)) {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    projection_cache cached_projections{};
    match_store matches{};
    int64_t row_number{0};
    int64_t last_cleanup_row{0};
    auto last_cleanup_time = steady_clock::now();
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      // Project given input based on the fields given in `configuration`.
      // Essentially, `project` returns a modified table slice,
      // which contains records that only have the fields
      // that are used for deduplication.
      // These projected records are also what are stored in `matches`.
      // The actual records we yield from this operator are subslices
      // of the input, not these projected slices.
      auto [projected_type, projected_batch]
        = project(cached_projections, slice, ctrl.diagnostics());
      if (not projected_batch) {
        co_return;
      }
      if (cfg_.project_only) {
        co_yield table_slice{projected_batch, projected_type};
        continue;
      }
      auto elements = projected_batch->ToStructArray().ValueOrDie();
      TENZIR_ASSERT(elements);
      for (auto&& new_slice :
           deduplicate(matches, row_number, slice, projected_type, *elements)) {
        if (new_slice.rows() > 0) {
          co_yield std::move(new_slice);
        }
      }
      // Clean up `matches` and `cached_indices` every so often:
      //  - we haven't cleaned up in a while (half the --timeout)
      //  - we haven't cleaned up in N rows (where N = --distance)
      //  - the projection cache has grown to over 256 elements
      //    (there's probably no need to ever cache more than 256 projections,
      //     but this number isn't based on any objective measurement)
      if (const auto now = steady_clock::now();
          now - last_cleanup_time > cfg_.timeout / 2
          || row_number - last_cleanup_row > cfg_.distance
          || cached_projections.size() > 256) {
        cleanup_matches(matches, row_number);
        cleanup_projection_cache(cached_projections);
        last_cleanup_time = now;
        last_cleanup_row = row_number;
        co_yield {};
      }
    }
  }

  auto name() const -> std::string override {
    return "deduplicate";
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    (void)order;
    if (cfg_.distance < std::numeric_limits<int64_t>::max()) {
      // When `--distance` is used, we're not allowed to optimize at all. Here's
      // a simple example that proves this:
      //   metrics platform
      //   | deduplicate connected --distance 1
      //   | where connected == false
      return do_not_optimize(*this);
    }
    return optimize_result{filter, event_order::ordered, copy()};
  }

  friend auto inspect(auto& f, deduplicate_operator& x) -> bool {
    return f.object(x)
      .pretty_name("deduplicate_operator")
      .fields(f.field("configuration", x.cfg_));
  }

private:
  struct cached_projection {
    using transformation_factory_type
      = std::function<auto(const table_slice&)->indexed_transformation>;

    /// Make a new `cached_projection`,
    /// for input with the same schema as `slice`, using the fields in `fields`.
    /// `flattened_slice` must be flat.
    static auto
    make(const table_slice& flattened_slice,
         std::span<const std::string> fields, diagnostic_handler& diag)
      -> std::optional<cached_projection>;

    /// Apply the projection contained in `*this` to `flattened_slice`.
    /// `flattened_slice` must be flat.
    /// `last_use` is not modified (`apply` is `const`).
    auto apply(const table_slice& flattened_slice) const
      -> std::pair<type, std::shared_ptr<arrow::RecordBatch>>;

    /// Record indices/offsets that are used for the projection.
    /// The input must be flattened first,
    /// and the result of `transformation_factory` applied, if present.
    /// An empty `indices` means matching over the entire event:
    /// all columns are selected, none are dropped.
    std::vector<offset> indices;

    /// If present, contains a transformation that must be applied to the input
    /// after flattening it, but before selecting `indices`.
    /// Currently used to insert `null` values for missing columns.
    transformation_factory_type transformation_factory;

    /// Contains the time this projection was last used.
    /// Used for cleanup purposes:
    /// (if needed, unused projections are cleaned up first).
    steady_clock::time_point last_use{steady_clock::now()};
  };
  using projection_cache = std::unordered_map<type, cached_projection>;

  struct match_type {
    int64_t count{0};
    int64_t last_row_number{0};
    steady_clock::time_point last_time{steady_clock::now()};
  };
  using match_store = std::unordered_map<sorted_flat_record, match_type>;

  /// Project `slice` based on the configuration,
  /// and return the projected table slice.
  /// On error, returns `pair{null_type, nullptr}`.
  auto project(projection_cache& cache, const table_slice& slice,
               diagnostic_handler& diag) const
    -> std::pair<type, std::shared_ptr<arrow::RecordBatch>> {
    auto [flattened_slice, _] = flatten(slice);
    auto projection_it = cache.find(slice.schema());
    if (projection_it == cache.end()) {
      auto new_projection
        = cached_projection::make(flattened_slice, cfg_.fields, diag);
      if (not new_projection) {
        return {{}, nullptr};
      }
      std::tie(projection_it, std::ignore)
        = cache.emplace(slice.schema(), std::move(*new_projection));
    } else {
      projection_it->second.last_use = steady_clock::now();
    }
    auto& projection = projection_it->second;
    return projection.apply(flattened_slice);
  }

  auto deduplicate(match_store& matches, int64_t& row_number,
                   const table_slice& slice, const type& projected_type,
                   const arrow::Array& projected_elements) const
    -> generator<table_slice> {
    size_t begin{};
    const auto now = steady_clock::now();
    // Logic adapted from the `unique` operator
    for (size_t row = 0; row < slice.rows(); ++row) {
      auto projected_value_view = value_at(projected_type, projected_elements,
                                           static_cast<int64_t>(row));
      auto projected_value = materialize(projected_value_view);
      TENZIR_ASSERT(caf::holds_alternative<record>(projected_value));
      auto& projected_record = caf::get<record>(projected_value);
      auto match_it = matches.find(projected_record);
      if (match_it == matches.end()) {
        std::tie(match_it, std::ignore)
          = matches.emplace(std::move(projected_record), match_type{});
      }
      auto& match = match_it->second;
      // This value hasn't been matched within the timeout,
      // reset match count to zero
      if (now - match.last_time > cfg_.timeout) {
        match.count = 0;
      }
      match.last_time = now;
      // Same as above, but for the distance / row number
      if (row_number - match.last_row_number > cfg_.distance) {
        match.count = 0;
      }
      match.last_row_number = row_number;
      // If we're over the --limit, skip the current row
      if (match.count >= cfg_.limit) {
        co_yield subslice(slice, begin, row);
        begin = row + 1;
      } else {
        ++match.count;
      }
      ++row_number;
    }
    co_yield subslice(slice, begin, slice.rows());
  }

  auto cleanup_matches(match_store& matches, int64_t row_number) const -> void {
    if (cfg_.distance == std::numeric_limits<int64_t>::max()
        && cfg_.limit == std::numeric_limits<int64_t>::max()) {
      return;
    }
    const auto now = steady_clock::now();
    // Erase stale matches
    std::erase_if(matches, [&](const auto& elem) -> bool {
      auto& match = elem.second;
      if (row_number - match.last_row_number > cfg_.distance) {
        return true;
      }
      if (now - match.last_time > cfg_.timeout) {
        return true;
      }
      return false;
    });
  }

  static auto cleanup_projection_cache(projection_cache& cache) -> void {
    // Not cleaning up cache if we're caching less than 128 items
    if (cache.size() < 128) {
      return;
    }
    // Extract all nodes from `cache`,
    // sort by `last_use` (descending),
    // insert back only the ones that have been used most recently.
    // Choosing 64 as a nice round number.
    std::vector<projection_cache::node_type> nodes{};
    nodes.reserve(cache.size());
    while (not cache.empty()) {
      nodes.emplace_back(cache.extract(cache.begin()));
    }
    TENZIR_ASSERT(cache.empty());
    std::partial_sort(nodes.begin(), nodes.begin() + 64, nodes.end(),
                      [](const auto& a, const auto& b) {
                        return a.mapped().last_use > b.mapped().last_use;
                      });
    nodes.resize(64);
    for (auto& node : nodes) {
      cache.insert(std::move(node));
    }
  }

  configuration cfg_{};
};

auto deduplicate_operator::cached_projection::make(
  const table_slice& flattened_slice, std::span<const std::string> fields,
  diagnostic_handler& diag)
  -> std::optional<deduplicate_operator::cached_projection> {
  TENZIR_ASSERT_EXPENSIVE(flattened_slice == flatten(flattened_slice).slice);
  if (fields.empty()) {
    // `fields` is empty, match on the entire event/record.
    // This is indicated by an empty `indices` vector.
    // Because we're matching on the entire input, by definition we won't have
    // any missing fields, then, either.
    return cached_projection{
      .indices = {},
      .transformation_factory = {},
    };
  }
  std::vector<offset> indices{};
  std::vector<std::string> missing_fields{};
  const auto& schema = flattened_slice.schema();
  TENZIR_ASSERT(caf::holds_alternative<record_type>(schema));
  // Resolve indices in `schema`.
  // If a field is missing, the field name is added to `missing_fields`.
  for (const auto& field : fields) {
    bool resolved = false;
    for (auto idx : schema.resolve(field)) {
      indices.emplace_back(std::move(idx));
      resolved = true;
    }
    if (not resolved) {
      TENZIR_ASSERT(not field.empty());
      if (field.starts_with(':')) {
        // We can't easily deal with missing type extractors, just erroring out.
        diagnostic::error(
          "failed to deduplicate due to unmatched type extractor")
          .note("`{}` matched no fields in schema `{}`", field, schema.name())
          .emit(diag);
        return std::nullopt;
      }
      missing_fields.emplace_back(field);
    }
  }
  if (missing_fields.empty()) {
    // Every field in `cfg_.fields` was found in `slice.schema()`.
    // Clean up the indices and return.
    std::ranges::sort(indices);
    {
      auto uniq = std::ranges::unique(indices);
      indices.erase(uniq.begin(), uniq.end());
    }
    return cached_projection{
      .indices = std::move(indices),
      .transformation_factory = {},
    };
  }
  // Some fields were missing.
  // Construct a `transformation_factory`, that'll add these fields in, with a
  // value of `null`.
  TENZIR_ASSERT_EXPENSIVE(std::unordered_set<std::string>(
                            missing_fields.begin(), missing_fields.end())
                            .size()
                          == missing_fields.size());
  const auto& layout = caf::get<record_type>(schema);
  auto transformation_callback =
    [missing_fields](int64_t rows, struct record_type::field input_field,
                     std::shared_ptr<arrow::Array> input_array) {
      auto result = std::vector<
        std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>>{};
      result.reserve(missing_fields.size() + 1);
      result.emplace_back(std::move(input_field), std::move(input_array));
      for (const auto& missing_field : missing_fields) {
        auto builder
          = null_type::make_arrow_builder(arrow::default_memory_pool());
        {
          const auto append_result = builder->AppendNulls(rows);
          TENZIR_ASSERT(append_result.ok(), append_result.ToString().c_str());
        }
        using field_type = struct record_type::field;
        result.emplace_back(field_type{missing_field, type{null_type{}}},
                            builder->Finish().ValueOrDie());
      }
      return result;
    };
  auto transformation_factory
    = [num_fields = layout.num_fields(), transformation_callback
                                         = std::move(transformation_callback)](
        const table_slice& slice) -> indexed_transformation {
    return {
      {num_fields - 1},
      [rows = slice.rows(),
       &transformation_callback](struct record_type::field input_field,
                                 std::shared_ptr<arrow::Array> input_array) {
        return transformation_callback(static_cast<int64_t>(rows),
                                       std::move(input_field),
                                       std::move(input_array));
      },
    };
  };
  // Transform the current `flattened_slice` with the just-constructed
  // `transformation_factory`,
  // and extract the indices of the new null fields.
  auto extended_slice = transform_columns(
    flattened_slice, std::vector{transformation_factory(flattened_slice)});
  const auto& extended_schema = extended_slice.schema();
  for (const auto& missing_field : missing_fields) {
    bool resolved = false;
    for (auto idx : extended_schema.resolve(missing_field)) {
      indices.emplace_back(std::move(idx));
      resolved = true;
    }
    TENZIR_ASSERT(resolved);
  }
  std::ranges::sort(indices);
  {
    auto uniq = std::ranges::unique(indices);
    indices.erase(uniq.begin(), uniq.end());
  }
  return cached_projection{
    .indices = std::move(indices),
    .transformation_factory = std::move(transformation_factory),
  };
}

auto deduplicate_operator::cached_projection::apply(
  const table_slice& flattened_slice) const
  -> std::pair<type, std::shared_ptr<arrow::RecordBatch>> {
  TENZIR_ASSERT_EXPENSIVE(flattened_slice == flatten(flattened_slice).slice);
  if (indices.empty()) {
    return {flattened_slice.schema(), to_record_batch(flattened_slice)};
  }
  if (not transformation_factory) {
    return select_columns(flattened_slice.schema(),
                          to_record_batch(flattened_slice), indices);
  }
  // Has `transformation_factory`, need to call it to transform the input
  // before calling `select_columns`.
  // (`projection.indices` are indices into this transformed input)
  auto transformed_slice = transform_columns(
    flattened_slice, std::vector{transformation_factory(flattened_slice)});
  return select_columns(transformed_slice.schema(),
                        to_record_batch(transformed_slice), indices);
}

class plugin final : public virtual operator_plugin<deduplicate_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    // First, parse the fields manually
    std::vector<std::string> fields{};
    {
      const auto p = parsers::optional_ws_or_comment >> ~parsers::extractor_list
                     >> parsers::optional_ws_or_comment;
      if (!p(f, l, fields)) {
        return {
          std::string_view{f, l},
          caf::make_error(ec::syntax_error,
                          fmt::format("failed to parse the extractor list "
                                      "argument in the deduplicate operator: "
                                      "'{}'",
                                      pipeline)),
        };
      }
      std::ranges::sort(fields);
      if (auto duplicate_it = std::ranges::adjacent_find(fields);
          duplicate_it != fields.end()) {
        return {
          std::string_view{f, l},
          caf::make_error(ec::syntax_error,
                          fmt::format("duplicate extractor in the extractor "
                                      "list for the deduplicate operator: '{}'",
                                      *duplicate_it)),
        };
      }
    }
    // Find out where this operator ends
    const auto* op_end = f;
    {
      const auto p = *(parsers::any - parsers::end_of_pipeline_operator)
                     >> parsers::end_of_pipeline_operator;
      p(op_end, l, unused);
    }
    // Parse the flags using argument_parser,
    // with the input being [f, op_end):
    // f points to the first argument after the fields,
    // op_end points to the end of this operator
    auto parser = argument_parser{"deduplicate", "https://docs.tenzir.com/"
                                                 "operators/deduplicate"};
    std::optional<int64_t> limit{};
    std::optional<int64_t> distance{};
    std::optional<duration> timeout{};
    bool project_only{false};
    parser.add("--limit", limit, "<count>");
    parser.add("--distance", distance, "<count>");
    parser.add("--timeout", timeout, "<duration>");
    parser.add("--project-only", project_only);
    collecting_diagnostic_handler diag_handler{};
    auto source
      = tql::make_parser_interface(std::string{f, op_end}, diag_handler);
    parser.parse(*source);
    if (auto diags = std::move(diag_handler).collect(); not diags.empty()) {
      return {
        std::string_view{f, l},
        add_context(diags.front().to_error(),
                    "failed to parse the flags in the deduplicate "
                    "operator: '{}'",
                    pipeline),
      };
    }
    // `0` as distance means infinity
    if (distance && *distance == 0) {
      distance.emplace(std::numeric_limits<int64_t>::max());
    }
    auto op = std::make_unique<deduplicate_operator>(configuration{
      .fields = std::move(fields),
      .limit = limit.value_or(1),
      .distance = distance.value_or(std::numeric_limits<int64_t>::max()),
      .timeout = timeout.value_or(
        steady_clock::duration{std::numeric_limits<int64_t>::max()}),
      .project_only = project_only,
    });
    return {
      std::string_view{op_end, l},
      std::move(op),
    };
  }
};

} // namespace

} // namespace tenzir::plugins::deduplicate

TENZIR_REGISTER_PLUGIN(tenzir::plugins::deduplicate::plugin)
