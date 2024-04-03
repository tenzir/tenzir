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

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)order;
    return optimize_result{filter, event_order::schema, copy()};
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

    std::vector<offset> indices;
    transformation_factory_type transformation_factory;
    steady_clock::time_point last_use{steady_clock::now()};
  };
  using projection_cache = std::unordered_map<type, cached_projection>;

  struct match_type {
    int64_t count{0};
    int64_t last_row_number{0};
    steady_clock::time_point last_time{steady_clock::now()};
  };
  struct match_hash {
    using is_transparent = void;

    auto operator()(const data& x) const -> size_t {
      return std::hash<data>{}(x);
    }
    auto operator()(const data_view& x) const -> size_t {
      return std::hash<data_view>{}(x);
    }
  };
  using match_store
    = std::unordered_map<data, match_type, match_hash, std::equal_to<>>;

  auto make_projection(const table_slice& slice, diagnostic_handler& diag) const
    -> std::optional<cached_projection> {
    std::vector<offset> indices{};
    std::vector<std::string> missing_fields{};
    const auto& schema = slice.schema();
    for (const auto& field : cfg_.fields) {
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
      = [num_fields = layout.num_fields(),
         transformation_callback = std::move(transformation_callback)](
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
    // Transform the current `slice` with the just-constructed
    // `transformation_factory`,
    // and extract the indices of the new null fields.
    auto extended_slice
      = transform_columns(slice, std::vector{transformation_factory(slice)});
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

  auto project(projection_cache& cache, const table_slice& slice,
               diagnostic_handler& diag) const
    -> std::pair<type, std::shared_ptr<arrow::RecordBatch>> {
    if (cfg_.fields.empty()) {
      // No extractors specified, match over the entire input
      return std::pair{slice.schema(), to_record_batch(slice)};
    }
    auto projection_it = cache.find(slice.schema());
    if (projection_it == cache.end()) {
      auto new_projection = make_projection(slice, diag);
      if (not new_projection) {
        return {{}, nullptr};
      }
      std::tie(projection_it, std::ignore)
        = cache.emplace(slice.schema(), std::move(*new_projection));
    } else {
      projection_it->second.last_use = steady_clock::now();
    }
    auto& projection = projection_it->second;
    TENZIR_ASSERT(not projection.indices.empty());
    if (not projection.transformation_factory) {
      return select_columns(slice.schema(), to_record_batch(slice),
                            projection.indices);
    }
    // Has `transformation_factory`, need to call it to transform the input
    // before calling `select_columns`.
    // (`projection.indices` are indices into this transformed input)
    auto transformed_slice = transform_columns(
      slice, std::vector{projection.transformation_factory(slice)});
    return select_columns(transformed_slice.schema(),
                          to_record_batch(transformed_slice),
                          projection.indices);
  }

  auto deduplicate(match_store& matches, int64_t& row_number,
                   const table_slice& slice, const type& ty,
                   const arrow::Array& elements) const
    -> generator<table_slice> {
    size_t begin{};
    const auto now = steady_clock::now();
    // Logic adapted from the `unique` operator
    for (size_t row = 0; row < slice.rows(); ++row) {
      auto element_view = value_at(ty, elements, static_cast<int64_t>(row));
      auto match_it = matches.find(element_view);
      if (match_it == matches.end()) {
        std::tie(match_it, std::ignore)
          = matches.emplace(materialize(element_view), match_type{});
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
