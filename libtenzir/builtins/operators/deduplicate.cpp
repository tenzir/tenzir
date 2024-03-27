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
                              f.field("timeout", x.timeout));
  }

  std::vector<std::string> fields{};
  int64_t limit{};
  int64_t distance{};
  steady_clock::duration timeout{};
};

class deduplicate_operator final : public crtp_operator<deduplicate_operator> {
public:
  deduplicate_operator() = default;

  deduplicate_operator(configuration cfg) : cfg_(std::move(cfg)) {
  }

  auto operator()(generator<table_slice> input) const
    -> generator<table_slice> {
    index_cache cached_indices{};
    match_store matches{};
    int64_t row_number{0};
    int64_t last_cleanup_row{0};
    auto last_cleanup_time = steady_clock::now();
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto [projected_type, projected_batch] = project(cached_indices, slice);
      TENZIR_ASSERT(projected_batch);
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
      //  - the index cache has grown to over 256 elements
      //    (there's probably no need to ever cache more than 256 projections,
      //     but this number isn't based on any objective measurement)
      if (const auto now = steady_clock::now();
          now - last_cleanup_time > cfg_.timeout / 2
          || row_number - last_cleanup_row > cfg_.distance
          || cached_indices.size() > 256) {
        cleanup_matches(matches, row_number);
        cleanup_indices(cached_indices);
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
  struct cached_index {
    std::vector<offset> indices;
    steady_clock::time_point last_use{steady_clock::now()};
  };
  using index_cache = std::unordered_map<type, cached_index>;

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

  auto make_indices(const type& schema) const -> std::vector<offset> {
    std::vector<offset> indices{};
    for (const auto& field : cfg_.fields) {
      for (auto idx : schema.resolve(field)) {
        indices.emplace_back(std::move(idx));
      }
    }
    std::sort(indices.begin(), indices.end());
    indices.erase(std::unique(indices.begin(), indices.end()), indices.end());
    return indices;
  }

  auto project(index_cache& indices, const table_slice& slice) const
    -> std::pair<type, std::shared_ptr<arrow::RecordBatch>> {
    if (cfg_.fields.empty()) {
      return std::pair{slice.schema(), to_record_batch(slice)};
    }
    auto offset_it = indices.find(slice.schema());
    if (offset_it == indices.end()) {
      std::tie(offset_it, std::ignore)
        = indices.emplace(slice.schema(), make_indices(slice.schema()));
    } else {
      offset_it->second.last_use = steady_clock::now();
    }
    auto& offsets = offset_it->second;
    return select_columns(slice.schema(), to_record_batch(slice),
                          offsets.indices);
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

  auto cleanup_indices(index_cache& indices) const -> void {
    // Not cleaning up indices if we're caching less than 128 of them
    if (indices.size() < 128) {
      return;
    }
    // Extract all nodes from `indices`,
    // sort by `last_use` (descending),
    // insert back only the ones that have been used most recently.
    // Choosing 64 as a nice round number.
    std::vector<index_cache::node_type> nodes{};
    nodes.reserve(indices.size());
    while (not indices.empty()) {
      nodes.emplace_back(indices.extract(indices.begin()));
    }
    TENZIR_ASSERT(indices.empty());
    std::partial_sort(nodes.begin(), nodes.begin() + 64, nodes.end(),
                      [](const auto& a, const auto& b) {
                        return a.mapped().last_use > b.mapped().last_use;
                      });
    nodes.resize(64);
    for (auto& node : nodes) {
      indices.insert(std::move(node));
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
      const auto p = parsers::required_ws_or_comment >> ~parsers::extractor_list
                     >> parsers::optional_ws_or_comment;
      if (!p(f, l, fields)) {
        return {
          std::string_view{f, l},
          caf::make_error(ec::syntax_error,
                          fmt::format("failed to parse the fields argument in "
                                      "the deduplicate operator: '{}'",
                                      pipeline)),
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
    parser.add("--limit", limit, "<count>");
    parser.add("--distance", distance, "<count>");
    parser.add("--timeout", timeout, "<duration>");
    collecting_diagnostic_handler diag_handler{};
    auto source
      = tql::make_parser_interface(std::string{f, op_end}, diag_handler);
    parser.parse(*source);
    if (auto diags = std::move(diag_handler).collect(); not diags.empty()) {
      return {std::string_view{f, l},
              add_context(diags.front().to_error(),
                          "failed to parse the flags in the deduplicate "
                          "operator: '{}'",
                          pipeline)};
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
