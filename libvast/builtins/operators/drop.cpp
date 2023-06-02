//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/arrow_table_slice.hpp>
#include <vast/concept/convertible/data.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/detail/inspection_common.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/plugin.hpp>
#include <vast/type.hpp>

#include <arrow/type.h>
#include <fmt/format.h>

namespace vast::plugins::drop {

namespace {

/// The configuration of a project pipeline operator.
struct configuration {
  /// The key suffixes of the fields to drop.
  std::vector<std::string> fields = {};

  /// The key suffixes of the schemas to drop.
  std::vector<std::string> schemas = {};

  /// Support type inspection for easy parsing with convertible.
  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return detail::apply_all(f, x.fields, x.schemas);
  }

  /// Enable parsing from a record via convertible.
  static inline const record_type& schema() noexcept {
    static auto result = record_type{
      {"fields", list_type{string_type{}}},
      {"schemas", list_type{string_type{}}},
    };
    return result;
  }
};

/// Drops the specifed fields from the input.
class drop_operator final
  : public schematic_operator<
      drop_operator, std::optional<std::vector<indexed_transformation>>> {
public:
  drop_operator() = default;

  explicit drop_operator(configuration config) noexcept
    : config_{std::move(config)} {
    // nop
  }

  auto initialize(const type& schema, operator_control_plane&) const
    -> caf::expected<state_type> override {
    // Determine whether we want to drop the entire batch first.
    const auto drop_schema
      = std::any_of(config_.schemas.begin(), config_.schemas.end(),
                    [&](const auto& dropped_schema) {
                      return dropped_schema == schema.name();
                    });
    if (drop_schema)
      return std::nullopt;

    // Apply the transformation.
    auto transform_fn
      = [&](struct record_type::field, std::shared_ptr<arrow::Array>) noexcept
      -> std::vector<
        std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>> {
      return {};
    };
    auto transformations = std::vector<indexed_transformation>{};
    for (const auto& field : config_.fields)
      for (auto&& index : caf::get<record_type>(schema).resolve_key_suffix(
             field, schema.name()))
        transformations.push_back({std::move(index), transform_fn});
    // transform_columns requires the transformations to be sorted, and that may
    // not necessarily be true if we have multiple fields configured, so we sort
    // again in that case.
    if (config_.fields.size() > 1)
      std::sort(transformations.begin(), transformations.end());
    return transformations;
  }

  /// Processes a single slice with the corresponding schema-specific state.
  auto process(table_slice slice, state_type& state) const
    -> output_type override {
    if (state) {
      return transform_columns(slice, *state);
    }
    return {};
  }

  [[nodiscard]] auto to_string() const noexcept -> std::string override {
    return fmt::format("drop {}", fmt::join(config_.fields, ", "));
  }

  auto name() const -> std::string override {
    return "drop";
  }

  friend auto inspect(auto& f, drop_operator& x) -> bool {
    return f.apply(x.config_);
  }

private:
  /// The underlying configuration of the transformation.
  configuration config_;
};

class plugin final : public virtual operator_plugin<drop_operator> {
public:
  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::end_of_pipeline_operator, parsers::required_ws_or_comment,
      parsers::optional_ws_or_comment, parsers::extractor_list;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = required_ws_or_comment >> extractor_list
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    auto config = configuration{};
    if (!p(f, l, config.fields)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse drop "
                                                      "operator: '{}'",
                                                      pipeline)),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<drop_operator>(std::move(config)),
    };
  }
};

} // namespace

} // namespace vast::plugins::drop

VAST_REGISTER_PLUGIN(vast::plugins::drop::plugin)
