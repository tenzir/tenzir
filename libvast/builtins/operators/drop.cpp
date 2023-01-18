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
#include <vast/concept/parseable/core.hpp>
#include <vast/concept/parseable/string/char_class.hpp>
#include <vast/detail/inspection_common.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/type.hpp>

#include <arrow/type.h>

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
class drop_operator : public pipeline_operator {
public:
  explicit drop_operator(configuration config) noexcept
    : config_{std::move(config)} {
    // nop
  }

  caf::error
  add(type schema, std::shared_ptr<arrow::RecordBatch> batch) override {
    VAST_DEBUG("drop operator adds batch");
    // Determine whether we want to drop the entire batch first.
    const auto drop_schema
      = std::any_of(config_.schemas.begin(), config_.schemas.end(),
                    [&](const auto& dropped_schema) {
                      return dropped_schema == schema.name();
                    });
    if (drop_schema)
      return caf::none;
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
    auto [adjusted_schema, adjusted_batch]
      = transform_columns(schema, batch, transformations);
    if (adjusted_schema) {
      VAST_ASSERT(adjusted_batch);
      transformed_.emplace_back(std::move(adjusted_schema),
                                std::move(adjusted_batch));
    }
    return caf::none;
  }

  caf::expected<std::vector<pipeline_batch>> finish() override {
    VAST_DEBUG("drop operator finished transformation");
    return std::exchange(transformed_, {});
  }

private:
  /// The slices being transformed.
  std::vector<pipeline_batch> transformed_;

  /// The underlying configuration of the transformation.
  configuration config_;
};

class plugin final : public virtual pipeline_operator_plugin {
public:
  // plugin API
  caf::error initialize(data) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "drop";
  };

  // transform plugin API
  [[nodiscard]] caf::expected<std::unique_ptr<pipeline_operator>>
  make_pipeline_operator(const record& options) const override {
    if (!(options.contains("fields") || options.contains("schemas")))
      return caf::make_error(ec::invalid_configuration,
                             "key 'fields' or 'schemas' is missing in "
                             "configuration for drop operator");
    auto config = to<configuration>(options);
    if (!config)
      return config.error();
    return std::make_unique<drop_operator>(std::move(*config));
  }

  [[nodiscard]] std::pair<std::string_view,
                          caf::expected<std::unique_ptr<pipeline_operator>>>
  make_pipeline_operator(std::string_view pipeline) const override {
    // '... | drop extractor, ... '
    //            ^ we start here
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    using parsers::space, parsers::eoi, parsers::alnum, parsers::chr;
    using namespace parser_literals;
    const auto required_ws = ignore(+space);
    const auto optional_ws = ignore(*space);
    auto extractor_char = alnum | chr{'_'} | chr{'-'} | chr{':'};
    // A field cannot start with:
    //  - '-' to leave room for potential arithmetic expressions in operands
    //  - ':' so it won't be interpreted as a type extractor
    auto extractor = (!('-'_p) >> (+extractor_char % '.'));
    // .then([](const std::vector<char>& in)
    // {
    //   return std::string{in.begin(), in.end()};
    // });
    const auto p = required_ws >> (extractor % (',' >> optional_ws))
                   >> optional_ws >> ('|' | eoi);
    auto config = configuration{};
    auto parse_result = std::string{};

    if (!extractor(f, l, parse_result)) {
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
