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
#include <vast/error.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/type.hpp>

#include <arrow/type.h>
#include <caf/expected.hpp>

#include <algorithm>
#include <utility>

namespace vast::plugins::select {

namespace {

/// The configuration of a select pipeline operator.
struct configuration {
  /// The key suffixes of the fields to keep.
  std::vector<std::string> fields = {};

  /// Support type inspection for easy parsing with convertible.
  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return f.apply(x.fields);
  }

  /// Enable parsing from a record via convertible.
  static inline const record_type& schema() noexcept {
    static auto result = record_type{
      {"fields", list_type{string_type{}}},
    };
    return result;
  }
};

class select_operator : public pipeline_operator {
public:
  explicit select_operator(configuration config) noexcept
    : config_{std::move(config)} {
    // nop
  }

  /// Projects an arrow record batch.
  /// @returns The new schema and the projected record batch.
  caf::error add(table_slice slice) override {
    VAST_TRACE("select operator adds batch");
    auto indices = std::vector<offset>{};
    for (const auto& field : config_.fields)
      for (auto&& index : caf::get<record_type>(slice.schema())
                            .resolve_key_suffix(field, slice.schema().name()))
        indices.push_back(std::move(index));
    std::sort(indices.begin(), indices.end());
    slice = select_columns(slice, indices);
    transformed_.push_back(std::move(slice));
    return caf::none;
  }

  caf::expected<std::vector<table_slice>> finish() override {
    VAST_TRACE("select operator finished transformation");
    return std::exchange(transformed_, {});
  }

private:
  /// The slices being transformed.
  std::vector<table_slice> transformed_ = {};

  /// The underlying configuration of the transformation.
  configuration config_ = {};
};

class plugin final : public virtual pipeline_operator_plugin {
public:
  // plugin API
  caf::error initialize([[maybe_unused]] const record& plugin_config,
                        [[maybe_unused]] const record& global_config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "select";
  };

  // transform plugin API
  [[nodiscard]] caf::expected<std::unique_ptr<pipeline_operator>>
  make_pipeline_operator(const record& options) const override {
    if (!options.contains("fields"))
      return caf::make_error(ec::invalid_configuration,
                             "key 'fields' is missing in configuration for "
                             "select operator");
    auto config = to<configuration>(options);
    if (!config)
      return config.error();
    return std::make_unique<select_operator>(std::move(*config));
  }

  [[nodiscard]] std::pair<std::string_view,
                          caf::expected<std::unique_ptr<pipeline_operator>>>
  make_pipeline_operator(std::string_view pipeline) const override {
    using parsers::end_of_pipeline_operator, parsers::required_ws_or_comment,
      parsers::optional_ws_or_comment, parsers::extractor,
      parsers::extractor_char, parsers::extractor_list;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = required_ws_or_comment >> extractor_list
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    auto config = configuration{};
    if (!p(f, l, config.fields)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse select "
                                                      "operator: '{}'",
                                                      pipeline)),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<select_operator>(std::move(config)),
    };
  }
};

} // namespace

} // namespace vast::plugins::select

VAST_REGISTER_PLUGIN(vast::plugins::select::plugin)
