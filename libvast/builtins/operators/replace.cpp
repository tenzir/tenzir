//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/arrow_table_slice.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/data.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/error.hpp>
#include <vast/legacy_pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice_builder.hpp>
#include <vast/type.hpp>

#include <arrow/array.h>
#include <fmt/format.h>

namespace vast::plugins::replace {

namespace {

/// The parsed configuration.
struct configuration {
  static caf::expected<configuration> make(const record& config) {
    if (config.size() != 1 || !config.contains("fields"))
      return caf::make_error(ec::invalid_configuration, "replace configuration "
                                                        "must contain only the "
                                                        "'fields' key");
    const auto* fields = caf::get_if<record>(&config.at("fields"));
    if (!fields)
      return caf::make_error(ec::invalid_configuration, "'fields' key in "
                                                        "replace configuration "
                                                        "must be a record");
    auto result = configuration{};
    for (const auto& [key, value] : *fields)
      result.extractor_to_value.emplace(key, value);
    return result;
  }

  std::unordered_map<std::string, data> extractor_to_value = {};
  bool reparse_values{true};
};

/// The confguration bound to a specific schema.
struct bound_configuration {
  /// Bind a *configuration* to a given schema.
  /// @param schema The schema to bind to.
  /// @param config The parsed configuration.
  static caf::expected<bound_configuration>
  make(const type& schema, const configuration& config) {
    auto result = bound_configuration{};
    const auto& schema_rt = caf::get<record_type>(schema);
    for (const auto& [extractor, value] : config.extractor_to_value) {
      // The config parsing never produces all possible alternatives of the data
      // variant, e.g., addresses will be represented as strings. Because of
      // that we need to re-parse the data if it's a string.
      auto reparsed_value = [&config](auto value) {
        if (!config.reparse_values) {
          return value;
        }
        const auto* str = caf::get_if<std::string>(&value);
        if (!str)
          return value;
        auto result = to<data>(*str);
        if (!result)
          return value;
        return std::move(*result);
      }(value);
      for (const auto& index :
           schema_rt.resolve_key_suffix(extractor, schema.name()))
        result.transformations.push_back(
          {index, make_transformation(reparsed_value)});
    }
    std::sort(result.transformations.begin(), result.transformations.end());
    result.transformations.erase(std::unique(result.transformations.begin(),
                                             result.transformations.end()),
                                 result.transformations.end());
    return result;
  }

  /// Create a transformation function for a single value.
  static indexed_transformation::function_type make_transformation(data value) {
    auto inferred_type = type::infer(value);
    return
      [inferred_type = std::move(inferred_type),
       value = std::move(value)](struct record_type::field field,
                                 std::shared_ptr<arrow::Array> array) noexcept
      -> std::vector<
        std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>> {
      field.type = inferred_type;
      auto builder
        = field.type.make_arrow_builder(arrow::default_memory_pool());
      auto f = [&]<concrete_type Type>(const Type& type) {
        if (caf::holds_alternative<caf::none_t>(value)) {
          for (int i = 0; i < array->length(); ++i) {
            const auto append_status = builder->AppendNull();
            VAST_ASSERT(append_status.ok(), append_status.ToString().c_str());
          }
        } else {
          for (int i = 0; i < array->length(); ++i) {
            VAST_ASSERT(caf::holds_alternative<type_to_data_t<Type>>(value));
            const auto append_status = append_builder(
              type, caf::get<type_to_arrow_builder_t<Type>>(*builder),
              make_view(caf::get<type_to_data_t<Type>>(value)));
            VAST_ASSERT(append_status.ok(), append_status.ToString().c_str());
          }
        }
      };
      caf::visit(f, field.type);
      array = builder->Finish().ValueOrDie();
      return {
        {
          std::move(field),
          std::move(array),
        },
      };
    };
  }

  /// The list of configured transformations.
  std::vector<indexed_transformation> transformations = {};
};

class replace_operator : public legacy_pipeline_operator {
public:
  explicit replace_operator(configuration config) noexcept
    : config_{std::move(config)} {
    // nop
  }

  caf::error add(table_slice slice) override {
    auto config = bound_config_.find(slice.schema());
    if (config == bound_config_.end()) {
      auto new_config = bound_configuration::make(slice.schema(), config_);
      if (!new_config)
        return new_config.error();
      auto [it, inserted] = bound_config_.emplace(
        type{chunk::copy(slice.schema())}, std::move(*new_config));
      VAST_ASSERT(inserted);
      config = it;
    }
    transformed_.push_back(
      transform_columns(slice, config->second.transformations));
    return caf::none;
  }

  caf::expected<std::vector<table_slice>> finish() override {
    return std::exchange(transformed_, {});
  }

private:
  /// The transformed slices.
  std::vector<table_slice> transformed_ = {};

  /// The underlying configuration of the transformation.
  configuration config_ = {};

  /// The configuration bound to a specific schema.
  std::unordered_map<type, bound_configuration> bound_config_ = {};
};

class plugin final : public virtual pipeline_operator_plugin {
public:
  caf::error initialize([[maybe_unused]] const record& plugin_config,
                        [[maybe_unused]] const record& global_config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "replace";
  };

  [[nodiscard]] caf::expected<std::unique_ptr<legacy_pipeline_operator>>
  make_pipeline_operator(const record& config) const override {
    auto parsed_config = configuration::make(config);
    if (!parsed_config)
      return parsed_config.error();
    return std::make_unique<replace_operator>(std::move(*parsed_config));
  }

  [[nodiscard]] std::pair<
    std::string_view, caf::expected<std::unique_ptr<legacy_pipeline_operator>>>
  make_pipeline_operator(std::string_view pipeline) const override {
    using parsers::end_of_pipeline_operator, parsers::required_ws_or_comment,
      parsers::optional_ws_or_comment, parsers::extractor_value_assignment_list,
      parsers::data;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = required_ws_or_comment >> extractor_value_assignment_list
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    std::vector<std::tuple<std::string, vast::data>> parsed_assignments;
    if (!p(f, l, parsed_assignments)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse extend "
                                                      "operator: '{}'",
                                                      pipeline)),
      };
    }
    record config_record;
    record fields_record;
    for (const auto& [key, data] : parsed_assignments) {
      fields_record[key] = data;
    }
    config_record["fields"] = std::move(fields_record);
    auto config = configuration::make(std::move(config_record));
    if (!config) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to generate "
                                                      "configuration for "
                                                      "extend "
                                                      "operator: '{}'",
                                                      config.error())),
      };
    }
    config->reparse_values = false;
    return {
      std::string_view{f, l},
      std::make_unique<replace_operator>(std::move(*config)),
    };
  }
};

} // namespace

} // namespace vast::plugins::replace

VAST_REGISTER_PLUGIN(vast::plugins::replace::plugin)
