//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/arrow_table_slice.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/data.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/error.hpp>
#include <vast/legacy_pipeline_operator.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice_builder.hpp>
#include <vast/type.hpp>

#include <arrow/array.h>
#include <fmt/format.h>

namespace vast::plugins::extend {

namespace {

/// The parsed configuration.
struct configuration {
  static caf::expected<configuration>
  make(const record& config, bool reparse_values = true) {
    if (config.size() != 1 || !config.contains("fields"))
      return caf::make_error(ec::invalid_configuration, "extend configuration "
                                                        "must contain only the "
                                                        "'fields' key");
    const auto* fields = caf::get_if<record>(&config.at("fields"));
    if (!fields)
      return caf::make_error(ec::invalid_configuration, "'fields' key in "
                                                        "extend configuration "
                                                        "must be a record");
    auto result = configuration{};
    for (const auto& [key, value] : *fields)
      result.field_to_value.emplace_back(key, value);
    result.transformation = [fields = *fields, reparse_values](
                              struct record_type::field field,
                              std::shared_ptr<arrow::Array> array) noexcept {
      const auto length = array->length();
      auto result = std::vector<
        std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>>{
        {std::move(field), std::move(array)},
      };
      for (const auto& kvp : fields) {
        auto& entry = result.emplace_back();
        entry.first.name = kvp.first;
        // The config parsing never produces all possible alternatives of the
        // data variant, e.g., addresses will be represented as strings. Because
        // of that we need to re-parse the data if it's a string.
        auto reparsed_value = [reparse_values](auto value) {
          if (!reparse_values) {
            return value;
          }
          const auto* str = caf::get_if<std::string>(&value);
          if (!str)
            return value;
          auto result = to<data>(*str);
          if (!result)
            return value;
          return std::move(*result);
        }(kvp.second);
        entry.first.type = type::infer(reparsed_value);
        VAST_ASSERT(entry.first.type);
        auto builder
          = entry.first.type.make_arrow_builder(arrow::default_memory_pool());
        auto f = [&]<concrete_type Type>(const Type& type) {
          if (caf::holds_alternative<caf::none_t>(kvp.second)) {
            for (int i = 0; i < length; ++i) {
              const auto append_status = builder->AppendNull();
              VAST_ASSERT(append_status.ok(), append_status.ToString().c_str());
            }
          } else {
            for (int i = 0; i < length; ++i) {
              VAST_ASSERT(
                caf::holds_alternative<type_to_data_t<Type>>(reparsed_value));
              const auto append_status = append_builder(
                type, caf::get<type_to_arrow_builder_t<Type>>(*builder),
                make_view(caf::get<type_to_data_t<Type>>(reparsed_value)));
              VAST_ASSERT(append_status.ok(), append_status.ToString().c_str());
            }
          }
        };
        caf::visit(f, entry.first.type);
        entry.second = builder->Finish().ValueOrDie();
      }
      return result;
    };
    return result;
  }

  // Note: `data` is only available when using pipeline syntax.
  std::vector<std::pair<std::string, data>> field_to_value = {};
  indexed_transformation::function_type transformation = {};
};

class extend_operator : public legacy_pipeline_operator {
public:
  explicit extend_operator(configuration config) noexcept
    : config_{std::move(config)} {
    // nop
  }

  caf::error add(table_slice slice) override {
    const auto& schema_rt = caf::get<record_type>(slice.schema());
    for (const auto& [field, _] : config_.field_to_value)
      if (schema_rt.resolve_key(field).has_value())
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("cannot extend {} with field {} "
                                           "as it already has a field with "
                                           "this name",
                                           slice.schema(), field));
    transformed_.push_back(transform_columns(
      slice, {{offset{schema_rt.num_fields() - 1}, config_.transformation}}));
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
};

class extend_operator2 final
  : public schematic_operator<extend_operator2,
                              std::vector<indexed_transformation>> {
public:
  explicit extend_operator2(configuration config) noexcept
    : config_{std::move(config)} {
    // nop
  }

  auto initialize(const type& schema, operator_control_plane&) const
    -> caf::expected<state_type> override {
    auto& schema_rt = caf::get<record_type>(schema);
    for (const auto& [field, _] : config_.field_to_value)
      if (schema_rt.resolve_key(field).has_value())
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("cannot extend {} with field {} "
                                           "as it already has a field with "
                                           "this name",
                                           schema, field));
    return std::vector<indexed_transformation>{
      {offset{schema_rt.num_fields() - 1}, config_.transformation}};
  }

  auto process(table_slice slice, state_type& state) const
    -> output_type override {
    return transform_columns(slice, state);
  }

  [[nodiscard]] auto to_string() const noexcept -> std::string override {
    auto result = std::string{"extend"};
    bool first = true;
    for (auto& [key, value] : config_.field_to_value) {
      if (first) {
        first = false;
      } else {
        result += ',';
      }
      result += fmt::format(" {}={}", key, value);
    }
    return result;
  }

private:
  /// The underlying configuration of the transformation.
  configuration config_ = {};
};

class plugin final : public virtual pipeline_operator_plugin,
                     public virtual operator_plugin {
public:
  caf::error initialize([[maybe_unused]] const record& plugin_config,
                        [[maybe_unused]] const record& global_config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "extend";
  };

  [[nodiscard]] caf::expected<std::unique_ptr<legacy_pipeline_operator>>
  make_pipeline_operator(const record& config) const override {
    auto parsed_config = configuration::make(config);
    if (!parsed_config)
      return parsed_config.error();
    return std::make_unique<extend_operator>(std::move(*parsed_config));
  }

  [[nodiscard]] std::pair<
    std::string_view, caf::expected<std::unique_ptr<legacy_pipeline_operator>>>
  make_pipeline_operator(std::string_view pipeline) const override {
    using parsers::optional_ws_or_comment, parsers::required_ws_or_comment,
      parsers::data, parsers::end_of_pipeline_operator,
      parsers::extractor_value_assignment_list;
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
    auto config = configuration::make(std::move(config_record), false);
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
    return {
      std::string_view{f, l},
      std::make_unique<extend_operator>(std::move(*config)),
    };
  }

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::optional_ws_or_comment, parsers::required_ws_or_comment,
      parsers::data, parsers::end_of_pipeline_operator,
      parsers::extractor_value_assignment_list;
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
    auto config = configuration::make(std::move(config_record), false);
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
    return {
      std::string_view{f, l},
      std::make_unique<extend_operator2>(std::move(*config)),
    };
  }
};

} // namespace

} // namespace vast::plugins::extend

VAST_REGISTER_PLUGIN(vast::plugins::extend::plugin)
