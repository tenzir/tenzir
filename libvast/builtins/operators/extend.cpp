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
#include <vast/detail/narrow.hpp>
#include <vast/error.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice_builder.hpp>
#include <vast/type.hpp>

#include <arrow/array.h>
#include <fmt/format.h>

namespace vast::plugins::extend {

namespace {

/// The parsed configuration.
struct configuration {
  static caf::expected<configuration> make(const record& config) {
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
    result.fields.reserve(fields->size());
    for (const auto& [key, _] : *fields)
      result.fields.emplace_back(key);
    result.transformation = [fields = *fields](
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
        auto reparsed_value = [](auto value) {
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

  std::vector<std::string> fields = {};
  indexed_transformation::function_type transformation = {};
};

class extend_operator : public pipeline_operator {
public:
  explicit extend_operator(configuration config) noexcept
    : config_{std::move(config)} {
    // nop
  }

  caf::error
  add(type schema, std::shared_ptr<arrow::RecordBatch> batch) override {
    const auto& schema_rt = caf::get<record_type>(schema);
    for (const auto& field : config_.fields)
      if (schema_rt.resolve_key(field).has_value())
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("cannot extend {} with field {} "
                                           "as it already has a field with "
                                           "this name",
                                           schema, field));
    auto [adjusted_layout, adjusted_batch] = transform_columns(
      schema, batch,
      {{offset{caf::get<record_type>(schema).num_fields() - 1},
        config_.transformation}});
    VAST_ASSERT(adjusted_layout);
    VAST_ASSERT(adjusted_batch);
    transformed_.emplace_back(std::move(adjusted_layout),
                              std::move(adjusted_batch));
    return caf::none;
  }

  caf::expected<std::vector<pipeline_batch>> finish() override {
    return std::exchange(transformed_, {});
  }

private:
  /// The transformed slices.
  std::vector<pipeline_batch> transformed_ = {};

  /// The underlying configuration of the transformation.
  configuration config_ = {};
};

class plugin final : public virtual pipeline_operator_plugin {
public:
  caf::error initialize(data) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "extend";
  };

  [[nodiscard]] caf::expected<std::unique_ptr<pipeline_operator>>
  make_pipeline_operator(const record& config) const override {
    auto parsed_config = configuration::make(config);
    if (!parsed_config)
      return parsed_config.error();
    return std::make_unique<extend_operator>(std::move(*parsed_config));
  }
};

} // namespace

} // namespace vast::plugins::extend

VAST_REGISTER_PLUGIN(vast::plugins::extend::plugin)
