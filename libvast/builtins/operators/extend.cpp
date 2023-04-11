//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
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

namespace vast::plugins::extend {

namespace {

/// The parsed configuration.
struct configuration {
  std::vector<std::tuple<std::string, data>> field_to_value = {};
};

/// The confguration bound to a specific schema.
struct bound_configuration {
  /// Bind a *configuration* to a given schema.
  /// @param schema The schema to bind to.
  /// @param config The parsed configuration.
  /// @param ctrl The operator control plane.
  static auto make(const type& schema, const configuration& config,
                   operator_control_plane& ctrl)
    -> caf::expected<bound_configuration> {
    auto result = bound_configuration{};
    const auto& schema_rt = caf::get<record_type>(schema);
    auto extensions = std::vector<std::tuple<std::string, data, type>>{};
    for (const auto& [extractor, value] : config.field_to_value) {
      if (schema_rt.resolve_key(extractor).has_value()) {
        ctrl.warn(caf::make_error(
          ec::invalid_argument,
          fmt::format("extend operator ignores assignment '{}={}' "
                      "as the field already exists in the schema {}",
                      extractor, value, schema)));
        continue;
      }
      auto inferred_type = type::infer(value);
      if (not inferred_type)
        return caf::make_error(ec::logic_error,
                               fmt::format("failed to infer type from '{}'",
                                           value));
      extensions.emplace_back(extractor, value, std::move(inferred_type));
    }
    // We maintain two separate lists of column transformations because we
    // cannot both modify the last column and add additional columns in a single
    // call to transform_columns, because that would modify a precondition of
    // the function.
    if (not extensions.empty())
      result.extensions.push_back(
        {{schema_rt.num_fields() - 1}, make_extend(std::move(extensions))});
    VAST_ASSERT_CHEAP(result.extensions.size() <= 1);
    return result;
  }

  /// Create a transformation function for a single value.
  static auto
  make_extend(std::vector<std::tuple<std::string, data, type>> extensions)
    -> indexed_transformation::function_type {
    return [extensions = std::move(extensions)](
             struct record_type::field field,
             std::shared_ptr<arrow::Array> array) noexcept {
      auto result = std::vector<
        std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>>{};
      const auto length = array->length();
      result.reserve(extensions.size() + 1);
      result.emplace_back(std::move(field), std::move(array));
      for (const auto& [name, value, type] : extensions) {
        auto& extension = result.emplace_back();
        extension.first.name = name;
        extension.first.type = type;
        extension.second = make_array(type, value, length);
      }
      return result;
    };
  }

  static auto make_array(const type& type, const data& value, int64_t length)
    -> std::shared_ptr<arrow::Array> {
    auto builder = type.make_arrow_builder(arrow::default_memory_pool());
    auto f = [&]<concrete_type Type>(const Type& type) {
      if (caf::holds_alternative<caf::none_t>(value)) {
        for (int i = 0; i < length; ++i) {
          const auto append_status = builder->AppendNull();
          VAST_ASSERT(append_status.ok(), append_status.ToString().c_str());
        }
      } else {
        for (int i = 0; i < length; ++i) {
          VAST_ASSERT(caf::holds_alternative<type_to_data_t<Type>>(value));
          const auto append_status
            = append_builder(type,
                             caf::get<type_to_arrow_builder_t<Type>>(*builder),
                             make_view(caf::get<type_to_data_t<Type>>(value)));
          VAST_ASSERT(append_status.ok(), append_status.ToString().c_str());
        }
      }
    };
    caf::visit(f, type);
    return builder->Finish().ValueOrDie();
  }

  /// The list of configured transformations.
  std::vector<indexed_transformation> extensions = {};
};

class extend_operator final
  : public schematic_operator<extend_operator, bound_configuration> {
public:
  explicit extend_operator(configuration config) noexcept
    : config_{std::move(config)} {
    // nop
  }

  auto initialize(const type& schema, operator_control_plane& ctrl) const
    -> caf::expected<state_type> override {
    return bound_configuration::make(schema, config_, ctrl);
  }

  auto process(table_slice slice, state_type& state) const
    -> output_type override {
    if (!state.extensions.empty())
      slice = transform_columns(slice, state.extensions);
    return slice;
  }

  [[nodiscard]] auto to_string() const noexcept -> std::string override {
    auto map = std::vector<std::tuple<std::string, data>>{
      config_.field_to_value.begin(), config_.field_to_value.end()};
    std::sort(map.begin(), map.end());
    auto result = std::string{"extend"};
    bool first = true;
    for (auto& [key, value] : map) {
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

class plugin final : public virtual operator_plugin {
public:
  auto initialize([[maybe_unused]] const record& plugin_config,
                  [[maybe_unused]] const record& global_config)
    -> caf::error override {
    return {};
  }

  auto name() const -> std::string override {
    return "extend";
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::end_of_pipeline_operator, parsers::required_ws_or_comment,
      parsers::optional_ws_or_comment, parsers::extractor_value_assignment_list,
      parsers::data;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = required_ws_or_comment >> extractor_value_assignment_list
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    auto config = configuration{};
    if (!p(f, l, config.field_to_value)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse extend "
                                                      "operator: '{}'",
                                                      pipeline)),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<extend_operator>(std::move(config)),
    };
  }
};

} // namespace

} // namespace vast::plugins::extend

VAST_REGISTER_PLUGIN(vast::plugins::extend::plugin)
