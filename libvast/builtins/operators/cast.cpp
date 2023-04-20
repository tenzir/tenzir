//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/pipeline.hpp"

#include <vast/cast.hpp>
#include <vast/concept/parseable/string/char_class.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/plugin.hpp>

#include <arrow/type.h>

namespace vast::plugins::cast {

namespace {

class cast_operator final : public schematic_operator<cast_operator, type> {
public:
  explicit cast_operator(std::string schema_name)
    : schema_name_{std::move(schema_name)} {
  }

  auto initialize(const type& input_schema, operator_control_plane& ctrl) const
    -> caf::expected<state_type> override {
    auto output_schema = std::find_if(
      ctrl.schemas().begin(), ctrl.schemas().end(), [this](const auto& schema) {
        return schema.name() == schema_name_;
      });
    if (output_schema == ctrl.schemas().end()) {
      return caf::make_error(
        ec::invalid_argument,
        fmt::format("cast operator failed to find schema '{}'", schema_name_));
    }
    auto castable = can_cast(input_schema, *output_schema);
    if (not castable) {
      return caf::make_error(
        ec::invalid_argument,
        fmt::format("cast operator cannot cast from '{}' to '{}': {}",
                    input_schema, schema_name_, castable.error()));
    }
    return *output_schema;
  }

  auto process(table_slice slice, state_type& output_schema) const
    -> table_slice override {
    return vast::cast(slice, output_schema);
  }

  auto to_string() const -> std::string override {
    return fmt::format("cast {}", schema_name_);
  }

private:
  std::string schema_name_ = {};
};

class plugin final : public virtual operator_plugin {
public:
  // plugin API
  auto initialize([[maybe_unused]] const record& plugin_config,
                  [[maybe_unused]] const record& global_config)
    -> caf::error override {
    return {};
  }

  auto name() const -> std::string override {
    return "cast";
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::optional_ws_or_comment, parsers::end_of_pipeline_operator,
      parsers::identifier;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = optional_ws_or_comment >> identifier
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    auto schema_name = std::string{};
    if (!p(f, l, schema_name)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse "
                                                      "cast operator: '{}'",
                                                      pipeline)),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<cast_operator>(std::move(schema_name)),
    };
  }
};

} // namespace

} // namespace vast::plugins::cast

VAST_REGISTER_PLUGIN(vast::plugins::cast::plugin)
