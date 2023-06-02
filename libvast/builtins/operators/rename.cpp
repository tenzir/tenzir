//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/arrow_table_slice.hpp>
#include <vast/cast.hpp>
#include <vast/concept/convertible/data.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/data.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/detail/inspection_common.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice_builder.hpp>
#include <vast/type.hpp>

#include <arrow/table.h>

namespace vast::plugins::rename {

/// The configuration of the rename pipeline operator.
struct configuration {
  struct name_mapping {
    std::string from = {};
    std::string to = {};

    template <class Inspector>
    friend auto inspect(Inspector& f, name_mapping& x) {
      return detail::apply_all(f, x.from, x.to);
    }

    static inline const record_type& schema() noexcept {
      static auto result = record_type{
        {"from", string_type{}},
        {"to", string_type{}},
      };
      return result;
    }
  };

  std::vector<name_mapping> schemas = {};
  std::vector<name_mapping> fields = {};

  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return detail::apply_all(f, x.schemas, x.fields);
  }

  static inline const record_type& schema() noexcept {
    // schemas:
    //   - from: zeek.conn
    //     to: zeek.aggregated_conn
    //   - from: suricata.flow
    //     to: suricata.aggregated_flow
    // fields:
    //   - from: resp_h
    //     to: response_h
    static auto result = record_type{
      {"schemas", list_type{name_mapping::schema()}},
      {"fields", list_type{name_mapping::schema()}},
    };
    return result;
  }
};

struct state_t {
  std::vector<indexed_transformation> field_transformations;
  std::optional<type> renamed_schema;
};

class rename_operator final
  : public schematic_operator<rename_operator, state_t> {
public:
  rename_operator() = default;

  rename_operator(configuration config) : config_{std::move(config)} {
    // nop
  }

  auto initialize(const type& schema, operator_control_plane&) const
    -> caf::expected<state_type> override {
    // Step 1: Adjust field names.
    auto field_transformations = std::vector<indexed_transformation>{};
    if (!config_.fields.empty()) {
      for (const auto& field : config_.fields) {
        for (const auto& index :
             caf::get<record_type>(schema).resolve_key_suffix(field.from,
                                                              schema.name())) {
          auto transformation
            = [&](struct record_type::field old_field,
                  std::shared_ptr<arrow::Array> array) noexcept
            -> std::vector<std::pair<struct record_type::field,
                                     std::shared_ptr<arrow::Array>>> {
            return {
              {{field.to, old_field.type}, array},
            };
          };
          field_transformations.push_back({index, std::move(transformation)});
        }
      }
      std::sort(field_transformations.begin(), field_transformations.end());
    }
    // Step 2: Adjust schema names.
    std::optional<type> renamed_schema;
    if (!config_.schemas.empty()) {
      const auto schema_mapping
        = std::find_if(config_.schemas.begin(), config_.schemas.end(),
                       [&](const auto& name_mapping) noexcept {
                         return name_mapping.from == schema.name();
                       });
      if (schema_mapping != config_.schemas.end()) {
        auto rename_schema = [&](const concrete_type auto& pruned_schema) {
          VAST_ASSERT(!schema.has_attributes());
          return type{schema_mapping->to, pruned_schema};
        };
        renamed_schema = caf::visit(rename_schema, schema);
      }
    }
    return state_t{std::move(field_transformations), std::move(renamed_schema)};
  }

  auto process(table_slice slice, state_type& state) const
    -> output_type override {
    slice = transform_columns(slice, state.field_transformations);
    if (state.renamed_schema) {
      slice = cast(std::move(slice), *state.renamed_schema);
    }
    return slice;
  }

  auto to_string() const noexcept -> std::string override {
    auto result = std::string{"rename"};
    auto first = true;
    for (auto& mapping : config_.schemas) {
      if (first) {
        first = false;
      } else {
        result += ',';
      }
      result += fmt::format(" {}=:{}", mapping.to, mapping.from);
    }
    for (auto& mapping : config_.fields) {
      if (first) {
        first = false;
      } else {
        result += ',';
      }
      result += fmt::format(" {}={}", mapping.to, mapping.from);
    }
    return result;
  }

  auto name() const -> std::string override {
    return "rename";
  }

  friend auto inspect(auto& f, rename_operator& x) -> bool {
    return f.apply(x.config_);
  }

private:
  /// Step-specific configuration, including the schema name mapping.
  configuration config_ = {};
};

// -- plugin ------------------------------------------------------------------

class plugin final : public virtual operator_plugin<rename_operator> {
public:
  auto initialize(const record& plugin_config,
                  [[maybe_unused]] const record& global_config)
    -> caf::error override {
    // We don't use any plugin-specific configuration under
    // vast.plugins.rename, so nothing is needed here.
    if (plugin_config.empty()) {
      return caf::none;
    }
    return caf::make_error(ec::invalid_configuration, "expected empty "
                                                      "configuration under "
                                                      "vast.plugins.rename");
  }

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::end_of_pipeline_operator, parsers::required_ws_or_comment,
      parsers::optional_ws_or_comment, parsers::extractor_assignment_list;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = required_ws_or_comment >> extractor_assignment_list
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    std::vector<std::tuple<std::string, std::string>> parsed_assignments;
    if (!p(f, l, parsed_assignments)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse extend "
                                                      "operator: '{}'",
                                                      pipeline)),
      };
    }
    auto config = configuration{};
    for (const auto& [to, from] : parsed_assignments) {
      if (from.starts_with(':')) {
        config.schemas.push_back({from.substr(1), to});
      } else {
        config.fields.push_back({from, to});
      }
    }
    return {
      std::string_view{f, l},
      std::make_unique<rename_operator>(std::move(config)),
    };
  }
};

} // namespace vast::plugins::rename

VAST_REGISTER_PLUGIN(vast::plugins::rename::plugin)
