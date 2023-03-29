//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/arrow_table_slice.hpp>
#include <vast/concept/convertible/data.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/data.hpp>
#include <vast/concept/parseable/vast/option_set.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/detail/inspection_common.hpp>
#include <vast/ip.hpp>
#include <vast/legacy_pipeline_operator.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice_builder.hpp>
#include <vast/type.hpp>

#include <arrow/table.h>

namespace vast::plugins::pseudonymize {

/// The configuration of the pseudonymize pipeline operator.
struct configuration {
  // field for future extensibility; currently we only use the Crypto-PAn method
  std::string method;
  std::string seed;
  std::array<ip::byte_type, vast::ip::pseudonymization_seed_array_size>
    seed_bytes{};
  std::vector<std::string> fields;

  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return detail::apply_all(f, x.method, x.seed, x.fields);
  }

  static inline const record_type& schema() noexcept {
    static auto result = record_type{
      {"method", string_type{}},
      {"seed", string_type{}},
      {"fields", list_type{string_type{}}},
    };
    return result;
  }
};

class pseudonymize_operator : public legacy_pipeline_operator {
public:
  pseudonymize_operator(configuration config) : config_{std::move(config)} {
    parse_seed_string();
  }

  /// Applies the transformation to an Arrow Record Batch with a corresponding
  /// VAST schema.
  [[nodiscard]] caf::error add(table_slice slice) override {
    std::vector<indexed_transformation> transformations;
    auto transformation = [&](struct record_type::field field,
                              std::shared_ptr<arrow::Array> array) noexcept
      -> std::vector<
        std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>> {
      auto builder = ip_type::make_arrow_builder(arrow::default_memory_pool());
      auto address_view_generator
        = values(ip_type{}, caf::get<type_to_arrow_array_t<ip_type>>(*array));
      for (const auto& address : address_view_generator) {
        auto append_status = arrow::Status{};
        if (address) {
          auto pseudonymized_address
            = vast::ip::pseudonymize(*address, config_.seed_bytes);
          append_status
            = append_builder(ip_type{}, *builder, pseudonymized_address);
        } else {
          append_status = builder->AppendNull();
        }
        VAST_ASSERT(append_status.ok(), append_status.ToString().c_str());
      }
      auto new_array = builder->Finish().ValueOrDie();
      return {
        {field, new_array},
      };
    };
    for (const auto& field_name : config_.fields) {
      for (const auto& index :
           caf::get<record_type>(slice.schema())
             .resolve_key_suffix(field_name, slice.schema().name())) {
        auto index_type
          = caf::get<record_type>(slice.schema()).field(index).type;
        if (!caf::holds_alternative<ip_type>(index_type)) {
          VAST_DEBUG("pseudonymize operator skips field '{}' of unsupported "
                     "type '{}'",
                     field_name, index_type.name());
          continue;
        }
        transformations.push_back({index, std::move(transformation)});
      }
    }
    std::sort(transformations.begin(), transformations.end());
    transformations.erase(std::unique(transformations.begin(),
                                      transformations.end()),
                          transformations.end());
    transformed_.push_back(transform_columns(slice, transformations));
    return caf::none;
  }

  /// Retrieves the result of the transformation.
  [[nodiscard]] caf::expected<std::vector<table_slice>> finish() override {
    return std::exchange(transformed_, {});
  }

private:
  /// Cache for transformed batches.
  std::vector<table_slice> transformed_ = {};

  /// Step-specific configuration, including the seed and field names.
  configuration config_ = {};

  void parse_seed_string() {
    auto max_seed_size = std::min(
      vast::ip::pseudonymization_seed_array_size * 2, config_.seed.size());
    for (auto i = size_t{0}; (i * 2) < max_seed_size; ++i) {
      auto byte_string_pos = i * 2;
      auto byte_size = (byte_string_pos + 2 > config_.seed.size()) ? 1 : 2;
      auto byte = config_.seed.substr(byte_string_pos, byte_size);
      if (byte_size == 1) {
        byte.append("0");
      }
      config_.seed_bytes[i] = std::strtoul(byte.c_str(), 0, 16);
    }
  }
};

class pseudonymize_operator2 final
  : public schematic_operator<pseudonymize_operator2,
                              std::vector<indexed_transformation>> {
public:
  pseudonymize_operator2(configuration config) : config_{std::move(config)} {
    parse_seed_string();
  }

  auto initialize(const type& schema) const
    -> caf::expected<state_type> override {
    std::vector<indexed_transformation> transformations;
    auto transformation = [&](struct record_type::field field,
                              std::shared_ptr<arrow::Array> array) noexcept
      -> std::vector<
        std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>> {
      auto builder = ip_type::make_arrow_builder(arrow::default_memory_pool());
      auto address_view_generator
        = values(ip_type{}, caf::get<type_to_arrow_array_t<ip_type>>(*array));
      for (const auto& address : address_view_generator) {
        auto append_status = arrow::Status{};
        if (address) {
          auto pseudonymized_address
            = vast::ip::pseudonymize(*address, config_.seed_bytes);
          append_status
            = append_builder(ip_type{}, *builder, pseudonymized_address);
        } else {
          append_status = builder->AppendNull();
        }
        VAST_ASSERT(append_status.ok(), append_status.ToString().c_str());
      }
      auto new_array = builder->Finish().ValueOrDie();
      return {
        {field, new_array},
      };
    };
    for (const auto& field_name : config_.fields) {
      for (const auto& index : caf::get<record_type>(schema).resolve_key_suffix(
             field_name, schema.name())) {
        auto index_type = caf::get<record_type>(schema).field(index).type;
        if (!caf::holds_alternative<ip_type>(index_type)) {
          VAST_DEBUG("pseudonymize operator skips field '{}' of unsupported "
                     "type '{}'",
                     field_name, index_type.name());
          continue;
        }
        transformations.push_back({index, std::move(transformation)});
      }
    }
    std::sort(transformations.begin(), transformations.end());
    transformations.erase(std::unique(transformations.begin(),
                                      transformations.end()),
                          transformations.end());
    return transformations;
  }

  auto process(table_slice slice, state_type& state) const
    -> output_type override {
    return transform_columns(slice, state);
  }

  auto to_string() const -> std::string override {
    auto result
      = fmt::format("pseudonymize --method=\"{}\" ",
                    config_.method.empty() ? "crypto-pan" : config_.method);
    if (!config_.seed.empty()) {
      result += fmt::format("--seed=\"{}\" ", config_.seed);
    }
    result += fmt::to_string(fmt::join(config_.fields, ", "));
    return result;
  }

private:
  /// Step-specific configuration, including the seed and field names.
  configuration config_ = {};

  void parse_seed_string() {
    auto max_seed_size = std::min(
      vast::ip::pseudonymization_seed_array_size * 2, config_.seed.size());
    for (auto i = size_t{0}; (i * 2) < max_seed_size; ++i) {
      auto byte_string_pos = i * 2;
      auto byte_size = (byte_string_pos + 2 > config_.seed.size()) ? 1 : 2;
      auto byte = config_.seed.substr(byte_string_pos, byte_size);
      if (byte_size == 1) {
        byte.append("0");
      }
      config_.seed_bytes[i] = std::strtoul(byte.c_str(), 0, 16);
    }
  }
};

// -- plugin ------------------------------------------------------------------

class plugin final : public virtual pipeline_operator_plugin,
                     public virtual operator_plugin {
public:
  caf::error initialize([[maybe_unused]] const record& plugin_config,
                        [[maybe_unused]] const record& global_config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "pseudonymize";
  };

  [[nodiscard]] caf::expected<std::unique_ptr<legacy_pipeline_operator>>
  make_pipeline_operator(const record& options) const override {
    if (options.size() != 3) {
      return caf::make_error(ec::invalid_configuration,
                             "Configuration under vast.plugins.pseudonymize "
                             "must "
                             "only contain 'method', 'seed' and 'fields' keys");
    }
    if (!options.contains("method")) {
      return caf::make_error(ec::invalid_configuration,
                             "Configuration under vast.plugins.pseudonymize "
                             "does not contain 'method' key");
    }
    if (!options.contains("seed")) {
      return caf::make_error(ec::invalid_configuration,
                             "Configuration under vast.plugins.pseudonymize "
                             "does not contain 'seed' key");
    }
    if (!options.contains("fields")) {
      return caf::make_error(ec::invalid_configuration,
                             "Configuration under vast.plugins.pseudonymize "
                             "does not contain 'fields' key");
    }
    auto config = to<configuration>(options);
    if (!config)
      return config.error();
    if (std::any_of(config->seed.begin(), config->seed.end(), [](auto c) {
          return !std::isxdigit(c);
        })) {
      return caf::make_error(ec::invalid_configuration,
                             "vast.plugins.pseudonymize.seed must"
                             "contain a hexadecimal value");
    }
    return std::make_unique<pseudonymize_operator>(std::move(*config));
  }

  [[nodiscard]] std::pair<
    std::string_view, caf::expected<std::unique_ptr<legacy_pipeline_operator>>>
  make_pipeline_operator(std::string_view pipeline) const override {
    using parsers::end_of_pipeline_operator, parsers::required_ws_or_comment,
      parsers::optional_ws_or_comment, parsers::extractor,
      parsers::extractor_list;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto options = option_set_parser{{{"method", 'm'}, {"seed", 's'}}};
    const auto option_parser = required_ws_or_comment >> options;
    auto parsed_options = std::unordered_map<std::string, data>{};
    if (!option_parser(f, l, parsed_options)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse "
                                                      "pseudonymize "
                                                      "operator options: '{}'",
                                                      pipeline)),
      };
    }
    const auto extractor_parser
      = extractor_list >> optional_ws_or_comment >> end_of_pipeline_operator;
    auto parsed_extractors = std::vector<std::string>{};
    if (!extractor_parser(f, l, parsed_extractors)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse "
                                                      "pseudonymize "
                                                      "operator extractor: "
                                                      "'{}'",
                                                      pipeline)),
      };
    }
    auto config = configuration{};
    config.fields = std::move(parsed_extractors);
    for (const auto& [key, value] : parsed_options) {
      auto value_str = caf::get_if<std::string>(&value);
      if (!value_str) {
        return {
          std::string_view{f, l},
          caf::make_error(ec::syntax_error, fmt::format("invalid option value "
                                                        "string for "
                                                        "pseudonymize "
                                                        "operator: "
                                                        "'{}'",
                                                        value)),
        };
      }
      if (key == "m" || key == "method") {
        config.method = *value_str;
      } else if (key == "s" || key == "seed") {
        config.seed = *value_str;
      }
    }
    return {
      std::string_view{f, l},
      std::make_unique<pseudonymize_operator>(std::move(config)),
    };
  }

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::end_of_pipeline_operator, parsers::required_ws_or_comment,
      parsers::optional_ws_or_comment, parsers::extractor,
      parsers::extractor_list;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto options = option_set_parser{{{"method", 'm'}, {"seed", 's'}}};
    const auto option_parser = required_ws_or_comment >> options;
    auto parsed_options = std::unordered_map<std::string, data>{};
    if (!option_parser(f, l, parsed_options)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse "
                                                      "pseudonymize "
                                                      "operator options: '{}'",
                                                      pipeline)),
      };
    }
    const auto extractor_parser
      = extractor_list >> optional_ws_or_comment >> end_of_pipeline_operator;
    auto parsed_extractors = std::vector<std::string>{};
    if (!extractor_parser(f, l, parsed_extractors)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse "
                                                      "pseudonymize "
                                                      "operator extractor: "
                                                      "'{}'",
                                                      pipeline)),
      };
    }
    auto config = configuration{};
    config.fields = std::move(parsed_extractors);
    for (const auto& [key, value] : parsed_options) {
      auto value_str = caf::get_if<std::string>(&value);
      if (!value_str) {
        return {
          std::string_view{f, l},
          caf::make_error(ec::syntax_error, fmt::format("invalid option value "
                                                        "string for "
                                                        "pseudonymize "
                                                        "operator: "
                                                        "'{}'",
                                                        value)),
        };
      }
      if (key == "m" || key == "method") {
        config.method = *value_str;
      } else if (key == "s" || key == "seed") {
        config.seed = *value_str;
      }
    }
    return {
      std::string_view{f, l},
      std::make_unique<pseudonymize_operator2>(std::move(config)),
    };
  }
};

} // namespace vast::plugins::pseudonymize

VAST_REGISTER_PLUGIN(vast::plugins::pseudonymize::plugin)
