//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/concept/convertible/data.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/concept/parseable/tenzir/option_set.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/detail/inspection_common.hpp>
#include <tenzir/ip.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/type.hpp>

#include <arrow/table.h>

namespace tenzir::plugins::pseudonymize {

/// The configuration of the pseudonymize pipeline operator.
struct configuration {
  // field for future extensibility; currently we only use the Crypto-PAn method
  std::string method;
  std::array<ip::byte_type, tenzir::ip::pseudonymization_seed_array_size>
    seed_bytes{};
  std::vector<std::string> fields;

  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return f.object(x).fields(f.field("method", x.method),
                              f.field("seed_bytes", x.seed_bytes),
                              f.field("fields", x.fields));
  }
};

class pseudonymize_operator final
  : public schematic_operator<pseudonymize_operator,
                              std::vector<indexed_transformation>> {
public:
  pseudonymize_operator() = default;

  explicit pseudonymize_operator(configuration config)
    : config_{std::move(config)} {
  }

  auto initialize(const type& schema, exec_ctx) const
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
            = tenzir::ip::pseudonymize(*address, config_.seed_bytes);
          append_status
            = append_builder(ip_type{}, *builder, pseudonymized_address);
        } else {
          append_status = builder->AppendNull();
        }
        TENZIR_ASSERT(append_status.ok(), append_status.ToString().c_str());
      }
      auto new_array = builder->Finish().ValueOrDie();
      return {
        {field, new_array},
      };
    };
    for (const auto& field_name : config_.fields) {
      if (auto index = schema.resolve_key_or_concept_once(field_name)) {
        auto index_type = caf::get<record_type>(schema).field(*index).type;
        if (!caf::holds_alternative<ip_type>(index_type)) {
          TENZIR_DEBUG("pseudonymize operator skips field '{}' of unsupported "
                       "type '{}'",
                       field_name, index_type.name());
          continue;
        }
        transformations.push_back(
          {std::move(*index), std::move(transformation)});
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

  auto name() const -> std::string override {
    return "pseudonymize";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    return optimize_result::order_invariant(*this, order);
  }

  friend auto inspect(auto& f, pseudonymize_operator& x) -> bool {
    return f.apply(x.config_);
  }

private:
  /// Step-specific configuration, including the seed and field names.
  configuration config_ = {};
};

// -- plugin ------------------------------------------------------------------

class plugin final : public virtual operator_plugin<pseudonymize_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
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
    auto seed = std::string{};
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
        seed = *value_str;
      }
    }
    auto max_seed_size
      = std::min(tenzir::ip::pseudonymization_seed_array_size * 2, seed.size());
    for (auto i = size_t{0}; (i * 2) < max_seed_size; ++i) {
      auto byte_string_pos = i * 2;
      auto byte_size = (byte_string_pos + 2 > seed.size()) ? 1 : 2;
      auto byte = seed.substr(byte_string_pos, byte_size);
      if (byte_size == 1) {
        byte.append("0");
      }
      TENZIR_ASSERT(i < config.seed_bytes.size());
      config.seed_bytes[i] = std::strtoul(byte.c_str(), 0, 16);
    }
    return {
      std::string_view{f, l},
      std::make_unique<pseudonymize_operator>(std::move(config)),
    };
  }
};

} // namespace tenzir::plugins::pseudonymize

TENZIR_REGISTER_PLUGIN(tenzir::plugins::pseudonymize::plugin)
