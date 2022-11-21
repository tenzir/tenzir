//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/address.hpp>
#include <vast/arrow_table_slice.hpp>
#include <vast/arrow_table_slice_builder.hpp>
#include <vast/concept/convertible/data.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/data.hpp>
#include <vast/pipeline_operator.hpp>
#include <vast/plugin.hpp>
#include <vast/type.hpp>

#include <arrow/table.h>

namespace vast::plugins::pseudonymize {

/// The configuration of the pseudonymize pipeline operator.
struct configuration {
  // field for future extensibility; currently we only use the Crypto-PAn method
  std::string method;
  std::string seed;
  std::array<address::byte_type, vast::address::pseudonymization_seed_array_size>
    seed_bytes{};
  std::vector<std::string> fields;

  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return f(x.method, x.seed, x.fields);
  }

  static inline const record_type& layout() noexcept {
    static auto result = record_type{
      {"method", string_type{}},
      {"seed", string_type{}},
      {"fields", list_type{string_type{}}},
    };
    return result;
  }
};

class pseudonymize_operator : public pipeline_operator {
public:
  pseudonymize_operator(configuration config) : config_{std::move(config)} {
    parse_seed_string();
  }

  /// Applies the transformation to an Arrow Record Batch with a corresponding
  /// VAST layout.
  [[nodiscard]] caf::error
  add(type layout, std::shared_ptr<arrow::RecordBatch> batch) override {
    std::vector<indexed_transformation> transformations;
    auto transformation = [&](struct record_type::field field,
                              std::shared_ptr<arrow::Array> array) noexcept
      -> std::vector<
        std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>> {
      auto builder
        = address_type::make_arrow_builder(arrow::default_memory_pool());
      auto address_view_generator = values(
        address_type{}, caf::get<type_to_arrow_array_t<address_type>>(*array));
      for (const auto& address : address_view_generator) {
        auto append_status = arrow::Status{};
        if (address) {
          auto pseudonymized_address
            = vast::address::pseudonymize(*address, config_.seed_bytes);
          append_status
            = append_builder(address_type{}, *builder, pseudonymized_address);
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
      for (const auto& index : caf::get<record_type>(layout).resolve_key_suffix(
             field_name, layout.name())) {
        if (!caf::holds_alternative<address_type>(
              caf::get<record_type>(layout).field(index).type)) {
          VAST_ASSERT(false, "record batch field to be pseudonymized but does "
                             "not "
                             "have address type");
          VAST_WARN(fmt::format("Field {} is to be pseudonymized but does not "
                                "contain "
                                "IP "
                                "address values; skipping pseudonymization",
                                field_name));
          continue;
        }
        transformations.push_back({index, std::move(transformation)});
      }
    }
    std::sort(transformations.begin(), transformations.end());
    transformations.erase(std::unique(transformations.begin(),
                                      transformations.end()),
                          transformations.end());
    auto [adjusted_layout, adjusted_batch]
      = transform_columns(layout, batch, transformations);
    transformed_batches_.emplace_back(std::move(adjusted_layout),
                                      std::move(adjusted_batch));
    return caf::none;
  }

  /// Retrieves the result of the transformation.
  [[nodiscard]] caf::expected<std::vector<pipeline_batch>> finish() override {
    return std::exchange(transformed_batches_, {});
  }

private:
  /// Cache for transformed batches.
  std::vector<pipeline_batch> transformed_batches_ = {};

  /// Step-specific configuration, including the seed and field names.
  configuration config_ = {};

  void parse_seed_string() {
    auto max_seed_size = std::min(
      vast::address::pseudonymization_seed_array_size * 2, config_.seed.size());
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

class plugin final : public virtual pipeline_operator_plugin {
public:
  caf::error initialize(data) override {
    return {};
  }

  [[nodiscard]] const char* name() const override {
    return "pseudonymize";
  };

  [[nodiscard]] caf::expected<std::unique_ptr<pipeline_operator>>
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
};

} // namespace vast::plugins::pseudonymize

VAST_REGISTER_PLUGIN(vast::plugins::pseudonymize::plugin)
