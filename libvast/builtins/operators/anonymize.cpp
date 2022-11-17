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

namespace vast::plugins::anonymize {

/// The configuration of the anonymize pipeline operator.
struct configuration {
  std::string key;
  std::array<address::byte_type, vast::address::anonymization_key_size> key_bytes{};
  std::vector<std::string> fields;

  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return f(x.key, x.fields);
  }

  static inline const record_type& layout() noexcept {
    static auto result = record_type{
      {"key", string_type{}},
      {"fields", list_type{string_type{}}},
    };
    return result;
  }
};

class anonymize_operator : public pipeline_operator {
public:
  anonymize_operator(configuration config) : config_{std::move(config)} {
    auto max_key_size
      = std::min(vast::address::anonymization_key_size * 2, config_.key.size());

    for (auto i = size_t{0}; (i*2) < max_key_size; ++i) {
      auto byte_string_pos = i*2;
      auto byte_size = (byte_string_pos + 2 > config_.key.size()) ? 1 : 2;
      auto byte = config_.key.substr(byte_string_pos, byte_size);
      if (byte_size == 1) {
        byte.append("0");
      }
      config_.key_bytes[i] = std::strtoul(byte.c_str(), 0, 16);
    }
  }

  /// Applies the transformation to an Arrow Record Batch with a corresponding
  /// VAST layout.
  [[nodiscard]] caf::error
  add(type layout, std::shared_ptr<arrow::RecordBatch> batch) override {
    std::vector<indexed_transformation> transformations;
    for (const auto& field : config_.fields) {
      for (const auto& index : caf::get<record_type>(layout).resolve_key_suffix(
             field, layout.name())) {
        auto transformation = [&](struct record_type::field field,
                                  std::shared_ptr<arrow::Array> array) noexcept
          -> std::vector<std::pair<struct record_type::field,
                                   std::shared_ptr<arrow::Array>>> {
          VAST_ASSERT(caf::holds_alternative<address_type>(field.type));
          auto builder
            = address_type::make_arrow_builder(arrow::default_memory_pool());

          auto address_view_generator
            = values(address_type{},
                     caf::get<type_to_arrow_array_t<address_type>>(*array));

          for (auto&& address : address_view_generator) {
            auto append_status = arrow::Status{};
            if (address) {
              address->anonymize(config_.key_bytes);
              append_status
                = append_builder(address_type{}, *builder, *address);
            } else {
              append_status = builder->AppendNull();
            }
            VAST_ASSERT(append_status.ok(), append_status.ToString().c_str());
          }

          // turn address::anonymize() into anonymize(key, address_view) -
          // static function

          auto new_array = builder->Finish().ValueOrDie(); // don't use this.
          return {
            {field, new_array},
          };
        };

        transformations.push_back({index, std::move(transformation)});
      }
    }

    std::sort(transformations.begin(), transformations.end());
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

  /// Step-specific configuration, including the key and field names.
  configuration config_ = {};
};

// -- plugin ------------------------------------------------------------------

class plugin final : public virtual pipeline_operator_plugin {
public:
  caf::error initialize(data) override {
    return {};
  }

  [[nodiscard]] const char* name() const override {
    return "anonymize";
  };

  [[nodiscard]] caf::expected<std::unique_ptr<pipeline_operator>>
  make_pipeline_operator(const record& options) const override {
    if (options.size() != 2) {
      return caf::make_error(ec::invalid_configuration,
                             "Configuration under vast.plugins.anonymize must "
                             "only contain the 'key' and 'fields' keys");
    }

    if (!options.contains("key")) {
      return caf::make_error(ec::invalid_configuration,
                             "Configuration under vast.plugins.anonymize must "
                             "does not contain 'key' key");
    }
    if (!options.contains("fields")) {
      return caf::make_error(ec::invalid_configuration,
                             "Configuration under vast.plugins.anonymize must "
                             "does not contain 'fields' key");
    }

    auto config = to<configuration>(options);
    if (!config)
      return config.error();
    if (std::any_of(config->key.begin(), config->key.end(), [](auto c) {
          return !std::isxdigit(c);
        })) {
      return caf::make_error(ec::invalid_configuration,
                             "vast.plugins.anonymize.key must"
                             "contain a hexadecimal value");
    }
    return std::make_unique<anonymize_operator>(std::move(*config));
  }
};

} // namespace vast::plugins::anonymize

VAST_REGISTER_PLUGIN(vast::plugins::anonymize::plugin)
