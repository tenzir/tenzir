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
      = std::min(vast::address::anonymization_key_size, config_.key.size());

    for (auto i = size_t{0}; i < max_key_size * 2; ++i) {
      auto byte = config_.key.substr(i * 2, 2);
      if (byte.size() == 1) {
        byte.append("0");
      }
      config_.key_bytes[i] = std::strtoul(byte.c_str(), 0, 16);
    }
  }

  /// Applies the transformation to an Arrow Record Batch with a corresponding
  /// VAST layout.
  [[nodiscard]] caf::error
  add(type layout, std::shared_ptr<arrow::RecordBatch> batch) override {

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
