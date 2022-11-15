//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

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
  struct address_mapping {
    std::string from = {};
    std::string to = {};

    template <class Inspector>
    friend auto inspect(Inspector& f, address_mapping& x) {
      return f(x.from, x.to);
    }

    static inline const record_type& layout() noexcept {
      static auto result = record_type{
        {"from", address_type{}},
        {"to", address_type{}},
      };
      return result;
    }
  };

  std::vector<address_mapping> addresses = {};

  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return f(x.addresses);
  }

  static inline const record_type& layout() noexcept {
    // addresses:
    //   - from: [non-anonymized]
    //     to: [anonymized]
    static auto result = record_type{
      {"addresses", list_type{address_mapping::layout()}},
    };
    return result;
  }
};

class anonymize_operator : public pipeline_operator {
public:
  anonymize_operator(configuration config) : config_{std::move(config)} {
    // nop
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

  /// Step-specific configuration, including the layout name mapping.
  configuration config_ = {};
};

// -- plugin ------------------------------------------------------------------

class plugin final : public virtual pipeline_operator_plugin {
public:
  caf::error initialize(data options) override {
    // Configuration needs a key
    if (const auto* rec = caf::get_if<record>(&options)) {
      if (!rec->empty()) {
        if (rec->size() != 1 || !caf::get_if<record>(&rec->at("key"))) {
          return caf::make_error(ec::invalid_configuration, "anonymize configuration "
                                                            "must contain only the "
                                                            "'key' key");
        }
        return caf::none;
      }

    }
    return caf::make_error(ec::invalid_configuration, "expected non-empty "
                                                      "configuration under "
                                                      "vast.plugins.anonymize");
  }

  /// The name is how the pipeline operator is addressed in a transform
  /// definition.
  [[nodiscard]] const char* name() const override {
    return "anonymize";
  };

  [[nodiscard]] caf::expected<std::unique_ptr<pipeline_operator>>
  make_pipeline_operator(const record& options) const override {
    auto config = to<configuration>(options);
    if (!config)
      return config.error();
    return std::make_unique<anonymize_operator>(std::move(*config));
  }
};

} // namespace vast::plugins::anonymize

VAST_REGISTER_PLUGIN(vast::plugins::anonymize::plugin)
