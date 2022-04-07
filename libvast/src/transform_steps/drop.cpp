//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/arrow_table_slice_builder.hpp>
#include <vast/concept/convertible/data.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/data.hpp>
#include <vast/plugin.hpp>
#include <vast/transform_step.hpp>
#include <vast/type.hpp>

#include <arrow/table.h>

namespace vast::plugins::drop {

/// The configuration of the drop transform step.
struct configuration {
  count number = 1;

  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return f(x.number);
  }

  static inline const record_type& layout() noexcept {
    static auto result = record_type{
      {"number", count_type{}},
    };
    return result;
  }
};

class drop_step : public transform_step {
public:
  explicit drop_step(configuration config) : config_{std::move(config)} {
    // nop
  }

  bool is_aggregate() const override {
    return true;
  }

  /// Applies the transformation to an Arrow Record Batch with a corresponding
  /// VAST layout.
  [[nodiscard]] caf::error
  add(type layout, std::shared_ptr<arrow::RecordBatch> batch) override {
    const auto num_rows = batch->num_rows();
    if (num_dropped_ >= config_.number) {
      transformed_batches_.emplace_back(std::move(layout), std::move(batch));
    } else if (num_dropped_ + num_rows > config_.number) {
      auto slice
        = batch->Slice(detail::narrow_cast<int>(config_.number - num_dropped_));
      num_dropped_ = config_.number;
      transformed_batches_.emplace_back(std::move(layout), std::move(slice));
    } else {
      num_dropped_ += num_rows;
    }
    return caf::none;
  }

  /// Retrieves the result of the transformation.
  [[nodiscard]] caf::expected<std::vector<transform_batch>> finish() override {
    return std::exchange(transformed_batches_, {});
  }

private:
  /// Cache for transformed batches.
  std::vector<transform_batch> transformed_batches_ = {};

  /// Step-specific configuration.
  configuration config_ = {};

  /// The number of rows already dropn.
  count num_dropped_ = 0;
};

// -- plugin ------------------------------------------------------------------

class plugin final : public virtual transform_plugin {
public:
  caf::error initialize(data options) override {
    // We don't use any plugin-specific configuration under
    // vast.plugins.drop, so nothing is needed here.
    if (caf::holds_alternative<caf::none_t>(options))
      return caf::none;
    if (const auto* rec = caf::get_if<record>(&options))
      if (rec->empty())
        return caf::none;
    return caf::make_error(ec::invalid_configuration, "expected empty "
                                                      "configuration under "
                                                      "vast.plugins.drop");
  }

  /// The name is how the transform step is addressed in a transform definition.
  [[nodiscard]] const char* name() const override {
    return "drop";
  };

  /// This is called once for every time this transform step appears in a
  /// transform definition. The configuration for the step is opaquely
  /// passed as the first argument.
  [[nodiscard]] caf::expected<std::unique_ptr<transform_step>>
  make_transform_step(const record& options) const override {
    auto config = to<configuration>(options);
    if (!config)
      return config.error();
    return std::make_unique<drop_step>(std::move(*config));
  }
};

} // namespace vast::plugins::drop

// Finally, register our plugin.
VAST_REGISTER_PLUGIN(vast::plugins::drop::plugin)
