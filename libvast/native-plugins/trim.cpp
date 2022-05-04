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
#include <vast/plugin.hpp>
#include <vast/transform_step.hpp>
#include <vast/type.hpp>

#include <arrow/compute/api.h>
#include <arrow/compute/api_vector.h>
#include <arrow/compute/function.h>
#include <arrow/table.h>

namespace vast::plugins::trim {

/// The configuration of the trim transform step.
struct configuration {
  std::vector<std::string> fields = {};
  std::string chars = {};

  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return f(x.fields, x.chars);
  }

  static inline const record_type& layout() noexcept {
    static auto result = record_type{
      {"fields", list_type{string_type{}}},
      {"chars", string_type{}},
    };
    return result;
  }
};

class trim_step : public transform_step {
public:
  trim_step(configuration config) : config_{std::move(config)} {
    // nop
  }

  /// Applies the transformation to an Arrow Record Batch with a corresponding
  /// VAST layout.
  [[nodiscard]] caf::error
  add(type layout, std::shared_ptr<arrow::RecordBatch> batch) override {
    auto transformations = std::vector<indexed_transformation>{};
    for (const auto& field : config_.fields) {
      for (const auto& index : caf::get<record_type>(layout).resolve_key_suffix(
             field, layout.name())) {
        auto transformation = [&](struct record_type::field field,
                                  std::shared_ptr<arrow::Array> array) noexcept
          -> std::vector<std::pair<struct record_type::field,
                                   std::shared_ptr<arrow::Array>>> {
          const auto options = arrow::compute::TrimOptions{config_.chars};
          auto trim_result
            = arrow::compute::CallFunction("ascii_trim", {array}, &options);
          // FIXME: Error handling
          array = trim_result.ValueOrDie().make_array();
          return {{std::move(field), std::move(array)}};
        };
        transformations.push_back({index, std::move(transformation)});
      }
    }
    std::sort(transformations.begin(), transformations.end());
    std::tie(layout, batch) = transform_columns(layout, batch, transformations);
    transformed_batches_.emplace_back(std::move(layout), std::move(batch));
    return caf::none;
  } // namespace vast::plugins::trim

  /// Retrieves the result of the transformation.
  [[nodiscard]] caf::expected<std::vector<transform_batch>> finish() override {
    return std::exchange(transformed_batches_, {});
  }

private:
  /// Cache for transformed batches.
  std::vector<transform_batch> transformed_batches_ = {};

  /// Step-specific configuration, including the layout name mapping.
  configuration config_ = {};
};

// -- plugin ------------------------------------------------------------------

class plugin final : public virtual transform_plugin {
public:
  caf::error initialize(data options) override {
    // We don't use any plugin-specific configuration under
    // vast.plugins.trim, so nothing is needed here.
    if (caf::holds_alternative<caf::none_t>(options))
      return caf::none;
    if (const auto* rec = caf::get_if<record>(&options))
      if (rec->empty())
        return caf::none;
    return caf::make_error(ec::invalid_configuration, "expected empty "
                                                      "configuration under "
                                                      "vast.plugins.trim");
  }

  /// The name is how the transform step is addressed in a transform definition.
  [[nodiscard]] const char* name() const override {
    return "trim";
  };

  /// This is called once for every time this transform step appears in a
  /// transform definition. The configuration for the step is opaquely
  /// passed as the first argument.
  [[nodiscard]] caf::expected<std::unique_ptr<transform_step>>
  make_transform_step(const record& options) const override {
    auto config = to<configuration>(options);
    if (!config)
      return config.error();
    return std::make_unique<trim_step>(std::move(*config));
  }
};

} // namespace vast::plugins::trim

VAST_REGISTER_PLUGIN(vast::plugins::trim::plugin)
