//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/arrow_table_slice.hpp>
#include <vast/concept/convertible/data.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice_builder_factory.hpp>
#include <vast/transform.hpp>
#include <vast/type.hpp>

#include <arrow/type.h>

namespace vast::plugins::drop {

namespace {

/// The configuration of a project transform step.
struct configuration {
  /// The key suffixes of the fields to drop.
  std::vector<std::string> fields = {};

  /// Support type inspection for easy parsing with convertible.
  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return f(x.fields);
  }

  /// Enable parsing from a record via convertible.
  static inline const record_type& layout() noexcept {
    static auto result = record_type{
      {"fields", list_type{string_type{}}},
    };
    return result;
  }
};

/// Drops the specifed fields from the input.
class drop_step : public transform_step {
public:
  explicit drop_step(configuration config) noexcept
    : config_{std::move(config)} {
    // nop
  }

  caf::error
  add(type layout, std::shared_ptr<arrow::RecordBatch> batch) override {
    VAST_DEBUG("drop step adds batch");
    // Apply the transformation.
    auto transform_fn
      = [&](struct record_type::field, std::shared_ptr<arrow::Array>) noexcept
      -> std::vector<
        std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>> {
      return {};
    };
    auto transformations = std::vector<indexed_transformation>{};
    for (const auto& field : config_.fields)
      for (auto&& index : caf::get<record_type>(layout).resolve_key_suffix(
             field, layout.name()))
        transformations.push_back({std::move(index), transform_fn});
    // transform_columns requires the transformations to be sorted, and that may
    // not necessarily be true if we have multiple fields configured, so we sort
    // again in that case.
    if (config_.fields.size() > 1)
      std::sort(transformations.begin(), transformations.end());
    auto [adjusted_layout, adjusted_batch]
      = transform_columns(layout, batch, transformations);
    if (adjusted_layout) {
      VAST_ASSERT(adjusted_batch);
      transformed_.emplace_back(std::move(adjusted_layout),
                                std::move(adjusted_batch));
    }
    return caf::none;
  }

  caf::expected<std::vector<transform_batch>> finish() override {
    VAST_DEBUG("drop step finished transformation");
    return std::exchange(transformed_, {});
  }

private:
  /// The slices being transformed.
  std::vector<transform_batch> transformed_;

  /// The underlying configuration of the transformation.
  configuration config_;
};

class plugin final : public virtual transform_plugin {
public:
  // plugin API
  caf::error initialize(data) override {
    return {};
  }

  [[nodiscard]] const char* name() const override {
    return "drop";
  };

  // transform plugin API
  [[nodiscard]] caf::expected<std::unique_ptr<transform_step>>
  make_transform_step(const record& options) const override {
    if (!options.contains("fields"))
      return caf::make_error(ec::invalid_configuration,
                             "key 'fields' is missing in configuration for "
                             "drop step");
    auto config = to<configuration>(options);
    if (!config)
      return config.error();
    return std::make_unique<drop_step>(std::move(*config));
  }
};

} // namespace

} // namespace vast::plugins::drop

VAST_REGISTER_PLUGIN(vast::plugins::drop::plugin)
