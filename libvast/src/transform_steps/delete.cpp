//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/transform_steps/delete.hpp"

#include "vast/arrow_table_slice.hpp"
#include "vast/concept/convertible/data.hpp"
#include "vast/concept/convertible/to.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/table_slice_builder_factory.hpp"

#include <arrow/type.h>

namespace vast {

delete_step::delete_step(delete_step_configuration configuration)
  : config_(std::move(configuration)) {
}

caf::error
delete_step::add(type layout, std::shared_ptr<arrow::RecordBatch> batch) {
  VAST_DEBUG("delete step adds batch");
  // Apply the transformation.
  auto transform_fn
    = [&](struct record_type::field, std::shared_ptr<arrow::Array>) noexcept
    -> std::vector<
      std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>> {
    return {};
  };
  auto transformations = std::vector<indexed_transformation>{};
  for (const auto& field : config_.fields)
    for (auto&& index :
         caf::get<record_type>(layout).resolve_key_suffix(field, layout.name()))
      transformations.push_back({std::move(index), transform_fn});
  std::sort(transformations.begin(), transformations.end());
  auto [adjusted_layout, adjusted_batch]
    = transform(layout, batch, transformations);
  if (adjusted_layout) {
    VAST_ASSERT(adjusted_batch);
    transformed_.emplace_back(std::move(adjusted_layout),
                              std::move(adjusted_batch));
  }
  return caf::none;
}

caf::expected<std::vector<transform_batch>> delete_step::finish() {
  VAST_DEBUG("delete step finished transformation");
  return std::exchange(transformed_, {});
}

class delete_step_plugin final : public virtual transform_plugin {
public:
  // plugin API
  caf::error initialize(data) override {
    return {};
  }

  [[nodiscard]] const char* name() const override {
    return "delete";
  };

  // transform plugin API
  [[nodiscard]] caf::expected<std::unique_ptr<transform_step>>
  make_transform_step(const record& options) const override {
    if (!options.contains("fields"))
      return caf::make_error(ec::invalid_configuration,
                             "key 'fields' is missing in configuration for "
                             "delete step");
    auto config = to<delete_step_configuration>(options);
    if (!config)
      return config.error();
    return std::make_unique<delete_step>(std::move(*config));
  }
};

} // namespace vast

VAST_REGISTER_PLUGIN(vast::delete_step_plugin)
