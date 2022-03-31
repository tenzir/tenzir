//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/transform_steps/project.hpp"

#include "vast/arrow_table_slice.hpp"
#include "vast/concept/convertible/data.hpp"
#include "vast/concept/convertible/to.hpp"
#include "vast/error.hpp"
#include "vast/plugin.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/type.hpp"

#include <arrow/type.h>
#include <caf/expected.hpp>

#include <algorithm>
#include <utility>

namespace vast {

project_step::project_step(project_step_configuration configuration)
  : config_(std::move(configuration)) {
}

caf::error
project_step::add(type layout, std::shared_ptr<arrow::RecordBatch> batch) {
  VAST_DEBUG("project step adds batch");
  auto indices = std::vector<offset>{};
  for (const auto& field : config_.fields)
    for (auto&& index :
         caf::get<record_type>(layout).resolve_key_suffix(field, layout.name()))
      indices.push_back(std::move(index));
  std::sort(indices.begin(), indices.end());
  auto [adjusted_layout, adjusted_batch]
    = select_columns(layout, batch, indices);
  if (adjusted_layout) {
    VAST_ASSERT(adjusted_batch);
    transformed_.emplace_back(std::move(adjusted_layout),
                              std::move(adjusted_batch));
  }
  return caf::none;
}

caf::expected<std::vector<transform_batch>> project_step::finish() {
  VAST_DEBUG("project step finished transformation");
  return std::exchange(transformed_, {});
}

class project_step_plugin final : public virtual transform_plugin {
public:
  // plugin API
  caf::error initialize(data) override {
    return {};
  }

  [[nodiscard]] const char* name() const override {
    return "project";
  };

  // transform plugin API
  [[nodiscard]] caf::expected<std::unique_ptr<transform_step>>
  make_transform_step(const record& options) const override {
    if (!options.contains("fields"))
      return caf::make_error(ec::invalid_configuration,
                             "key 'fields' is missing in configuration for "
                             "project step");
    auto config = to<project_step_configuration>(options);
    if (!config)
      return config.error();
    return std::make_unique<project_step>(std::move(*config));
  }
};

} // namespace vast

VAST_REGISTER_PLUGIN(vast::project_step_plugin)
