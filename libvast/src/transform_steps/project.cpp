//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/transform_steps/project.hpp"

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

caf::expected<std::pair<vast::type, std::vector<int>>>
project_step::adjust_layout(const vast::type& layout) const {
  auto to_keep = std::unordered_set<vast::offset>{};
  auto flat_index_to_keep = std::vector<int>{};
  const auto& layout_rt = caf::get<record_type>(layout);
  for (const auto& key : config_.fields) {
    auto offsets = layout_rt.resolve_key_suffix(key);
    for (const auto& offset : offsets) {
      if (to_keep.emplace(offset).second) {
        flat_index_to_keep.emplace_back(layout_rt.flat_index(offset));
      }
    }
  }
  if (to_keep.empty())
    return caf::no_error;
  // adjust layout
  auto transformations = std::vector<record_type::transformation>{};
  for (const auto& [field, offset] : layout_rt.leaves()) {
    if (!to_keep.contains(offset))
      transformations.push_back({offset, record_type::drop()});
  }
  auto adjusted_layout_rt = layout_rt.transform(std::move(transformations));
  if (!adjusted_layout_rt)
    return caf::make_error(ec::unspecified, "failed to remove a field from "
                                            "layout");
  auto adjusted_layout = type{*adjusted_layout_rt};
  adjusted_layout.assign_metadata(layout);
  std::sort(flat_index_to_keep.begin(), flat_index_to_keep.end());
  return std::pair{std::move(adjusted_layout), std::move(flat_index_to_keep)};
}

caf::error
project_step::add(type layout, std::shared_ptr<arrow::RecordBatch> batch) {
  VAST_DEBUG("project step adds batch");
  auto layout_result = adjust_layout(layout);
  if (!layout_result) {
    if (layout_result.error()) {
      transformed_.clear();
      return layout_result.error();
    }
    return caf::none;
  }
  // remove columns
  auto& [adjusted_layout, to_keep] = *layout_result;
  auto result = batch->SelectColumns(to_keep);
  if (!result.ok()) {
    transformed_.clear();
    return caf::make_error(ec::unspecified,
                           fmt::format("failed to select columns: {}",
                                       result.status().ToString()));
  }
  transformed_.emplace_back(std::move(adjusted_layout),
                            result.MoveValueUnsafe());
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
  make_transform_step(const record& opts) const override {
    auto config = to<project_step_configuration>(opts);
    if (!config)
      return config.error(); // FIXME: Better error message?
    return std::make_unique<project_step>(std::move(*config));
  }
};

} // namespace vast

VAST_REGISTER_PLUGIN(vast::project_step_plugin)
