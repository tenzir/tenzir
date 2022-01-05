//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/transform_steps/delete.hpp"

#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/table_slice_builder_factory.hpp"

#include <arrow/type.h>

namespace vast {

delete_step::delete_step(std::vector<std::string> fields)
  : fields_(std::move(fields)) {
}

caf::expected<std::pair<vast::type, std::vector<int>>>
delete_step::adjust_layout(const vast::type& layout) const {
  auto to_remove = std::set<vast::offset>{};
  const auto& layout_rt = caf::get<record_type>(layout);
  for (const auto& key : fields_) {
    auto offsets = layout_rt.resolve_key_suffix(key);
    for (const auto& offset : offsets) {
      to_remove.emplace(offset);
    }
  }
  if (to_remove.empty())
    return caf::no_error;
  auto transformations = std::vector<record_type::transformation>{};
  auto flat_index_to_keep = std::vector<int>{};
  for (auto flat_index = 0; const auto& [field, offset] : layout_rt.leaves()) {
    if (to_remove.contains(offset))
      transformations.push_back({offset, record_type::drop()});
    else
      flat_index_to_keep.push_back(flat_index);
    ++flat_index;
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

caf::error delete_step::add(vast::id offset, type layout,
                            std::shared_ptr<arrow::RecordBatch> batch) {
  VAST_DEBUG("delete_step add offset {}", offset);
  auto layout_result = adjust_layout(layout);
  if (!layout_result) {
    if (layout_result.error()) {
      transformed_.clear();
      return layout_result.error();
    }
    transformed_.emplace_back(offset, std::move(layout), std::move(batch));
    return caf::none;
  }
  auto& [adjusted_layout, to_keep] = *layout_result;
  auto result = batch->SelectColumns(to_keep);
  if (!result.ok()) {
    transformed_.clear();
    return caf::make_error(ec::unspecified,
                           fmt::format("failed to delete columns: {}",
                                       result.status().ToString()));
  }
  transformed_.emplace_back(offset, std::move(adjusted_layout),
                            result.MoveValueUnsafe());
  return caf::none;
}

caf::expected<batch_vector> delete_step::finish() {
  VAST_DEBUG("delete_step finished");
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
  [[nodiscard]] caf::expected<transform_step_ptr>
  make_transform_step(const caf::settings& opts) const override {
    auto fields = caf::get_if<std::vector<std::string>>(&opts, "fields");
    if (!fields)
      return caf::make_error(ec::invalid_configuration,
                             "key 'fields' is missing or not a string list in "
                             "configuration for delete step");
    return std::make_unique<delete_step>(*fields);
  }
};

} // namespace vast

VAST_REGISTER_PLUGIN(vast::delete_step_plugin)
