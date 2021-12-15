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

caf::expected<table_slice> delete_step::operator()(table_slice&& slice) const {
  const auto& layout = slice.layout();
  auto layout_result = adjust_layout(layout);
  if (!layout_result) {
    if (layout_result.error())
      return layout_result.error();
    return slice;
  }
  const auto& [adjusted_layout, to_keep] = *layout_result;
  auto builder_ptr
    = factory<table_slice_builder>::make(slice.encoding(), adjusted_layout);
  builder_ptr->reserve(slice.rows());
  for (size_t i = 0; i < slice.rows(); ++i) {
    for (const auto j : to_keep) {
      if (!builder_ptr->add(slice.at(i, j)))
        return caf::make_error(ec::unspecified, "delete step: unknown error "
                                                "in table slice builder");
    }
  }
  return builder_ptr->finish();
}

caf::expected<std::pair<type, std::shared_ptr<arrow::RecordBatch>>>
delete_step::operator()(type layout,
                        std::shared_ptr<arrow::RecordBatch> batch) const {
  auto layout_result = adjust_layout(layout);
  if (!layout_result) {
    if (layout_result.error())
      return layout_result.error();
    return std::make_pair(std::move(layout), std::move(batch));
  }
  auto& [adjusted_layout, to_keep] = *layout_result;
  auto result = batch->SelectColumns(to_keep);
  if (!result.ok())
    return caf::make_error(ec::unspecified,
                           fmt::format("failed to delete columns: {}",
                                       result.status().ToString()));
  return std::make_pair(std::move(adjusted_layout), result.MoveValueUnsafe());
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
