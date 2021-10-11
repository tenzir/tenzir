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

delete_step::delete_step(const std::string& fieldname) : fieldname_(fieldname) {
}

caf::expected<table_slice> delete_step::operator()(table_slice&& slice) const {
  const auto& layout = slice.layout().type;
  auto offset = layout.resolve_prefix(fieldname_);
  if (!offset)
    return std::move(slice);
  auto columnn_index = layout.flat_index(*offset);
  auto adjusted_layout = layout.transform({{*offset, record_type::drop()}});
  if (!adjusted_layout)
    return caf::make_error(ec::unspecified, "failed to remove field from "
                                            "layout");
  auto builder_ptr
    = factory<table_slice_builder>::make(slice.encoding(), *adjusted_layout);
  builder_ptr->reserve(slice.rows());
  for (size_t i = 0; i < slice.rows(); ++i) {
    for (size_t j = 0; j < slice.columns(); ++j) {
      if (j == columnn_index)
        continue;
      if (!builder_ptr->add(slice.at(i, j)))
        return caf::make_error(ec::unspecified, "delete step: unknown error "
                                                "in table slice builder");
    }
  }
  return builder_ptr->finish();
}

caf::expected<std::pair<record_type, std::shared_ptr<arrow::RecordBatch>>>
delete_step::operator()(record_type layout,
                        std::shared_ptr<arrow::RecordBatch> batch) const {
  auto offset = layout.resolve_prefix(fieldname_);
  if (!offset)
    return std::make_pair(std::move(layout), std::move(batch));
  auto column_index = layout.flat_index(*offset);
  auto removed = batch->RemoveColumn(column_index);
  if (!removed.ok())
    return caf::make_error(
      ec::unspecified, fmt::format("failed to remove field from record "
                                   "batch schema at index {}: {}",
                                   column_index, removed.status().ToString()));
  auto adjusted_layout = layout.transform({{*offset, record_type::drop()}});
  if (!adjusted_layout)
    return caf::make_error(ec::unspecified, "failed to remove field from "
                                            "layout");
  return std::make_pair(std::move(*adjusted_layout),
                        std::move(removed.ValueOrDie()));
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
    auto field = caf::get_if<std::string>(&opts, "field");
    if (!field)
      return caf::make_error(ec::invalid_configuration,
                             "key 'field' is missing or not a string in "
                             "configuration for delete step");
    return std::make_unique<delete_step>(*field);
  }
};

} // namespace vast

VAST_REGISTER_PLUGIN(vast::delete_step_plugin)
