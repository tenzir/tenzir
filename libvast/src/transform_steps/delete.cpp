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
  const auto& layout = slice.layout();
  auto offset = layout.resolve(fieldname_);
  if (!offset)
    return std::move(slice);
  auto flat_index = layout.flat_index_at(*offset);
  // We just got the offset from `layout`, so it should be valid.
  VAST_ASSERT(flat_index);
  auto new_layout = remove_field(layout, *offset);
  if (!new_layout)
    return caf::make_error(ec::unspecified, "failed to remove field from "
                                            "layout");
  auto builder_ptr
    = factory<table_slice_builder>::make(slice.encoding(), *new_layout);
  builder_ptr->reserve(slice.rows());
  for (size_t i = 0; i < slice.rows(); ++i) {
    for (size_t j = 0; j < slice.columns(); ++j) {
      if (j == flat_index)
        continue;
      if (!builder_ptr->add(slice.at(i, j)))
        return caf::make_error(ec::unspecified, "delete step: unknown error "
                                                "in table slice builder");
    }
  }
  return builder_ptr->finish();
}

std::pair<vast::legacy_record_type, std::shared_ptr<arrow::RecordBatch>>
delete_step::operator()(vast::legacy_record_type layout,
                        std::shared_ptr<arrow::RecordBatch> batch) const {
  auto offset = layout.resolve(fieldname_);
  if (!offset)
    return std::make_pair(std::move(layout), std::move(batch));
  auto flat_index = layout.flat_index_at(*offset);
  VAST_ASSERT(flat_index); // We just got this from `layout`.
  auto column_index = static_cast<int>(*flat_index);
  auto maybe_transformed = batch->RemoveColumn(column_index);
  if (!maybe_transformed.ok())
    return {};
  auto transformed = maybe_transformed.ValueOrDie();
  auto new_layout = remove_field(layout, *offset);
  if (!new_layout)
    return {};
  return std::make_pair(std::move(*new_layout), std::move(transformed));
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
