//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/transform_steps/replace.hpp"

#include "vast/arrow_table_slice_builder.hpp"
#include "vast/concept/convertible/data.hpp"
#include "vast/concept/convertible/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/error.hpp"
#include "vast/plugin.hpp"
#include "vast/table_slice_builder_factory.hpp"

#include <arrow/array.h>
#include <fmt/format.h>

namespace vast {

replace_step::replace_step(replace_step_configuration configuration, data value)
  : value_(std::move(value)), config_(std::move(configuration)) {
  VAST_ASSERT(is_basic(value_));
}

caf::error
replace_step::add(type layout, std::shared_ptr<arrow::RecordBatch> batch) {
  VAST_TRACE("replace step adds batch");
  const auto& layout_rt = caf::get<record_type>(layout);
  auto column_offset = layout_rt.resolve_key(config_.field);
  if (!column_offset) {
    transformed_.emplace_back(layout, std::move(batch));
    return caf::none;
  }
  auto column_index = layout_rt.flat_index(*column_offset);
  // Compute the hash values.
  // TODO: Consider making this strongly typed so we don't need to infer the
  // type at this point.
  const auto inferred_type = type::infer(value_);
  auto cb = arrow_table_slice_builder::column_builder::make(
    inferred_type, arrow::default_memory_pool());
  for (int i = 0; i < batch->num_rows(); ++i) {
    cb->add(make_view(value_));
  }
  auto values_column = cb->finish();
  auto adjusted_batch
    = batch->SetColumn(detail::narrow_cast<int>(column_index),
                       arrow::field(config_.field, values_column->type()),
                       values_column);
  if (!adjusted_batch.ok()) {
    transformed_.clear();
    return caf::make_error(ec::unspecified,
                           fmt::format("failed to replace field in record "
                                       "batch schema at index {}: {}",
                                       column_index,
                                       adjusted_batch.status().ToString()));
  }
  // Adjust layout.
  auto field = layout_rt.field(*column_offset);
  auto adjusted_layout_rt = layout_rt.transform(
    {{*column_offset,
      record_type::assign({{std::string{field.name}, inferred_type}})}});
  VAST_ASSERT(adjusted_layout_rt); // replacing a field cannot fail.
  auto adjusted_layout = type{*adjusted_layout_rt};
  adjusted_layout.assign_metadata(layout);
  transformed_.emplace_back(std::move(adjusted_layout),
                            adjusted_batch.MoveValueUnsafe());
  return caf::none;
}

caf::expected<std::vector<transform_batch>> replace_step::finish() {
  VAST_DEBUG("replace step finished transformation");
  auto retval = std::move(transformed_);
  transformed_.clear();
  return retval;
}

class replace_step_plugin final : public virtual transform_plugin {
public:
  // Plugin API
  caf::error initialize(data) override {
    return {};
  }

  [[nodiscard]] const char* name() const override {
    return "replace";
  };

  // Transform Plugin API
  [[nodiscard]] caf::expected<std::unique_ptr<transform_step>>
  make_transform_step(const record& options) const override {
    if (!options.contains("field"))
      return caf::make_error(ec::invalid_configuration,
                             "key 'field' is missing in configuration for "
                             "replace step");
    if (!options.contains("value"))
      return caf::make_error(ec::invalid_configuration,
                             "key 'value' is missing in configuration for "
                             "replace step");
    auto config = to<replace_step_configuration>(options);
    if (!config)
      return config.error();
    auto data = from_yaml(config->value);
    if (!data)
      return caf::make_error(ec::invalid_configuration,
                             fmt::format("could not parse '{}' as valid data "
                                         "object: {}",
                                         config->value, render(data.error())));
    if (!is_basic(*data))
      return caf::make_error(ec::invalid_configuration, "only basic types are "
                                                        "allowed for 'replace' "
                                                        "transform");
    return std::make_unique<replace_step>(std::move(*config), std::move(*data));
  }
};

} // namespace vast

VAST_REGISTER_PLUGIN(vast::replace_step_plugin)
