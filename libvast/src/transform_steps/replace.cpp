//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/transform_steps/replace.hpp"

#include "vast/arrow_table_slice_builder.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/error.hpp"
#include "vast/plugin.hpp"
#include "vast/table_slice_builder_factory.hpp"

#include <fmt/format.h>

namespace vast {

replace_step::replace_step(const std::string& fieldname,
                           const vast::data& value)
  : field_(fieldname), value_(value) {
  VAST_ASSERT(is_basic(value_));
}

caf::expected<table_slice> replace_step::operator()(table_slice&& slice) const {
  const auto& layout = slice.layout();
  const auto& layout_rt = caf::get<record_type>(layout);
  auto offset = layout_rt.resolve_key(field_);
  if (!offset)
    return std::move(slice);
  // We just got the offset from `layout`, so we can safely dereference.
  auto column_index = layout_rt.flat_index(*offset);
  const auto inferred_type = type::infer(value_);
  auto field = layout_rt.field(*offset);
  auto adjusted_layout_rt = layout_rt.transform(
    {{*offset,
      record_type::assign({{std::string{field.name}, inferred_type}})}});
  VAST_ASSERT(adjusted_layout_rt); // cannot fail
  auto adjusted_layout = type{*adjusted_layout_rt};
  adjusted_layout.assign_metadata(layout);
  auto builder_ptr
    = factory<table_slice_builder>::make(slice.encoding(), adjusted_layout);
  for (size_t i = 0; i < slice.rows(); ++i) {
    for (size_t j = 0; j < slice.columns(); ++j) {
      const auto& item = slice.at(i, j);
      auto success
        = j == column_index ? builder_ptr->add(value_) : builder_ptr->add(item);
      if (!success)
        return caf::make_error(ec::unspecified, "anonymize step: unknown error "
                                                "in table slice builder");
    }
  }
  return builder_ptr->finish();
}

caf::expected<std::pair<type, std::shared_ptr<arrow::RecordBatch>>>
replace_step::operator()(type layout,
                         std::shared_ptr<arrow::RecordBatch> batch) const {
  const auto& layout_rt = caf::get<record_type>(layout);
  auto offset = layout_rt.resolve_key(field_);
  if (!offset)
    return std::make_pair(std::move(layout), std::move(batch));
  auto column_index = layout_rt.flat_index(*offset);
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
  auto removed = batch->RemoveColumn(detail::narrow_cast<int>(column_index));
  if (!removed.ok())
    return caf::make_error(
      ec::unspecified, fmt::format("failed to remove field from record "
                                   "batch schema at index {}: {}",
                                   column_index, removed.status().ToString()));
  batch = removed.ValueOrDie();
  // SetColumn inserts *before* the element at the given index.
  auto added = batch->AddColumn(detail::narrow_cast<int>(column_index), field_,
                                values_column);
  if (!added.ok())
    return caf::make_error(ec::unspecified,
                           fmt::format("failed to add field {} to record batch "
                                       "schema at index {}: {}",
                                       field_, column_index,
                                       added.status().ToString()));
  batch = added.ValueOrDie();
  // Adjust layout.
  auto field = layout_rt.field(*offset);
  auto adjusted_layout_rt = layout_rt.transform(
    {{*offset,
      record_type::assign({{std::string{field.name}, inferred_type}})}});
  VAST_ASSERT(adjusted_layout_rt); // replacing a field cannot fail.
  auto adjusted_layout = type{*adjusted_layout_rt};
  adjusted_layout.assign_metadata(layout);
  return std::make_pair(std::move(adjusted_layout), std::move(batch));
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
  [[nodiscard]] caf::expected<transform_step_ptr>
  make_transform_step(const caf::settings& opts) const override {
    auto field = caf::get_if<std::string>(&opts, "field");
    if (!field)
      return caf::make_error(ec::invalid_configuration,
                             "key 'field' is missing or not a string in "
                             "configuration for delete step");
    auto value = caf::get_if<std::string>(&opts, "value");
    if (!value)
      return caf::make_error(ec::invalid_configuration,
                             "key 'value' is missing or not a string in "
                             "configuration for delete step");
    auto data = from_yaml(*value);
    if (!data)
      return caf::make_error(ec::invalid_configuration,
                             fmt::format("could not parse '{}' as valid data "
                                         "object: {}",
                                         *value, render(data.error())));
    if (!is_basic(*data))
      return caf::make_error(ec::invalid_configuration, "only basic types are "
                                                        "allowed for 'replace' "
                                                        "transform");
    return std::make_unique<replace_step>(*field, std::move(*data));
  }
};

} // namespace vast

VAST_REGISTER_PLUGIN(vast::replace_step_plugin)
