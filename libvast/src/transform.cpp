//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/transform.hpp"

#include "vast/arrow_table_slice_builder.hpp"
#include "vast/logger.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/table_slice_encoding.hpp"

#include <arrow/type.h>

namespace vast {

caf::expected<table_slice> transform_step::apply(table_slice&& slice) const {
  const auto* generic_step = dynamic_cast<const generic_transform_step*>(this);
  const auto* arrow_step = dynamic_cast<const arrow_transform_step*>(this);
  VAST_ASSERT(arrow_step || generic_step);
  if (arrow_step)
    // Call arrow step if the slice uses arrow encoding or when table slice is
    // not using arrow encoding but there is no generic step to call (the slice
    // will be converted to arrow in that case).
    if (slice.encoding() == table_slice_encoding::arrow
        || (!generic_step && slice.encoding() != table_slice_encoding::arrow))
      return (*arrow_step)(std::move(slice));
  return (*generic_step)(std::move(slice));
}

caf::expected<table_slice>
arrow_transform_step::operator()(table_slice&& x) const {
  // NOTE: It's important that `batch` is kept alive until `create()`
  // is finished: If a copy was made, `batch` will hold the only reference
  // to its underlying table slice, but the RecordBatches created by the
  // transform step will most likely reference some of same underlying data.
  auto batch = as_record_batch(x);
  auto result = (*this)(x.layout(), batch);
  if (!result)
    return result.error();
  return arrow_table_slice_builder::create(result->second, result->first);
}

transform::transform(std::string name, std::vector<std::string>&& event_types)
  : name_(std::move(name)),
    arrow_fast_path_(true),
    event_types_(std::move(event_types)) {
}

void transform::add_step(transform_step_ptr step) {
  steps_.emplace_back(std::move(step));
  arrow_fast_path_
    = arrow_fast_path_ && dynamic_cast<arrow_transform_step*>(step.get());
}

const std::string& transform::name() const {
  return name_;
}

const std::vector<std::string>& transform::event_types() const {
  return event_types_;
}

caf::expected<table_slice> transform::apply(table_slice&& x) const {
  VAST_DEBUG("applying {} steps of transform {}", steps_.size(), name_);
  // TODO: Add a private method `unchecked_apply()` that the transformation
  // engine can call directly to avoid repeating the check.
  const auto& layout = x.layout();
  if (std::find(event_types_.begin(), event_types_.end(),
                std::string{layout.name()})
      == event_types_.end())
    return std::move(x);
  // TODO: Use the fast-path overload if all steps are `arrow_transform_step`s.
  for (const auto& step : steps_) {
    auto transformed = step->apply(std::move(x));
    if (!transformed)
      return transformed.error();
    x = std::move(*transformed);
  }
  return std::move(x);
}

std::pair<vast::type, std::shared_ptr<arrow::RecordBatch>>
transform::apply(vast::type layout,
                 std::shared_ptr<arrow::RecordBatch> batch) const {
  VAST_DEBUG("applying {} arrow steps of transform {}", steps_.size(), name_);
  for (const auto& step : steps_) {
    auto arrow_step = dynamic_cast<const arrow_transform_step*>(step.get());
    VAST_ASSERT(arrow_step);
    auto result = (*arrow_step)(layout, std::move(batch));
    if (!result)
      return std::make_pair(std::move(layout), std::move(batch));
    std::tie(layout, batch) = std::move(*result);
    if (!batch)
      return std::make_pair(std::move(layout), std::move(batch));
  }
  return std::make_pair(std::move(layout), std::move(batch));
}

transformation_engine::transformation_engine(std::vector<transform>&& transforms)
  : transforms_(std::move(transforms)) {
  for (size_t i = 0; i < transforms_.size(); ++i)
    for (const auto& type : transforms_[i].event_types())
      layout_mapping_[type].push_back(i);
}

/// Apply relevant transformations to the table slice.
caf::expected<table_slice> transformation_engine::apply(table_slice&& x) const {
  auto offset = x.offset();
  auto size = x.rows();
  auto layout = x.layout();
  // TODO: Consider using a tsl robin map instead for transparent key lookup.
  const auto& matching = layout_mapping_.find(std::string{layout.name()});
  if (matching == layout_mapping_.end())
    return std::move(x);
  const auto& indices = matching->second;
  VAST_INFO("applying {} transforms for received table slice w/ layout {}",
            indices.size(), x.layout());
  auto arrow_fast_path
    = std::all_of(indices.begin(), indices.end(), [&](size_t idx) {
        return transforms_.at(idx).arrow_fast_path_;
      });
  if (arrow_fast_path) {
    VAST_INFO("selected fast path because all transforms support arrow");
    // NOTE: It's important that `batch` is kept alive until `create()`
    // is finished: If a copy was made, `batch` will hold the only reference
    // to its underlying table slice, but the RecordBatches created by the
    // transform step will most likely reference some of same underlying data.
    auto original_batch = as_record_batch(x);
    auto batch = original_batch;
    for (auto idx : indices) {
      const auto& t = transforms_.at(idx);
      std::tie(layout, batch) = t.apply(std::move(layout), std::move(batch));
      if (batch->num_rows() > static_cast<int>(size))
        return caf::make_error(ec::invalid_result, "adding rows in a transform "
                                                   "is currently not "
                                                   "supported");
      if (!batch)
        return caf::make_error(ec::convert_error, "error while applying arrow "
                                                  "transform");
    }
    return arrow_table_slice_builder::create(batch, layout);
  }
  VAST_INFO("falling back to generic path because not all transforms support "
            "arrow");
  for (auto idx : indices) {
    const auto& t = transforms_.at(idx);
    auto transformed = t.apply(std::move(x));
    if (!transformed)
      return transformed.error();
    if (transformed->rows() > size)
      return caf::make_error(ec::invalid_result, "adding rows in a transform "
                                                 "is currently not supported");
    x = std::move(*transformed);
  }
  x.offset(offset);

  return std::move(x);
}

const std::vector<transform>& transformation_engine::transforms() {
  return transforms_;
}

} // namespace vast
