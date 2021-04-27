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

#include <arrow/type.h>

namespace vast {

caf::expected<table_slice> transform_step::apply(table_slice&& slice) const {
  bool enable_arrow = VAST_ENABLE_ARROW > 0;
  const auto* arrow_step = dynamic_cast<const arrow_transform_step*>(this);
  const auto* generic_step = dynamic_cast<const generic_transform_step*>(this);
  if (arrow_step && enable_arrow) {
    return (*arrow_step)(std::move(slice));
  }
  return (*generic_step)(std::move(slice));
}

caf::expected<table_slice>
arrow_transform_step::operator()(table_slice&& x) const {
  auto [layout, transformed] = (*this)(x.layout(), as_record_batch(x));
  return arrow_table_slice_builder::create(transformed, layout);
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
  // TODO: Use the fast-path overload if all steps are `arrow_transform_step`s.
  bool enable_arrow = VAST_ENABLE_ARROW > 0;
  for (const auto& step : steps_) {
    if (const auto* arrow_step
        = dynamic_cast<const arrow_transform_step*>(step.get());
        enable_arrow && arrow_step) {
      auto transformed = (*arrow_step)(std::move(x));
      if (!transformed)
        return transformed;
      x = std::move(*transformed);
    } else if (const auto* generic_step
               = dynamic_cast<const generic_transform_step*>(step.get())) {
      auto transformed = (*generic_step)(std::move(x));
      if (!transformed)
        return transformed;
      x = std::move(*transformed);
    } else {
      // There are currently no more kinds of `transform_step` implemented.
      VAST_ASSERT(!"Invalid transform step type");
    }
  }
  return std::move(x);
}

std::pair<vast::record_type, std::shared_ptr<arrow::RecordBatch>>
transform::apply(vast::record_type layout,
                 std::shared_ptr<arrow::RecordBatch> batch) const {
  VAST_DEBUG("applying {} arrow steps of transform {}", steps_.size(), name_);
  for (const auto& step : steps_) {
    auto arrow_step = dynamic_cast<const arrow_transform_step*>(step.get());
    VAST_ASSERT(arrow_step);
    std::tie(layout, batch)
      = (*arrow_step)(std::move(layout), std::move(batch));
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
  const auto& matching = layout_mapping_.find(x.layout().name());
  if (matching == layout_mapping_.end())
    return std::move(x);
  auto& indices = matching->second;
  VAST_INFO("applying {} transforms for received table slice w/ layout {}",
            indices.size(), x.layout().name());
#if VAST_ENABLE_ARROW > 0
  auto arrow_fast_path
    = std::all_of(indices.begin(), indices.end(), [&](size_t idx) {
        return transforms_.at(idx).arrow_fast_path_;
      });
  if (arrow_fast_path) {
    VAST_INFO("selected fast path because all transforms support arrow");
    auto layout = x.layout();
    auto batch = as_record_batch(x);
    for (auto idx : indices) {
      const auto& t = transforms_.at(idx);
      std::tie(layout, batch) = t.apply(std::move(layout), std::move(batch));
      if (!batch)
        return caf::make_error(ec::convert_error, "error while applying arrow "
                                                  "transform");
    }
    return arrow_table_slice_builder::create(std::move(batch),
                                             std::move(layout));
  }
#endif
  VAST_INFO("falling back to generic path because not all transforms support "
            "arrow");
  for (auto idx : indices) {
    const auto& t = transforms_.at(idx);
    auto transformed = t.apply(std::move(x));
    if (!transformed)
      return transformed.error();
    x = std::move(*transformed);
  }
  x.offset(offset);
  return std::move(x);
}

} // namespace vast