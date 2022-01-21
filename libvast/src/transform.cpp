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
#include <caf/expected.hpp>
#include <caf/none.hpp>
#include <caf/type_id.hpp>

#include <algorithm>

namespace vast {

transform::transform(std::string name, std::vector<std::string>&& event_types)
  : name_(std::move(name)), event_types_(std::move(event_types)) {
}

void transform::add_step(transform_step_ptr step) {
  steps_.emplace_back(std::move(step));
}

const std::string& transform::name() const {
  return name_;
}

const std::vector<std::string>& transform::event_types() const {
  return event_types_;
}

caf::error transform::add(table_slice&& x) {
  VAST_DEBUG("transform {} adds a slice", name_);
  auto batch = as_record_batch(x);
  return add_batch(x.layout(), batch);
}

caf::expected<std::vector<table_slice>> transform::finish() {
  VAST_DEBUG("transform {} retrieves results from {} steps", name_,
             steps_.size());
  auto guard = caf::detail::make_scope_guard([this]() {
    this->to_transform_.clear();
  });
  std::vector<table_slice> result{};
  auto finished = finish_batch();
  if (!finished)
    return std::move(finished.error());
  for (auto& [layout, batch] : *finished) {
    auto slice = arrow_table_slice_builder::create(batch, layout);
    result.push_back(std::move(slice));
  }
  return result;
}

caf::error transform::add_batch(vast::type layout,
                                std::shared_ptr<arrow::RecordBatch> batch) {
  VAST_DEBUG("add arrow data to transform {}", name_);
  to_transform_.emplace_back(std::move(layout), std::move(batch));
  return caf::none;
}

caf::error transform::process_queue(const transform_step_ptr& step) {
  caf::error failed{};
  const auto size = to_transform_.size();
  for (size_t i = 0; i < size; ++i) {
    auto [layout, batch] = std::move(to_transform_.front());
    to_transform_.pop_front();
    // TODO: Add a private method `unchecked_apply()` that the transformation
    // engine can call directly to avoid repeating the check.
    if (std::find(event_types_.begin(), event_types_.end(),
                  std::string{layout.name()})
        == event_types_.end()) {
      // The transform does not change slices of unconfigured event types.
      to_transform_.emplace_back(std::move(layout), std::move(batch));
      continue;
    }
    if (auto err = step->add(std::move(layout), std::move(batch))) {
      failed = caf::make_error(
        static_cast<vast::ec>(err.code()),
        fmt::format("transform aborts because of an error: {}", err));
      to_transform_.clear();
      break;
    }
  }
  // Finish frees up resource inside the plugin.
  auto finished = step->finish();
  if (!finished && !failed)
    failed = std::move(finished.error());
  if (failed) {
    to_transform_.clear();
    return failed;
  }
  for (const auto& b : *finished)
    to_transform_.push_back(b);
  return caf::none;
}

caf::expected<batch_vector> transform::finish_batch() {
  VAST_DEBUG("applying {} transform {}", steps_.size(), name_);
  for (const auto& step : steps_) {
    auto failed = process_queue(step);
    if (failed) {
      to_transform_.clear();
      return failed;
    }
  }
  batch_vector result{};
  while (!to_transform_.empty()) {
    result.emplace_back(std::move(to_transform_.front()));
    to_transform_.pop_front();
  }
  to_transform_.clear();
  return result;
}

transformation_engine::transformation_engine(std::vector<transform>&& transforms)
  : transforms_(std::move(transforms)) {
  for (size_t i = 0; i < transforms_.size(); ++i)
    for (const auto& type : transforms_[i].event_types())
      layout_mapping_[type].push_back(i);
}

/// Apply relevant transformations to the table slice.
caf::error transformation_engine::add(table_slice&& x) {
  VAST_DEBUG("transformation engine adds a slice");
  auto layout = x.layout();
  to_transform_[layout].emplace_back(std::move(x));
  return caf::none;
}

caf::error
transformation_engine::process_queue(transform& transform, batch_queue& queue) {
  caf::error failed{};
  const auto size = queue.size();
  for (size_t i = 0; i < size; ++i) {
    auto [layout, batch] = std::move(queue.front());
    queue.pop_front();
    if (auto err = transform.add_batch(layout, batch)) {
      failed = err;
      while (!queue.empty())
        queue.pop_front();
      break;
    }
  }
  // Finish frees up resource inside the plugin.
  auto finished = transform.finish_batch();
  if (!finished && !failed)
    failed = std::move(finished.error());
  if (failed)
    return failed;
  for (const auto& b : *finished)
    queue.push_back(b);
  return caf::none;
}

/// Apply relevant transformations to the table slice.
caf::expected<std::vector<table_slice>> transformation_engine::finish() {
  VAST_DEBUG("transformation engine retrieves results");
  auto to_transform = std::exchange(to_transform_, {});
  // NOTE: It's important that `batch` is kept alive until `create()`
  // is finished: If a copy was made, `batch` will hold the only reference
  // to its underlying table slice, but the RecordBatches created by the
  // transform step will most likely reference some of same underlying data.
  std::vector<std::shared_ptr<arrow::RecordBatch>> keep_alive{};
  std::unordered_map<vast::type, batch_queue> batches{};
  std::vector<table_slice> result{};
  for (auto& [layout, queue] : to_transform) {
    // TODO: Consider using a tsl robin map instead for transparent key lookup.
    const auto& matching = layout_mapping_.find(std::string{layout.name()});
    if (matching == layout_mapping_.end()) {
      for (auto& s : queue)
        result.emplace_back(std::move(s));
      queue.clear();
      continue;
    }
    auto& bq = batches[layout];
    for (auto& s : queue) {
      auto b = as_record_batch(s);
      bq.emplace_back(layout, b);
    }
    queue.clear();
    for (auto& [layout, batch] : bq)
      keep_alive.push_back(batch);
    const auto& indices = matching->second;
    VAST_INFO("transformation engine applies {} transforms on received table "
              "slices with layout {}",
              indices.size(), layout);
    for (auto idx : indices) {
      auto& t = transforms_.at(idx);
      auto failed = process_queue(t, bq);
      if (failed)
        return failed;
    }
  }
  for (auto& [layout, queue] : batches) {
    for (auto& [layout, batch] : queue) {
      auto slice = arrow_table_slice_builder::create(batch, layout);
      result.emplace_back(slice);
    }
    queue.clear();
  }
  return result;
}

const std::vector<transform>& transformation_engine::transforms() {
  return transforms_;
}

} // namespace vast
