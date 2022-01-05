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

void offset_range::add(vast::id offset, table_slice::size_type rows) {
  ranges_.emplace_back(offset, rows);
}

bool offset_range::contains(vast::id offset, table_slice::size_type rows) {
  return std::any_of(ranges_.begin(), ranges_.end(), [&](auto p) {
    return offset >= p.first && offset + rows <= p.first + p.second;
  });
}

void offset_range::clear() {
  ranges_.clear();
}

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

caf::error transform::add_slice(table_slice&& x) {
  VAST_DEBUG("add slice data to transform {}", name_);
  range_.add(x.offset(), x.rows());
  auto batch = as_record_batch(x);
  return add(x.offset(), x.layout(), batch);
}

caf::expected<std::vector<table_slice>> transform::finish_slice() {
  VAST_DEBUG("retrieving result from {} steps of transform {}", steps_.size(),
             name_);
  auto guard = caf::detail::make_scope_guard([this]() {
    this->clear_slices();
  });
  std::vector<table_slice> result{};
  auto finished = finish();
  if (!finished)
    return std::move(finished.error());
  for (auto& [offset, layout, batch] : *finished) {
    auto slice = arrow_table_slice_builder::create(batch, layout);
    slice.offset(offset);
    if (!range_.contains(slice.offset(), slice.rows())) {
      return caf::make_error(ec::invalid_result, "transform returned a slice "
                                                 "with an offset outside the "
                                                 "input offsets");
    }
    result.push_back(slice);
  }
  return result;
}

void transform::clear_slices() {
  to_transform_.clear();
  range_.clear();
}

caf::error transform::add(vast::id offset, vast::type layout,
                          std::shared_ptr<arrow::RecordBatch> batch) {
  VAST_DEBUG("add arrow data to transform {}", name_);
  to_transform_.emplace_back(offset, std::move(layout), std::move(batch));
  return caf::none;
}

caf::error
transform::process_queue(const transform_step_ptr& step, batch_queue& queue) {
  caf::error failed{};
  const auto size = queue.size();
  for (size_t i = 0; i < size; ++i) {
    auto [offset, layout, batch] = std::move(queue.front());
    queue.pop_front();
    // TODO: Add a private method `unchecked_apply()` that the transformation
    // engine can call directly to avoid repeating the check.
    if (std::find(event_types_.begin(), event_types_.end(),
                  std::string{layout.name()})
        == event_types_.end()) {
      // The transform does not change slices of unconfigured event types.
      queue.emplace_back(offset, std::move(layout), std::move(batch));
      continue;
    }
    auto add_failed = step->add(offset, std::move(layout), std::move(batch));
    if (add_failed) {
      failed = add_failed;
      queue.clear();
      break;
    }
  }
  // Finish frees up resource inside the plugin.
  auto finished = step->finish();
  if (!finished && !failed)
    failed = std::move(finished.error());
  if (failed) {
    queue.clear();
    return failed;
  }
  for (const auto& b : *finished)
    queue.push_back(b);
  return caf::none;
}

caf::expected<batch_vector> transform::finish() {
  VAST_DEBUG("applying {} transform {}", steps_.size(), name_);
  for (const auto& step : steps_) {
    auto failed = process_queue(step, to_transform_);
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
  VAST_DEBUG("transformation engine add");
  range_.add(x.offset(), x.rows());
  auto layout = x.layout();
  to_transform_[layout].emplace_back(std::move(x));
  return caf::none;
}

caf::error
transformation_engine::process_queue(transform& transform, batch_queue& queue) {
  caf::error failed{};
  const auto size = queue.size();
  for (size_t i = 0; i < size; ++i) {
    auto [offset, layout, batch] = std::move(queue.front());
    queue.pop_front();
    auto add_failed = transform.add(offset, layout, batch);
    if (add_failed) {
      failed = add_failed;
      while (!queue.empty())
        queue.pop_front();
      break;
    }
  }
  // Finish frees up resource inside the plugin.
  auto finished = transform.finish();
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
  VAST_DEBUG("transformation engine finish");
  // NOTE: It's important that `batch` is kept alive until `create()`
  // is finished: If a copy was made, `batch` will hold the only reference
  // to its underlying table slice, but the RecordBatches created by the
  // transform step will most likely reference some of same underlying data.
  std::vector<std::shared_ptr<arrow::RecordBatch>> keep_alive{};
  std::unordered_map<vast::type, batch_queue> batches{};
  std::vector<table_slice> result{};
  auto guard = caf::detail::make_scope_guard([this]() {
    this->clear_slices();
  });

  for (auto& [layout, queue] : to_transform_) {
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
      bq.emplace_back(s.offset(), layout, b);
    }
    queue.clear();
    for (auto& [offset, layout, batch] : bq)
      keep_alive.push_back(batch);
    const auto& indices = matching->second;
    VAST_INFO("applying {} transforms for received table slice w/ layout {}",
              indices.size(), layout);
    for (auto idx : indices) {
      auto& t = transforms_.at(idx);
      auto failed = process_queue(t, bq);
      if (failed)
        return failed;
    }
  }
  for (auto& [layout, queue] : batches) {
    for (auto& [offset, layout, batch] : queue) {
      auto slice = arrow_table_slice_builder::create(batch, layout);
      slice.offset(offset);
      if (!range_.contains(slice.offset(), slice.rows())) {
        return caf::make_error(ec::invalid_result, "transform returned a slice "
                                                   "with an offset outside the "
                                                   "input offsets");
      }
      result.emplace_back(slice);
    }
    queue.clear();
  }
  std::sort(result.begin(), result.end(), [](table_slice& a, table_slice& b) {
    return a.offset() < b.offset();
  }); // FIXME: Unit test for the correct order
  return result;
}

void transformation_engine::clear_slices() {
  for (auto& [layout2, queue2] : to_transform_)
    queue2.clear(); // FIXME: for loop may not be needed
  to_transform_.clear();
  range_.clear();
}

const std::vector<transform>& transformation_engine::transforms() {
  return transforms_;
}

} // namespace vast
