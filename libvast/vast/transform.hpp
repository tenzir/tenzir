//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/transform_step.hpp"
#include "vast/type.hpp"

#include <queue>

namespace vast {

class offset_range {
public:
  void add(vast::id offset, table_slice::size_type rows);
  bool contains(vast::id offset, table_slice::size_type rows);
  void clear();

private:
  std::vector<std::pair<vast::id, table_slice::size_type>> ranges_;
};

class transform {
public:
  transform(std::string name, std::vector<std::string>&& event_types);

  ~transform() = default;

  transform(const transform&) = delete;
  transform(transform&&) = default;

  transform& operator=(const transform&) = delete;
  transform& operator=(transform&&) = default;

  void add_step(transform_step_ptr step);

  // FIXME: Convinence function
  [[nodiscard]] caf::error add_slice(table_slice&&);
  // FIXME: Convinence function
  // The result is not ordered by offset
  [[nodiscard]] caf::expected<std::vector<table_slice>> finish_slice();

  [[nodiscard]] const std::vector<std::string>& event_types() const;

  [[nodiscard]] const std::string& name() const;

private:
  [[nodiscard]] caf::error add(vast::id offset, vast::type layout,
                               std::shared_ptr<arrow::RecordBatch> batch);

  [[nodiscard]] caf::expected<batch_vector> finish();

  caf::error process_queue(const transform_step_ptr& step, batch_queue& queue);

  // Grant access to the transformation engine so it can check the fast path.
  friend class transformation_engine;

  /// Name assigned to this transformation.
  std::string name_;

  /// Sequence of transformation steps
  std::vector<transform_step_ptr> steps_;

  /// Triggers for this transform
  std::vector<std::string> event_types_;

  /// Transformed slices
  batch_queue to_transform_; // FIXME: write doc

  offset_range range_;

  void clear_slices();
};

// TODO: Find a more descriptive name for this class.
class transformation_engine {
public:
  // member functions

  /// Constructor.
  transformation_engine() = default;
  explicit transformation_engine(std::vector<transform>&&);

  /// Apply relevant transformations to the table slice.
  caf::error add(table_slice&&);
  caf::expected<std::vector<table_slice>> finish();

  /// Get a list of the transformations.
  const std::vector<transform>& transforms();

private:
  static caf::error process_queue(transform& transform, batch_queue& queue);

  /// Apply relevant transformations to the table slice.
  caf::expected<table_slice> transform_slice(table_slice&& x);

  /// The set of transforms
  std::vector<transform> transforms_;

  /// event type -> applicable transforms
  std::unordered_map<std::string, std::vector<size_t>> layout_mapping_;

  std::unordered_map<vast::type, slice_queue> to_transform_;

  offset_range range_;

  void clear_slices();
};

} // namespace vast
