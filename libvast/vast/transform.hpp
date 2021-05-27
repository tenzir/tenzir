//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/transform_step.hpp"

namespace vast {

class transform {
public:
  transform(std::string name, std::vector<std::string>&& event_types);

  ~transform() = default;

  transform(const transform&) = delete;
  transform(transform&&) = default;

  transform& operator=(const transform&) = delete;
  transform& operator=(transform&&) = default;

  void add_step(transform_step_ptr step);

  caf::expected<table_slice> apply(table_slice&&) const;

  [[nodiscard]] const std::vector<std::string>& event_types() const;

  [[nodiscard]] const std::string& name() const;

private:
  [[nodiscard]] std::pair<vast::record_type, std::shared_ptr<arrow::RecordBatch>>
  apply(vast::record_type layout,
        std::shared_ptr<arrow::RecordBatch> batch) const;

  // Grant access to the transformation engine so it can check the fast path.
  friend class transformation_engine;

  /// Name assigned to this transformation.
  std::string name_;

  /// Sequence of transformation steps
  std::vector<transform_step_ptr> steps_;

  /// If all steps of this transform have specialized arrow handlers,
  /// we can save all intermediate (de)serialization steps.
  /// NOTE: This is ignored if VAST is built without arrow support.
  bool arrow_fast_path_;

  /// Triggers for this transform
  std::vector<std::string> event_types_;
};

// TODO: Find a more descriptive name for this class.
class transformation_engine {
public:
  // member functions

  /// Constructor.
  transformation_engine() = default;
  explicit transformation_engine(std::vector<transform>&&);

  /// Apply relevant transformations to the table slice.
  caf::expected<table_slice> apply(table_slice&&) const;

private:
  /// The set of transforms
  std::vector<transform> transforms_;

  /// event type -> applicable transforms
  std::unordered_map<std::string, std::vector<size_t>> layout_mapping_;
};

} // namespace vast
