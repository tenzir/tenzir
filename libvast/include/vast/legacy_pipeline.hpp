//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/ids.hpp"
#include "vast/legacy_pipeline_operator.hpp"
#include "vast/type.hpp"

#include <queue>

namespace vast {

class legacy_pipeline {
public:
  /// Parse a pipeline from its textual representation.
  /// @param name the pipeline name
  /// @param repr the textual representation of the pipeline itself
  /// @param schema_names the schemas to restrict the pipeline to
  static caf::expected<legacy_pipeline>
  parse(std::string name, std::string_view repr,
        std::vector<std::string> schema_names = {});

  legacy_pipeline(std::string name, std::vector<std::string>&& schema_names);

  ~legacy_pipeline() = default;

  legacy_pipeline(const legacy_pipeline&) = delete;
  legacy_pipeline(legacy_pipeline&&) = default;

  legacy_pipeline& operator=(const legacy_pipeline&) = delete;
  legacy_pipeline& operator=(legacy_pipeline&&) = default;

  void add_operator(std::unique_ptr<legacy_pipeline_operator> op);

  /// Returns true if any of the pipeline operators is blocking.
  [[nodiscard]] bool is_blocking() const;

  /// Tests whether the transform applies to events of the given type.
  [[nodiscard]] bool applies_to(std::string_view event_name) const;

  /// Adds the table to the internal queue of batches to be transformed.
  [[nodiscard]] caf::error add(table_slice&&);

  /// Applies pipelines to the batches in the internal queue.
  /// @note The offsets of the slices may not be preserved.
  [[nodiscard]] caf::expected<std::vector<table_slice>> finish();

  [[nodiscard]] const std::string& name() const;

private:
  // Returns the list of schemas that the transform should apply to.
  // An empty vector means that the transform should apply to everything.
  [[nodiscard]] const std::vector<std::string>& schema_names() const;

  /// Add the batch to the internal queue of batches to be transformed.
  [[nodiscard]] caf::error add_slice(table_slice slice);

  /// Applies pipelines to the batches in the internal queue.
  /// @note The offsets of the slices may not be preserved.
  [[nodiscard]] caf::expected<std::vector<table_slice>> finish_batch();

  /// Applies the pipeline operator to every batch in the queue.
  caf::error process_queue(legacy_pipeline_operator& op,
                           std::vector<table_slice>& result, bool check_schema);

  /// Grant access to the pipelines engine so it can call
  /// add_batch/finsih_batch.
  friend class pipeline_executor;

  /// Name assigned to this pipelines.
  std::string name_;

  /// Sequence of pipelines steps
  std::vector<std::unique_ptr<legacy_pipeline_operator>> operators_;

  /// Triggers for this transform
  std::vector<std::string> schema_names_;

  /// The slices being transformed.
  std::deque<table_slice> to_transform_;

  /// The import timestamps collected since the last call to finish.
  std::vector<time> import_timestamps_ = {};
};

class pipeline_executor {
public:
  /// Constructor.
  pipeline_executor() = default;
  explicit pipeline_executor(std::vector<legacy_pipeline>&&);

  /// Starts applying relevant pipelines to the table.
  caf::error add(table_slice&&);

  /// Finishes applying pipelines to the added tables.
  /// @note The offsets of the slices may not be preserved.
  caf::expected<std::vector<table_slice>> finish();

  /// Get a list of the pipelines.
  const std::vector<legacy_pipeline>& pipelines() const;

  /// Returns whether any of the contained pipelines is blocking.
  bool is_blocking() const;

private:
  static caf::error
  process_queue(legacy_pipeline& transform, std::deque<table_slice>& queue);

  /// Apply relevant pipelines to the table slice.
  caf::expected<table_slice> transform_slice(table_slice&& x);

  /// The set of pipelines.
  std::vector<legacy_pipeline> pipelines_;

  /// Mapping from event type to applicable pipelines.
  std::unordered_map<std::string, std::vector<size_t>> schema_mapping_;

  /// The pipelines that will be applied to *all* types.
  std::vector<size_t> general_pipelines_;

  /// The slices being transformed.
  std::unordered_map<vast::type, std::deque<table_slice>> to_transform_;
};

} // namespace vast
