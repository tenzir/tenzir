//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/plugin.hpp"
#include "vast/transform_step.hpp"

namespace vast::plugins {

// This example transform shows the necessary scaffolding in order to
// use the `transform_plugin` API.

// The main job of a transform plugin is to create a `transform_step`
// when required. A transform step is a function that gets a table
// slice and returns the slice with a transformation applied.
// We derive from `arrow_transform_step` to signal to VAST that we
// implemented special code that can handle arrow-encoded table slices
// natively.
class example_transform_step : public generic_transform_step,
                               public arrow_transform_step {
public:
  example_transform_step() = default;

  // This handler receives a generic table slice in any encoding. It can be
  // inefficient to modify table slices on this level, on the other hand
  // VAST never needs to perform a conversion before calling this transform.
  [[nodiscard]] caf::expected<table_slice>
  operator()(table_slice&& slice) const override {
    // Transform the table slice here.
    return std::move(slice);
  }

  // A variant of the transform specialized to table slices encoded in
  // arrow format. Note that with this we don't necessarily need to generic
  // implemenetation above, because the `arrow_transform_step` already defines
  // a function that takes a generic table slice and transforms it to
  // arrow format. Many operations can be expressed more efficiently
  [[nodiscard]] std::pair<vast::record_type, std::shared_ptr<arrow::RecordBatch>>
  operator()(vast::record_type layout,
             std::shared_ptr<arrow::RecordBatch> batch) const override {
    return std::make_pair(std::move(layout), std::move(batch));
  }
};

// The plugin definition itself is below.
class example_transform_plugin final : public virtual transform_plugin {
public:
  caf::error initialize(data) override {
    return {};
  }

  // The name is how the transform step is addressed in a transform
  // definition, for example:
  //
  //     vast:
  //       transforms:
  //         transform1:
  //           - step1:
  //           - example_transform_step:
  //              setting: value
  //           - step3:
  //
  [[nodiscard]] const char* name() const override {
    return "example_transform_step";
  };

  // This is called once for every time this transform step appears in a
  // transform definition. The configuration for the step is opaquely
  // passed as the first argument.
  [[nodiscard]] caf::expected<transform_step_ptr>
  make_transform_step(const caf::settings&) const override {
    return std::make_unique<vast::plugins::example_transform_step>();
  }
};

} // namespace vast::plugins

// Finally, register our plugin.
VAST_REGISTER_PLUGIN(vast::plugins::example_transform_plugin, 0, 1)
