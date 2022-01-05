//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/plugin.hpp>
#include <vast/transform_step.hpp>

namespace vast::plugins {

// This example transform shows the necessary scaffolding in order to
// use the `transform_plugin` API.

// The main job of a transform plugin is to create a `transform_step`
// when required. A transform step is a function that gets a table
// slice and returns the slice with a transformation applied.
// We derive from `arrow_transform_step` to signal to VAST that we
// implemented special code that can handle arrow-encoded table slices
// natively.
class example_transform_step : public transform_step {
public:
  example_transform_step() = default;

  /// Applies the transformation to a record batch(arrow encoding) with a
  /// corresponding vast layout.
  [[nodiscard]] caf::error
  add(vast::id offset, type layout,
      std::shared_ptr<arrow::RecordBatch> batch) override {
    // Transform the table slice here.
    transformed_.emplace_back(offset, std::move(layout), std::move(batch));
    return caf::none;
  }

  /// Retrieves the result of the transformation.
  [[nodiscard]] caf::expected<batch_vector> finish() override {
    return std::exchange(transformed_, {});
  }

private:
  batch_vector transformed_;
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
  //           - example-transform:
  //              setting: value
  //           - step3:
  //
  [[nodiscard]] const char* name() const override {
    return "example-transform";
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
VAST_REGISTER_PLUGIN(vast::plugins::example_transform_plugin)
