//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/pipeline_operator.hpp>
#include <vast/plugin.hpp>

namespace vast::plugins {

// This example pipeline shows the necessary scaffolding in order to
// use the `pipeline_plugin` API.

// The main job of a pipeline plugin is to create a `pipeline_opeartor`
// when required. A pipeline step is a function that gets a table
// slice and returns the slice with a transformation applied.
class example_pipeline_operator : public pipeline_operator {
public:
  example_pipeline_operator() = default;

  /// Applies the transformation to an Arrow Record Batch with a corresponding
  /// VAST layout.
  [[nodiscard]] caf::error
  add(type layout, std::shared_ptr<arrow::RecordBatch> batch) override {
    // Transform the table slice here.
    transformed_.emplace_back(std::move(layout), std::move(batch));
    return caf::none;
  }

  /// Retrieves the result of the transformation.
  [[nodiscard]] caf::expected<std::vector<pipeline_batch>> finish() override {
    return std::exchange(transformed_, {});
  }

private:
  /// The slices being transformed.
  std::vector<pipeline_batch> transformed_;
};

// The plugin definition itself is below.
class example_pipeline_plugin final : public virtual pipeline_operator_plugin {
public:
  caf::error initialize(data) override {
    return {};
  }

  // The name is how the pipeline step is addressed in a pipeline
  // definition, for example:
  //
  //     vast:
  //       pipelines:
  //         pipeline1:
  //           - step1:
  //           - example-pipeline:
  //              setting: value
  //           - step3:
  //
  [[nodiscard]] std::string name() const override {
    return "example-pipeline";
  };

  // This is called once for every time this pipeline step appears in a
  // pipeline definition. The configuration for the step is opaquely
  // passed as the first argument.
  [[nodiscard]] caf::expected<std::unique_ptr<pipeline_operator>>
  make_pipeline_operator(const vast::record&) const override {
    return std::make_unique<vast::plugins::example_pipeline_operator>();
  }
};

} // namespace vast::plugins

// Finally, register our plugin.
VAST_REGISTER_PLUGIN(vast::plugins::example_pipeline_plugin)
