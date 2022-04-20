//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/arrow_table_slice_builder.hpp"
#include "vast/defaults.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/transform.hpp"
#include "vast/type.hpp"

#include <arrow/type.h>
#include <caf/error.hpp>
#include <caf/expected.hpp>

#include <string_view>

namespace vast {

namespace {

// Counts the rows in the input.
class count_step : public transform_step {
public:
  count_step() = default;

  [[nodiscard]] bool is_aggregate() const override {
    return true;
  }

  [[nodiscard]] caf::error
  add([[maybe_unused]] type layout,
      std::shared_ptr<arrow::RecordBatch> batch) override {
    VAST_TRACE("count step adds batch");
    count_ += batch->num_rows();
    return caf::none;
  }

  [[nodiscard]] caf::expected<std::vector<transform_batch>> finish() override {
    VAST_DEBUG("count step finished transformation");
    auto builder = vast::factory<vast::table_slice_builder>::make(
      vast::table_slice_encoding::arrow, schema);
    if (builder == nullptr)
      return caf::make_error(ec::invalid_result, "count_step failed to get a "
                                                 "table slice builder");
    if (!builder->add(count_))
      return caf::make_error(ec::invalid_result, "count_step failed to add row "
                                                 "to the result");
    auto result = to_record_batch(builder->finish());
    auto transformed = std::vector<transform_batch>{};
    transformed.emplace_back(schema, result);
    count_ = 0;
    return transformed;
  }

private:
  size_t count_{};

  static inline const auto schema = type{
    "vast.count",
    record_type{
      {"count", count_type{}},
    },
  };
};

class count_step_plugin final : public virtual transform_plugin {
public:
  // plugin API
  caf::error initialize(data) override {
    return {};
  }

  [[nodiscard]] const char* name() const override {
    return "count";
  };

  // transform plugin API
  [[nodiscard]] caf::expected<std::unique_ptr<transform_step>>
  make_transform_step(const record&) const override {
    return std::make_unique<count_step>();
  }
};

} // namespace

} // namespace vast

VAST_REGISTER_PLUGIN(vast::count_step_plugin)
