//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/transform_steps/identity.hpp"

#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/table_slice_builder_factory.hpp"

#include <arrow/type.h>

namespace vast {

caf::error
identity_step::add(type layout, std::shared_ptr<arrow::RecordBatch> batch) {
  VAST_TRACE("identity step adds batch");
  transformed_.emplace_back(layout, std::move(batch));
  return caf::none;
}

caf::expected<std::vector<transform_batch>> identity_step::finish() {
  VAST_DEBUG("identity step finished transformation");
  return std::exchange(transformed_, {});
}

class identity_step_plugin final : public virtual transform_plugin {
public:
  // plugin API
  caf::error initialize(data) override {
    return {};
  }

  [[nodiscard]] const char* name() const override {
    return "identity";
  };

  // transform plugin API
  [[nodiscard]] caf::expected<std::unique_ptr<transform_step>>
  make_transform_step(const record&) const override {
    return std::make_unique<identity_step>();
  }
};

} // namespace vast

VAST_REGISTER_PLUGIN(vast::identity_step_plugin)
