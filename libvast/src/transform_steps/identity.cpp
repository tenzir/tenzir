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

caf::expected<table_slice>
identity_step::operator()(table_slice&& slice) const {
  return std::move(slice);
}

std::pair<vast::legacy_record_type, std::shared_ptr<arrow::RecordBatch>>
identity_step::operator()(vast::legacy_record_type layout,
                          std::shared_ptr<arrow::RecordBatch> batch) const {
  return std::make_pair(std::move(layout), std::move(batch));
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
  [[nodiscard]] caf::expected<transform_step_ptr>
  make_transform_step(const caf::settings&) const override {
    return std::make_unique<identity_step>();
  }
};

} // namespace vast

VAST_REGISTER_PLUGIN(vast::identity_step_plugin)
