//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/transform.hpp"

namespace vast {

// Does nothing with the input.
class identity_step : public generic_transform_step,
                      public arrow_transform_step {
public:
  identity_step() = default;

  caf::expected<table_slice> operator()(table_slice&& slice) const override;

  std::pair<vast::legacy_record_type, std::shared_ptr<arrow::RecordBatch>>
  operator()(vast::legacy_record_type layout,
             std::shared_ptr<arrow::RecordBatch> batch) const override;
};

} // namespace vast
