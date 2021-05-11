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

class replace_step : public generic_transform_step,
                     public arrow_transform_step {
public:
  replace_step(const std::string& fieldname, const vast::data& value);

  caf::expected<table_slice> operator()(table_slice&& slice) const override;

#if VAST_ENABLE_ARROW
  [[nodiscard]] std::pair<vast::record_type, std::shared_ptr<arrow::RecordBatch>>
  operator()(vast::record_type layout,
             std::shared_ptr<arrow::RecordBatch> batch) const override;
#endif

private:
  std::string field_;
  vast::data value_;
};

} // namespace vast
