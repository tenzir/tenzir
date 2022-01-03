//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/expression.hpp"
#include "vast/transform.hpp"

namespace vast {

// Selects mathcing rows from the input
class select_step : public generic_transform_step {
public:
  select_step(std::string expr);

  caf::expected<table_slice> operator()(table_slice&& slice) const override;

private:
  caf::expected<vast::expression> expression_;
};

} // namespace vast
