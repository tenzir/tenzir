//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/aggregation_function.hpp"

#include "vast/arrow_table_slice.hpp"

namespace vast {

void aggregation_function::add(const arrow::Array& array) {
  for (const auto& value : values(input_type_, array))
    add(value);
}

aggregation_function::aggregation_function(type input_type) noexcept
  : input_type_{std::move(input_type)} {
  // nop
}

const type& aggregation_function::input_type() const noexcept {
  return input_type_;
}

} // namespace vast
