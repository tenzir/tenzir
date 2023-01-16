//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "cef/add.hpp"

#include "cef/parse.hpp"

#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/data.hpp>
#include <vast/error.hpp>
#include <vast/table_slice_builder.hpp>

#include <fmt/core.h>

namespace vast::plugins::cef {

caf::error add(const message_view& msg, table_slice_builder& builder) {
  auto append = [&](const auto& x) -> caf::error {
    if (!builder.add(make_data_view(x)))
      return caf::make_error(ec::parse_error, //
                             fmt::format("failed to add value: {}", x));
    return caf::none;
  };
  // High-order helper function for the monadic caf::error::eval utility.
  auto f = [&](const auto& x) {
    return [&]() {
      return append(x);
    };
  };
  // Append first 7 fields.
  if (auto err
      = caf::error::eval(f(uint64_t{msg.cef_version}), f(msg.device_vendor),
                         f(msg.device_product), f(msg.device_version),
                         f(msg.signature_id), f(msg.name), f(msg.severity)))
    return err;
  // Append extension fields.
  for (const auto& [_, value] : msg.extension) {
    if (auto err = append(value))
      return err;
  }
  return caf::none;
}

} // namespace vast::plugins::cef
