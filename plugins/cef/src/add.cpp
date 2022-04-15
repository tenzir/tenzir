//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "cef/add.hpp"

#include "cef/parse.hpp"

#include <vast/error.hpp>
#include <vast/table_slice_builder.hpp>

#include <fmt/core.h>

namespace vast::plugins::cef {

caf::error add(const message& msg, table_slice_builder& builder) {
  auto result = builder.add(make_view(count{msg.cef_version}))
                && builder.add(make_data_view(msg.device_vendor))
                && builder.add(make_data_view(msg.device_product))
                && builder.add(make_data_view(msg.device_version))
                && builder.add(make_data_view(msg.signature_id))
                && builder.add(make_data_view(msg.name))
                && builder.add(make_data_view(msg.severity));
  if (!result)
    return caf::make_error(ec::parse_error, //
                           fmt::format("failed to add first 7 message fields"));
  // TODO: implement extension transposition.
  if (!builder.add(make_data_view("dummy")))
    return caf::make_error(ec::parse_error, //
                           fmt::format("failed to add extension field"));
  return caf::none;
}

} // namespace vast::plugins::cef
