//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/port.hpp"

#include <string>

namespace vast::system {

struct connect_request {
  /// Port of a remote node server.
  vast::port::number_type port;
  /// Hostname or IP address of a remote node server.
  std::string host;
};

bool inspect(auto& f, connect_request& x) {
  return f.object(x)
    .pretty_name("connect_request")
    .fields(f.field("port", x.port), f.field("host", x.host));
}

} // namespace vast::system
