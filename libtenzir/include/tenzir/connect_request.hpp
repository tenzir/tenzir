//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/port.hpp"

#include <string>

namespace tenzir {

struct connect_request {
  /// Port of a remote node server.
  tenzir::port::number_type port;
  /// Hostname or IP address of a remote node server.
  std::string host;
};

bool inspect(auto& f, connect_request& x) {
  return f.object(x)
    .pretty_name("connect_request")
    .fields(f.field("port", x.port), f.field("host", x.host));
}

} // namespace tenzir
