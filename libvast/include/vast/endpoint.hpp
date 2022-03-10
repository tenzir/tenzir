//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/port.hpp"

#include <optional>
#include <string>

namespace vast {

/// A transport-layer endpoint consisting of host and port.
struct endpoint {
  std::string host;         ///< The hostname or IP address.
  std::optional<class port> port; ///< The transport-layer port.
};

} // namespace vast
