//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <cstdint>

namespace vast::system {

// All endpoint ids of the built-in VAST endpoints.

enum class query_endpoints : uint64_t { new_, next };

enum class status_endpoints : uint64_t {
  status,
};

} // namespace vast::system
