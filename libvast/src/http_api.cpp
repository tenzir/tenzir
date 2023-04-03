//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/http_api.hpp>

#include <fmt/format.h>

namespace vast {

std::string rest_endpoint::canonical_path() const {
  // eg. "POST /query/:id/next (v0)"
  return fmt::format("{} {} ({})", method, path, version);
}

} // namespace vast
