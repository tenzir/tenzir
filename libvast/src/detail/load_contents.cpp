//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/load_contents.hpp"

#include "vast/error.hpp"

#include <caf/expected.hpp>
#include <caf/fwd.hpp>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

namespace vast::detail {

caf::expected<std::string> load_contents(const std::filesystem::path& p) {
  std::ifstream in{p};
  std::stringstream ss;
  if (!in)
    return caf::make_error(ec::filesystem_error,
                           "failed to read from file " + p.string());
  ss << in.rdbuf();
  return ss.str();
}

} // namespace vast::detail
