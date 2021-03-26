//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/load_contents.hpp"

#include <caf/expected.hpp>
#include <caf/fwd.hpp>
#include <caf/streambuf.hpp>

#include <filesystem>
#include <fstream>
#include <string>
#include <type_traits>

#include <vast/error.hpp>
#include <vast/path.hpp>

namespace vast::detail {

caf::expected<std::string> load_contents(const std::filesystem::path& p) {
  std::string contents;
  caf::containerbuf<std::string> obuf{contents};
  std::ostream out{&obuf};
  std::ifstream in{p.string()};
  if (!in)
    return caf::make_error(ec::filesystem_error,
                           "failed to read from file " + p.string());
  out << in.rdbuf();
  return contents;
}

caf::expected<std::string> load_contents(const vast::path& p) {
  return load_contents(std::filesystem::path{p.str()});
}

} // namespace vast::detail
