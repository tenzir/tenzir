//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "cef/parse.hpp"

#include <vast/detail/string.hpp>

namespace vast::plugins::cef {

caf::expected<std::vector<std::string_view>> tokenize(std::string_view line) {
  auto tokens = detail::split(line, "|", "\\");
  // TODO: do the heavy lifting:
  // 1. get basic record structure
  // 2. split extensions (unescape =)
  // 3. unescape \r and \n in extensions only
  return tokens;
}

} // namespace vast::plugins::cef
