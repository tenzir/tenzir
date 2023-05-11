//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/detail/to_xsv_sep.hpp>
#include <vast/error.hpp>

#include <fmt/format.h>

namespace vast {

auto to_xsv_sep(std::string_view x) -> caf::expected<char> {
  if (x == "\\t") {
    return '\t';
  }
  if (x == "\\0" || x == "NUL") {
    return '\0';
  }
  if (x.size() == 1) {
    using namespace std::literals;
    auto allowed_chars = ",;\t\0 "sv;
    if (allowed_chars.find(x[0]) != std::string_view::npos) {
      return x[0];
    }
  }
  return caf::make_error(ec::invalid_argument,
                         fmt::format("separator must be one of comma, "
                                     "semicolon, tab, NUL, or space, but is "
                                     "'{}'",
                                     x));
}

} // namespace vast