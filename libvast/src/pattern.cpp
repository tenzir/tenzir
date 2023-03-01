//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/pattern.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/pattern.hpp"
#include "vast/data.hpp"
#include "vast/error.hpp"
#include "vast/view.hpp"

#include <regex>

namespace vast {

auto pattern::make(std::string str, pattern_options options) noexcept
  -> caf::expected<pattern> {
  try {
    auto mode = std::regex_constants::ECMAScript;
    if (options.case_insensitive)
      mode |= std::regex_constants::icase;
    auto regex = std::regex{str, mode};
    return pattern{std::move(str), std::move(options), std::move(regex)};
  } catch (const std::regex_error& err) {
    return caf::make_error(
      ec::syntax_error, fmt::format("failed to create regex: {}", err.what()));
  }
}

bool pattern::match(std::string_view str) const {
  return std::regex_match(str.begin(), str.end(), regex_);
}

bool pattern::search(std::string_view str) const {
  return std::regex_search(str.begin(), str.end(), regex_);
}

const std::string& pattern::string() const {
  return str_;
}

const pattern_options& pattern::options() const {
  return options_;
}

bool operator==(const pattern& lhs, const pattern& rhs) noexcept {
  return pattern_view{lhs} == pattern_view{rhs};
}

std::strong_ordering
operator<=>(const pattern& lhs, const pattern& rhs) noexcept {
  return pattern_view{lhs} <=> pattern_view{rhs};
}

bool operator==(const pattern& lhs, std::string_view rhs) noexcept {
  return lhs.match(rhs);
}

bool convert(const pattern& p, data& d) {
  d = to_string(p);
  return true;
}

pattern::pattern(std::string str, pattern_options options, std::regex regex)
  : str_{std::move(str)}, options_{std::move(options)}, regex_{std::move(regex)} {
  // nop
}

} // namespace vast
