/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include <regex>

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/pattern.hpp"
#include "vast/json.hpp"
#include "vast/pattern.hpp"

namespace vast {

pattern pattern::glob(std::string const& str) {
  auto rx = std::regex_replace(str, std::regex("\\."), "\\.");
  rx = std::regex_replace(rx, std::regex("\\*"), ".*");
  return pattern{std::regex_replace(rx, std::regex("\\?"), ".")};
}

pattern::pattern(std::string str) : str_(std::move(str)) {
}

bool pattern::match(std::string const& str) const {
  return std::regex_match(str.begin(), str.end(), std::regex{str_});
}

bool pattern::search(std::string const& str) const {
  return std::regex_search(str.begin(), str.end(), std::regex{str_});
}

const std::string& pattern::string() const {
  return str_;
}

bool operator==(pattern const& lhs, pattern const& rhs) {
  return lhs.str_ == rhs.str_;
}

bool operator<(pattern const& lhs, pattern const& rhs) {
  return lhs.str_ < rhs.str_;
}

bool convert(pattern const& p, json& j) {
  j = to_string(p);
  return true;
}

} // namespace vast
