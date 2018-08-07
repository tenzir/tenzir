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

pattern pattern::glob(std::string_view str) {
  std::string rx;
  std::regex_replace(std::back_inserter(rx), str.begin(), str.end(),
                     std::regex("\\."), "\\.");
  rx = std::regex_replace(rx, std::regex("\\*"), ".*");
  return pattern{std::regex_replace(rx, std::regex("\\?"), ".")};
}

pattern::pattern(std::string str) : str_(std::move(str)) {
}

bool pattern::match(std::string_view str) const {
  return std::regex_match(str.begin(), str.end(), std::regex{str_});
}

bool pattern::search(std::string_view str) const {
  return std::regex_search(str.begin(), str.end(), std::regex{str_});
}

const std::string& pattern::string() const {
  return str_;
}

pattern& pattern::operator+=(const pattern& other) {
  return *this += std::string_view{other.str_};
}

pattern& pattern::operator+=(std::string_view other) {
  str_ += other;
  return *this;
}

pattern& pattern::operator|=(const pattern& other) {
  return *this |= std::string_view{other.str_};
}

pattern& pattern::operator|=(std::string_view other) {
  str_.insert(str_.begin(), '(');
  str_ += ")|(";
  str_.append(other.begin(), other.end());
  str_ += ')';
  return *this;
}

pattern& pattern::operator&=(const pattern& other) {
  return *this &= std::string_view{other.str_};
}

pattern& pattern::operator&=(std::string_view other) {
  str_.insert(str_.begin(), '(');
  str_ += ")(";
  str_.append(other.begin(), other.end());
  str_ += ')';
  return *this;
}

pattern operator+(const pattern& x, std::string_view y) {
  return pattern{x.string() + std::string{y}};
}

pattern operator+(std::string_view x, const pattern& y) {
  return pattern{std::string{x} + y.string()};
}

pattern operator|(const pattern& x, std::string_view y) {
  pattern result{x};
  result |= y;
  return result;
}

pattern operator|(std::string_view x, const pattern& y) {
  pattern result{std::string{x}};
  result |= y;
  return result;
}

pattern operator&(const pattern& x, std::string_view y) {
  pattern result{x};
  result &= y;
  return result;
}

pattern operator&(std::string_view x, const pattern& y) {
  pattern result{std::string{x}};
  result &= y;
  return result;
}

bool operator==(const pattern& lhs, const pattern& rhs) {
  return lhs.str_ == rhs.str_;
}

bool operator<(const pattern& lhs, const pattern& rhs) {
  return lhs.str_ < rhs.str_;
}

bool convert(const pattern& p, json& j) {
  j = to_string(p);
  return true;
}

} // namespace vast
