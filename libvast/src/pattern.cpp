//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/concept/printable/vast/pattern.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/data.hpp"
#include "vast/pattern.hpp"

#include <regex>

namespace vast {

pattern pattern::glob(std::string_view str) {
  std::string rx;
  std::regex_replace(std::back_inserter(rx), str.begin(), str.end(),
                     std::regex("\\."), "\\.");
  rx = std::regex_replace(rx, std::regex("\\*"), ".*");
  return pattern{std::regex_replace(rx, std::regex("\\?"), ".")};
}

pattern::pattern(std::string str, bool case_insensitive)
  : str_(std::move(str)), case_insensitive_(case_insensitive) {
}

bool pattern::match(std::string_view str) const {
  return std::regex_match(
    str.begin(), str.end(),
    std::regex{str_, case_insensitive_ ? std::regex_constants::ECMAScript
                                           | std::regex_constants::icase
                                       : std::regex_constants::ECMAScript});
}

bool pattern::search(std::string_view str) const {
  return std::regex_search(
    str.begin(), str.end(),
    std::regex{str_, case_insensitive_ ? std::regex_constants::ECMAScript
                                           | std::regex_constants::icase
                                       : std::regex_constants::ECMAScript});
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

bool operator==(const pattern& lhs, std::string_view rhs) {
  return lhs.match(rhs);
}

bool operator!=(const pattern& lhs, std::string_view rhs) {
  return !(lhs == rhs);
}

bool operator==(std::string_view lhs, const pattern& rhs) {
  return rhs.match(lhs);
}

bool operator!=(std::string_view lhs, const pattern& rhs) {
  return !(lhs == rhs);
}

bool convert(const pattern& p, data& d) {
  d = to_string(p);
  return true;
}

} // namespace vast
