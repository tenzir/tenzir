//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concept/parseable/numeric/real.hpp"

#include <algorithm>
#include <charconv>
#include <cmath>
#include <limits>
#include <memory>

namespace tenzir {

namespace {

// Maximum buffer size for double parsing with comma separator.
// Sized generously to handle numbers with many digits.
constexpr size_t max_double_chars = 64;

// Handle result_out_of_range by setting value to infinity or zero.
// std::from_chars leaves value unmodified on error, so we must set it.
void handle_out_of_range(const char* first, const char* ptr, double& value) {
  // Determine sign from input.
  bool negative = (first != ptr && *first == '-');
  // Look for exponent to distinguish overflow from underflow.
  // Overflow: large positive exponent (e.g., 1e309) → infinity
  // Underflow: large negative exponent (e.g., 1e-500) → zero
  const char* p = first;
  if (p != ptr && (*p == '+' || *p == '-')) {
    ++p;
  }
  // Skip digits and decimal point to find exponent.
  while (p != ptr && ((*p >= '0' && *p <= '9') || *p == '.')) {
    ++p;
  }
  if (p != ptr && (*p == 'e' || *p == 'E')) {
    ++p;
    bool exp_negative = false;
    if (p != ptr && (*p == '+' || *p == '-')) {
      exp_negative = (*p == '-');
      ++p;
    }
    if (exp_negative) {
      // Underflow: set to zero (preserving sign for -0.0 if needed).
      value = negative ? -0.0 : 0.0;
    } else {
      // Overflow: set to infinity.
      value = negative ? -std::numeric_limits<double>::infinity()
                       : std::numeric_limits<double>::infinity();
    }
  } else {
    // No exponent found but out_of_range - assume overflow.
    value = negative ? -std::numeric_limits<double>::infinity()
                     : std::numeric_limits<double>::infinity();
  }
}

} // namespace

template <char Separator>
template <class Iterator>
bool double_parser<Separator>::parse(Iterator& f, const Iterator& l,
                                     unused_type) const {
  double result{};
  const char* first = std::to_address(f);
  const char* last = std::to_address(l);
  const char* original_first = first;
  std::from_chars_result fcr;
  if constexpr (Separator == '.') {
    // Skip leading '+' since std::from_chars doesn't accept it.
    if (first != last && *first == '+') {
      ++first;
    }
    fcr = std::from_chars(first, last, result);
  } else {
    // For non-dot separators, copy to buffer and replace separator with '.'.
    const auto len = static_cast<size_t>(last - first);
    if (len > max_double_chars) {
      return false;
    }
    char buffer[max_double_chars];
    std::copy(first, last, buffer);
    for (size_t i = 0; i < len; ++i) {
      if (buffer[i] == Separator) {
        buffer[i] = '.';
      }
    }
    // Skip leading '+' since std::from_chars doesn't accept it.
    const char* buf_start = buffer;
    if (len > 0 && buffer[0] == '+') {
      ++buf_start;
      ++first; // Also advance first to keep them in sync
    }
    fcr = std::from_chars(buf_start, buffer + len, result);
    // Adjust ptr to point into original input.
    fcr.ptr = first + (fcr.ptr - buf_start);
  }
  // Accept both success and out_of_range (for overflow/underflow to inf/zero).
  if (fcr.ec == std::errc{}) {
    f += std::ranges::distance(original_first, fcr.ptr);
    return true;
  }
  if (fcr.ec == std::errc::result_out_of_range) {
    // Handle overflow/underflow - set result appropriately (unused here).
    handle_out_of_range(first, fcr.ptr, result);
    f += std::ranges::distance(original_first, fcr.ptr);
    return true;
  }
  return false;
}

template <char Separator>
template <class Iterator>
bool double_parser<Separator>::parse(Iterator& f, const Iterator& l,
                                     double& a) const {
  const char* first = std::to_address(f);
  const char* last = std::to_address(l);
  const char* original_first = first;
  std::from_chars_result fcr;
  if constexpr (Separator == '.') {
    // Skip leading '+' since std::from_chars doesn't accept it.
    if (first != last && *first == '+') {
      ++first;
    }
    fcr = std::from_chars(first, last, a);
  } else {
    // For non-dot separators, copy to buffer and replace separator with '.'.
    const auto len = static_cast<size_t>(last - first);
    if (len > max_double_chars) {
      return false;
    }
    char buffer[max_double_chars];
    std::copy(first, last, buffer);
    for (size_t i = 0; i < len; ++i) {
      if (buffer[i] == Separator) {
        buffer[i] = '.';
      }
    }
    // Skip leading '+' since std::from_chars doesn't accept it.
    const char* buf_start = buffer;
    if (len > 0 && buffer[0] == '+') {
      ++buf_start;
      ++first; // Also advance first to keep them in sync
    }
    fcr = std::from_chars(buf_start, buffer + len, a);
    // Adjust ptr to point into original input.
    fcr.ptr = first + (fcr.ptr - buf_start);
  }
  // Accept both success and out_of_range (for overflow/underflow to inf/zero).
  if (fcr.ec == std::errc{}) {
    f += std::ranges::distance(original_first, fcr.ptr);
    return true;
  }
  if (fcr.ec == std::errc::result_out_of_range) {
    // Handle overflow/underflow - set value to infinity or zero.
    handle_out_of_range(first, fcr.ptr, a);
    f += std::ranges::distance(original_first, fcr.ptr);
    return true;
  }
  return false;
}

template bool
double_parser<'.'>::parse(std::string::iterator&, const std::string::iterator&,
                          unused_type) const;
template bool
double_parser<'.'>::parse(std::string::iterator&, const std::string::iterator&,
                          double&) const;
template bool
double_parser<','>::parse(std::string::iterator&, const std::string::iterator&,
                          unused_type) const;
template bool
double_parser<','>::parse(std::string::iterator&, const std::string::iterator&,
                          double&) const;

template bool double_parser<'.'>::parse(std::string::const_iterator&,
                                        const std::string::const_iterator&,
                                        unused_type) const;
template bool
double_parser<'.'>::parse(std::string::const_iterator&,
                          const std::string::const_iterator&, double&) const;
template bool double_parser<','>::parse(std::string::const_iterator&,
                                        const std::string::const_iterator&,
                                        unused_type) const;
template bool
double_parser<','>::parse(std::string::const_iterator&,
                          const std::string::const_iterator&, double&) const;

template bool
double_parser<'.'>::parse(char const*&, char const* const&, unused_type) const;
template bool
double_parser<'.'>::parse(char const*&, char const* const&, double&) const;
template bool
double_parser<','>::parse(char const*&, char const* const&, unused_type) const;
template bool
double_parser<','>::parse(char const*&, char const* const&, double&) const;

} // namespace tenzir
