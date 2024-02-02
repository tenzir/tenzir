//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concept/parseable/numeric/real.hpp"

#include "tenzir/logger.hpp"

#include <fast_float/fast_float.h>

#include <memory>
#include <string>

namespace tenzir {

template <char Separator>
template <class Iterator>
bool double_parser<Separator>::parse(Iterator& f, const Iterator& l,
                                     unused_type) const {
  double result;
  fast_float::parse_options options{fast_float::chars_format::general,
                                    Separator};
  const auto [ptr, ec] = fast_float::from_chars_advanced(
    std::to_address(f), std::to_address(l), result, options);
  const auto success = ec == std::errc();
  if (success) {
    f += std::ranges::distance(std::to_address(f), ptr);
  }
  return success;
}

template <char Separator>
template <class Iterator>
bool double_parser<Separator>::parse(Iterator& f, const Iterator& l,
                                     double& a) const {
  fast_float::parse_options options{fast_float::chars_format::general,
                                    Separator};
  const auto [ptr, ec] = fast_float::from_chars_advanced(
    std::to_address(f), std::to_address(l), a, options);
  const auto success = ec == std::errc();
  if (success) {
    f += std::ranges::distance(std::to_address(f), ptr);
  }
  return success;
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
