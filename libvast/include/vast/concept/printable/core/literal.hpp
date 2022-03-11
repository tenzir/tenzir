//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/printable/string/literal.hpp"

#include <cstddef>
#include <string>
#include <type_traits>

namespace vast::printer_literals {

inline auto operator"" _P(char c) {
  return literal_printer{c};
}

inline auto operator"" _P(const char* str) {
  return literal_printer{std::string{str}};
}

inline auto operator"" _P(const char* str, size_t size) {
  return literal_printer{{str, size}};
}

inline auto operator"" _P(unsigned long long int x) {
  return literal_printer{x};
}

inline auto operator"" _P(long double x) {
  return literal_printer{x};
}

} // namespace vast::printer_literals
