// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <cstddef>
#include <type_traits>
#include <string>

#include "vast/concept/parseable/core/ignore.hpp"
#include "vast/concept/parseable/string/char.hpp"
#include "vast/concept/parseable/string/literal.hpp"
#include "vast/concept/parseable/string/string.hpp"

namespace vast::parser_literals {

constexpr auto operator"" _p(char c) {
  return ignore(parsers::chr{c});
}

constexpr auto operator"" _p(const char* str) {
  return ignore(parsers::lit{str});
}

constexpr auto operator"" _p(const char* str, size_t size) {
  return ignore(parsers::lit{std::string_view{str, size}});
}

inline auto operator"" _p(unsigned long long int x) {
  return ignore(parsers::str{std::to_string(x)});
}

inline auto operator"" _p(long double x) {
  return ignore(parsers::str{std::to_string(x)});
}

} // namespace vast::parser_literals
