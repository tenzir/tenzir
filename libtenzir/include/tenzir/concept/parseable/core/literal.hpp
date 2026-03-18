//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/core/ignore.hpp"
#include "tenzir/concept/parseable/string/char.hpp"
#include "tenzir/concept/parseable/string/literal.hpp"
#include "tenzir/concept/parseable/string/string.hpp"

#include <cstddef>
#include <string>
#include <string_view>
#include <type_traits>

namespace tenzir::parser_literals {

constexpr auto operator""_p(char c) {
  return ignore(parsers::chr{c});
}

constexpr auto operator""_p(const char* str) {
  return ignore(parsers::lit{str});
}

constexpr auto operator""_p(const char* str, size_t size) {
  return ignore(parsers::lit{std::string_view{str, size}});
}

inline auto operator""_p(unsigned long long int x) {
  return ignore(parsers::str{std::to_string(x)});
}

inline auto operator""_p(long double x) {
  return ignore(parsers::str{std::to_string(x)});
}

} // namespace tenzir::parser_literals
