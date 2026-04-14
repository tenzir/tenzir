//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/core/ignore.hpp"
#include "tenzir/concept/parseable/core/parser.hpp"
#include "tenzir/concept/parseable/string/char.hpp"
#include "tenzir/concept/parseable/string/string.hpp"

#include <string>

namespace tenzir {

// -- unary -------------------------------------------------------------------

constexpr auto to_parser(char c) {
  return ignore(parsers::chr{c});
}

inline auto to_parser(std::string str) {
  return ignore(parsers::str{std::move(str)});
}

template <class T>
constexpr auto to_parser(T x)
  requires(std::is_arithmetic_v<T> and not std::is_same_v<T, bool>)
{
  return ignore(parsers::str{std::to_string(x)});
}

template <parser T>
constexpr auto to_parser(T x) -> T {
  return x; // A good compiler will elide the copy.
}

// -- binary ------------------------------------------------------------------

template <class T>
constexpr bool is_convertible_to_unary_parser_v
  = std::is_convertible_v<T, std::string>
    or (std::is_arithmetic_v<T> and not std::is_same_v<T, bool>);

template <class T, class U>
constexpr bool is_convertible_to_binary_parser_v
  = (parser<T> and parser<U>)
    or (parser<T> and is_convertible_to_unary_parser_v<U>)
    or (is_convertible_to_unary_parser_v<T> and parser<U>);

// clang-format off
template <
  template <class, class> class BinaryParser,
  class T,
  class U
>
using make_binary_parser =
  std::conditional_t<
    parser<T> and parser<U>,
    BinaryParser<T, U>,
    std::conditional_t<
      parser<T> and is_convertible_to_unary_parser_v<U>,
      BinaryParser<T, decltype(to_parser(std::declval<U>()))>,
      std::conditional_t<
        is_convertible_to_unary_parser_v<T> and parser<U>,
        BinaryParser<decltype(to_parser(std::declval<T>())), U>,
        std::false_type
      >
    >
  >;
// clang-format on

template <template <class, class> class BinaryParser, class T, class U>
constexpr auto to_parser(T&& x, U&& y)
  -> make_binary_parser<BinaryParser, decltype(to_parser(std::forward<T>(x))),
                        decltype(to_parser(std::forward<U>(y)))>
  requires(is_convertible_to_binary_parser_v<std::decay_t<T>, std::decay_t<U>>)
{
  return {to_parser(std::forward<T>(x)), to_parser(std::forward<U>(y))};
}

} // namespace tenzir
