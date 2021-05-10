//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core/to_parser.hpp"

#include <type_traits>

namespace vast {

template <class>
class and_parser;

template <class>
class maybe_parser;

template <class>
class not_parser;

template <class>
class optional_parser;

template <class>
class kleene_parser;

template <class>
class plus_parser;

template <class, class>
class difference_parser;

template <class, class>
class list_parser;

template <class, class>
class sequence_parser;

template <class, class>
class choice_parser;

//
// Unary
//

template <class T>
constexpr auto operator&(T&& x)
  -> std::enable_if_t<is_parser_v<std::decay_t<T>>,
                      and_parser<std::decay_t<T>>> {
  return and_parser<std::decay_t<T>>{std::forward<T>(x)};
}

template <class T>
constexpr auto operator!(T&& x)
  -> std::enable_if_t<is_parser_v<std::decay_t<T>>,
                      not_parser<std::decay_t<T>>> {
  return not_parser<std::decay_t<T>>{std::forward<T>(x)};
}

template <class T>
constexpr auto operator-(T&& x)
  -> std::enable_if_t<is_parser_v<std::decay_t<T>>,
                      optional_parser<std::decay_t<T>>> {
  return optional_parser<std::decay_t<T>>{std::forward<T>(x)};
}

template <class T>
constexpr auto operator*(T&& x)
  -> std::enable_if_t<is_parser_v<std::decay_t<T>>,
                      kleene_parser<std::decay_t<T>>> {
  return kleene_parser<std::decay_t<T>>{std::forward<T>(x)};
}

template <class T>
constexpr auto operator+(T&& x)
  -> std::enable_if_t<is_parser_v<std::decay_t<T>>,
                      plus_parser<std::decay_t<T>>> {
  return plus_parser<std::decay_t<T>>{std::forward<T>(x)};
}

template <class T>
constexpr auto operator~(T&& x)
  -> std::enable_if_t<is_parser_v<std::decay_t<T>>,
                      maybe_parser<std::decay_t<T>>> {
  return maybe_parser<std::decay_t<T>>{std::forward<T>(x)};
}

//
// Binary
//

template <class LHS, class RHS>
constexpr auto operator-(LHS&& lhs, RHS&& rhs)
  -> decltype(to_parser<difference_parser>(lhs, rhs)) {
  return {to_parser(std::forward<LHS>(lhs)), to_parser(std::forward<RHS>(rhs))};
}

template <class LHS, class RHS>
constexpr auto operator%(LHS&& lhs, RHS&& rhs)
  -> decltype(to_parser<list_parser>(lhs, rhs)) {
  return {to_parser(std::forward<LHS>(lhs)), to_parser(std::forward<RHS>(rhs))};
}

template <class LHS, class RHS>
constexpr auto operator>>(LHS&& lhs, RHS&& rhs)
  -> decltype(to_parser<sequence_parser>(lhs, rhs)) {
  return {to_parser(std::forward<LHS>(lhs)), to_parser(std::forward<RHS>(rhs))};
}

template <class LHS, class RHS>
constexpr auto operator|(LHS&& lhs, RHS&& rhs)
  -> decltype(to_parser<choice_parser>(lhs, rhs)) {
  return {to_parser(std::forward<LHS>(lhs)), to_parser(std::forward<RHS>(rhs))};
}

} // namespace vast
