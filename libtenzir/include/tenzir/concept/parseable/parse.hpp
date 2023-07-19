//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/access.hpp"
#include "tenzir/concept/parseable/core/parser.hpp"

#include <type_traits>

namespace tenzir {

template <class Iterator, registered_parser_type T, class... Args>
auto parse(Iterator& f, const Iterator& l, T& x, Args&&... args) -> bool {
  return make_parser<T>{std::forward<Args>(args)...}(f, l, x);
}

template <class Iterator, access_parser T, class... Args>
  requires(!registered_parser_type<T>)
auto parse(Iterator& f, const Iterator& l, T& x, Args&&... args) -> bool {
  return access::parser_base<T>{std::forward<Args>(args)...}(f, l, x);
}

namespace detail {

template <class Iterator, class T>
bool conjunctive_parse(Iterator& f, const Iterator& l, T& x) {
  return parse(f, l, x);
}

template <class Iterator, class T, class... Ts>
bool conjunctive_parse(Iterator& f, const Iterator& l, T& x, Ts&... xs) {
  return conjunctive_parse(f, l, x) && conjunctive_parse(f, l, xs...);
}

} // namespace detail

template <class Iterator, access_state T>
  requires(!registered_parser_type<T>)
auto parse(Iterator& f, const Iterator& l, T& x) -> bool {
  bool r;
  auto fun = [&](auto&... xs) {
    r = detail::conjunctive_parse(f, l, xs...);
  };
  access::state<T>::call(x, fun);
  return r;
}

template <class I, class T>
concept parseable = requires(I first, I last, T& t) {
  parse(first, last, t);
};

} // namespace tenzir
