//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/support/unused_type.hpp"
#include "vast/detail/type_traits.hpp"

#include <iterator>
#include <tuple>
#include <type_traits>

namespace vast {

template <class, class>
class when_parser;

template <class, class>
class action_parser;

template <class, class>
class guard_parser;

template <class Derived>
struct parser_base {
  template <class Condition>
  auto when(Condition fun) const {
    return when_parser<Derived, Condition>{derived(), fun};
  }

  template <class Action>
  [[nodiscard]] auto then(Action fun) const {
    return action_parser<Derived, Action>{derived(), fun};
  }

  template <class Action>
  auto operator->*(Action fun) const {
    return then(fun);
  }

  template <class Guard>
  [[nodiscard]] auto with(Guard fun) const {
    return guard_parser<Derived, Guard>{derived(), fun};
  }

  template <size_t N, class Attribute = unused_type>
  bool operator()(const char (&r)[N], Attribute& a = unused) const {
    // Because there exists overload of std::begin / std::end for char arrays,
    // we must have this overload and strip the NUL byte at the end.
    auto f = r;
    auto l = r + N - 1; // No NUL byte.
    return derived().parse(f, l, a) && f == l;
  }

  // FIXME: don't ignore ADL.
  template <class Range, class Attribute = unused_type>
  auto operator()(Range&& r, Attribute& a = unused) const
  -> decltype(std::begin(r), std::end(r), bool()) {
    auto f = std::begin(r);
    auto l = std::end(r);
    return derived().parse(f, l, a) && f == l;
  }

  // FIXME: don't ignore ADL.
  template <class Range, class A0, class A1, class... As>
  auto operator()(Range&& r, A0& a0, A1& a1, As&... as) const
  -> decltype(std::begin(r), std::end(r), bool()) {
    auto t = std::tie(a0, a1, as...);
    return operator()(r, t);
  }

  template <class Iterator, class Attribute>
  auto operator()(Iterator& f, const Iterator& l, Attribute& a) const
  -> decltype(*f, ++f, f == l, bool()) {
    return derived().parse(f, l, a);
  }

  template <class Iterator, class A0, class A1, class... As>
  auto operator()(Iterator& f, const Iterator& l, A0& a0, A1& a1,
                  As&... as) const
  -> decltype(*f, ++f, f == l, bool()) {
    auto t = std::tie(a0, a1, as...);
    return derived().parse(f, l, t);
  }

private:
  [[nodiscard]] const Derived& derived() const {
    return static_cast<const Derived&>(*this);
  }
};

/// Associates a parser for a given type. To register a parser with a type, one
/// needs to specialize this struct and expose a member `type` with the
/// concrete parser type.
/// @tparam T the type to register a parser with.
template <class T, class = void>
struct parser_registry;

/// Retrieves a registered parser.
template <class T>
using make_parser = typename parser_registry<T>::type;

/// Checks whether the parser registry has a given type registered.
template <class T>
concept has_parser_v = requires {
  typename parser_registry<T>::type;
};

/// Checks whether a given type is-a parser, i.e., derived from ::vast::parser.
template <class T>
using is_parser = std::is_base_of<parser_base<T>, T>;

template <class T>
using is_parser_t = typename is_parser<T>::type;

template <class T>
concept is_parser_v = is_parser<std::decay_t<T>>::value;

} // namespace vast

