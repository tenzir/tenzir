/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <type_traits>
#include <iterator>
#include <tuple>

#include "vast/concept/support/unused_type.hpp"

#include "vast/detail/type_traits.hpp"

namespace vast {

template <class, class>
class when_parser;

template <class, class>
class action_parser;

template <class, class>
class guard_parser;

template <class Derived>
struct parser {
  template <class Condition>
  auto when(Condition fun) const {
    return when_parser<Derived, Condition>{derived(), fun};
  }

  template <class Action>
  auto then(Action fun) const {
    return action_parser<Derived, Action>{derived(), fun};
  }

  template <class Action>
  auto operator->*(Action fun) const {
    return then(fun);
  }

  template <class Guard>
  auto with(Guard fun) const {
    return guard_parser<Derived, Guard>{derived(), fun};
  }

  // FIXME: don't ignore ADL.
  template <class Range, class Attribute = unused_type>
  auto operator()(Range&& r, Attribute& a = unused) const
  -> decltype(std::begin(r), std::end(r), bool()) {
    auto f = std::begin(r);
    auto l = std::end(r);
    return derived().parse(f, l, a);
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
  const Derived& derived() const {
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

namespace detail {

struct has_parser {
  template <class T>
  static auto test(T*) -> std::is_class<typename parser_registry<T>::type>;

  template <class>
  static auto test(...) -> std::false_type;
};

} // namespace detail

/// Checks whether the parser registry has a given type registered.
template <class T>
constexpr bool has_parser_v
  = decltype(detail::has_parser::test<T>(0))::value;

/// Checks whether a given type is-a parser, i.e., derived from ::vast::parser.
template <class T>
constexpr bool is_parser_v = std::is_base_of<parser<T>, T>::value;

} // namespace vast

