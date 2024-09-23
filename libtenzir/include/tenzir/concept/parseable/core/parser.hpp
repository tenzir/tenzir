//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/support/unused_type.hpp"
#include "tenzir/concepts.hpp"

#include <iterator>
#include <tuple>
#include <type_traits>

namespace tenzir {

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

  template <class... TAttributes>
  auto operator()(std::ranges::range auto&& r,
                  TAttributes&... attributes) const -> bool {
    auto f = std::begin(r);
    auto l = std::end(r);
    if constexpr (sizeof...(TAttributes) == 0) {
      return derived().parse(f, l, unused) && f == l;
    } else if constexpr (sizeof...(TAttributes) == 1) {
      return derived().parse(f, l, attributes...) && f == l;
    } else {
      auto t = std::tie(attributes...);
      return derived().parse(f, l, t);
    }
  }

  template <class Iterator, class... TAttributes>
    requires requires(Iterator first, Iterator last) {
      *first;
      ++first;
      first == last;
    }
  auto operator()(Iterator& first, const Iterator& last,
                  TAttributes&... attributes) const {
    if constexpr (sizeof...(TAttributes) == 1) {
      return derived().parse(first, last, attributes...);
    } else {
      auto t = std::tie(attributes...);
      return derived().parse(first, last, t);
    }
  }

  template <class Iterator, class D = Derived>
  auto apply(Iterator& f,
             const Iterator& l) const -> std::optional<typename D::attribute> {
    auto result = typename D::attribute{};
    if (!(*this)(f, l, result)) {
      return std::nullopt;
    }
    return result;
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
concept registered_parser_type
  = requires { typename parser_registry<T>::type; };

/// Checks whether a given type is-a parser, i.e., derived from ::tenzir::parser.
template <class T>
using is_parser = std::is_base_of<parser_base<T>, T>;

template <class T>
using is_parser_t = typename is_parser<T>::type;

template <class T>
concept parser = is_parser<std::decay_t<T>>::value;

} // namespace tenzir
