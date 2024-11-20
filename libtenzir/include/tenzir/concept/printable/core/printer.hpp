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
class action_printer;

template <class, class>
class guard_printer;

template <class Derived>
struct printer_base {
  template <class Action>
  [[nodiscard]] auto before(Action fun) const {
    return action_printer<Derived, Action>{derived(), fun};
  }

  template <class Action>
  auto operator->*(Action fun) const {
    return before(fun);
  }

  template <class Guard>
  [[nodiscard]] auto with(Guard fun) const {
    return guard_printer<Derived, Guard>{derived(), fun};
  }

  template <class... TAttributes>
  auto operator()(std::ranges::range auto&& r,
                  const TAttributes&... attributes) const {
    if constexpr (sizeof...(TAttributes) == 0) {
      auto out = std::back_inserter(r);
      return derived().print(out, unused);
    } else if constexpr (sizeof...(TAttributes) == 1) {
      auto out = std::back_inserter(r);
      return derived().print(out, attributes...);
    } else {
      return operator()(r, std::tie(attributes...));
    }
  }

  template <class Iterator, class... TAttributes>
    requires requires(Iterator out) {
      *out;
      ++out;
    }
  auto operator()(Iterator&& out, const TAttributes&... attributes) const {
    if constexpr (sizeof...(TAttributes) == 0) {
      return derived().print(out, unused);
    } else if constexpr (sizeof...(TAttributes) == 1) {
      return derived().print(out, attributes...);
    } else {
      return operator()(out, std::tie(attributes...));
    }
  }

private:
  [[nodiscard]] const Derived& derived() const {
    return static_cast<const Derived&>(*this);
  }
};

/// Associates a printer for a given type. To register a printer with a type,
/// one needs to specialize this struct and expose a member `type` with the
/// concrete printer type.
/// @tparam T the type to register a printer with.
template <class T>
struct printer_registry;

/// Retrieves a registered printer.
template <class T>
using make_printer = typename printer_registry<T>::type;

/// Checks whether the printer registry has a given type registered.
template <class T>
concept registered_printer = requires { typename printer_registry<T>::type; };

/// Checks whether a given type is-a printer, i.e., derived from
/// ::tenzir::printer.
template <class T>
using is_printer = std::is_base_of<printer_base<T>, T>;

template <class T>
concept printer = is_printer<std::decay_t<T>>::value;

} // namespace tenzir
