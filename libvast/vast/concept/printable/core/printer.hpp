//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/support/unused_type.hpp"

#include <iterator>
#include <tuple>
#include <type_traits>

namespace vast {

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

  // FIXME: don't ignore ADL.
  template <class Range, class Attribute = unused_type>
  auto operator()(Range&& r, const Attribute& a = unused) const
  -> decltype(std::begin(r), std::end(r), bool()) {
    auto out = std::back_inserter(r);
    return derived().print(out, a);
  }

  // FIXME: don't ignore ADL.
  template <class Range, class A0, class A1, class... As>
  auto operator()(Range&& r, const A0& a0, const A1& a1, const As&... as) const
  -> decltype(std::begin(r), std::end(r), bool()) {
    return operator()(r, std::tie(a0, a1, as...));
  }

  template <class Iterator, class Attribute = unused_type>
  auto operator()(Iterator&& out, const Attribute& a = unused) const
  -> decltype(*out, ++out, bool()) {
    return derived().print(out, a);
  }

  template <class Iterator, class A0, class A1, class... As>
  auto operator()(Iterator&& out, const A0& a0, const A1& a1, const As&... as) const
  -> decltype(*out, ++out, bool()) {
    return operator()(out, std::tie(a0, a1, as...));
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
concept registered_printer = requires {
  typename printer_registry<T>::type;
};

/// Checks whether a given type is-a printer, i.e., derived from
/// ::vast::printer.
template <class T>
using is_printer = std::is_base_of<printer_base<T>, T>;

template <class T>
concept printer = is_printer<std::decay_t<T>>::value;

} // namespace vast

