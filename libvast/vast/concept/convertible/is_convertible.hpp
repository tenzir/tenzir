// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <type_traits>

namespace vast {
namespace detail {

struct is_convertible {
  template <class From, class To>
  static auto test(const From* from, To* to)
    -> decltype(convert(*from, *to), std::true_type());

  template <class, class>
  static auto test(...) -> std::false_type;
};

} // namespace detail

/// Type trait that checks whether a type is convertible to another.
template <class From, class To>
struct is_convertible
  : decltype(detail::is_convertible::test<std::decay_t<From>, To>(0, 0)) {};

} // namespace vast

