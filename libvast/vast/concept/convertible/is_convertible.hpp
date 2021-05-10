//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
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
