//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/operators.hpp"

namespace vast {

struct unused_type : detail::equality_comparable<unused_type>,
                     detail::integer_arithmetic<unused_type> {
  unused_type() = default;

  template <class T>
  unused_type(const T&) {
  }

  template <class T>
  unused_type& operator=(const T&) {
    return *this;
  }

  template <class T>
  const unused_type& operator=(const T&) const {
    return *this;
  }

  template <class T>
  const unused_type& operator+=(T&&) const {
    return *this;
  }

  template <class T>
  const unused_type& operator-=(T&&) const {
    return *this;
  }

  template <class T>
  const unused_type& operator*=(T&&) const {
    return *this;
  }

  template <class T>
  const unused_type& operator/=(T&&) const {
    return *this;
  }

  friend inline bool operator==(unused_type, unused_type) {
    return true;
  }
};

static auto unused = unused_type{};

//
// Unary
//

inline unused_type operator-(unused_type) {
  return unused;
}

//
// Binary
//

template <class T>
using is_unused_type = std::is_same<T, unused_type>;

template <class T>
using is_unused_type_t = typename is_unused_type<T>::type;

template <class T>
constexpr auto is_unused_type_v = is_unused_type<T>::value;

} // namespace vast
