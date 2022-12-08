//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/operators.hpp"
#include "vast/hash/hash.hpp"
#include "vast/hash/uniquely_represented.hpp"

#include <cstdint>

namespace vast {

struct integer : detail::totally_ordered<integer> {
  using value_type = int64_t;

  value_type value = 0;

  integer();
  integer(const integer&);
  integer(integer&&) noexcept;

  explicit integer(int64_t v);

  integer& operator=(const integer&);
  integer& operator=(integer&&) noexcept;

  friend integer operator+(integer lhs, integer rhs);

  friend bool operator==(integer lhs, integer rhs);

  friend bool operator<(integer lhs, integer rhs);

  template <class Inspector>
  friend auto inspect(Inspector& f, integer& x) {
    return f.object(x)
      .pretty_name("vast.integer")
      .fields(f.field("value", x.value));
  }
};

template <>
struct is_uniquely_represented<integer>
  : std::bool_constant<sizeof(integer) == sizeof(integer::value_type)> {};

} // namespace vast

namespace std {

template <>
struct hash<vast::integer> {
  size_t operator()(const vast::integer& x) const {
    return vast::hash(x);
  }
};

} // namespace std
