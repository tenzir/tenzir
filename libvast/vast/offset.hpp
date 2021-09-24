//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/stack_vector.hpp"

#include <caf/meta/type_name.hpp>

#include <compare>
#include <cstddef>

namespace vast {

/// A sequence of indexes to recursively access a type or value.
struct offset : detail::stack_vector<size_t, 64> {
  using super = detail::stack_vector<size_t, 64>;
  using super::super;

  template <class Inspector>
  friend auto inspect(Inspector& f, offset& x) ->
    typename Inspector::result_type {
    return f(caf::meta::type_name("vast.offset"), static_cast<super&>(x));
  }

  friend std::strong_ordering
  operator<=>(const offset& lhs, const offset& rhs) noexcept;
};

} // namespace vast
