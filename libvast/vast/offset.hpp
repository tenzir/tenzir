// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/stack_vector.hpp"

#include <caf/meta/type_name.hpp>

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
};

} // namespace vast
