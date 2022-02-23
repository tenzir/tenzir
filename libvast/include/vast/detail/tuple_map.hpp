//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/detail/assert.hpp"

#include <tuple>
#include <utility>

namespace vast::detail {

template <class Tuple, class F, class Range, size_t... Is>
Tuple tuple_map_impl(Range&& xs, F&& f, std::index_sequence<Is...>) {
  return std::make_tuple(
    (f.template operator()<std::tuple_element_t<Is, Tuple>>(
      std::forward<decltype(xs[Is])>(xs[Is])))...);
}

/// Turn a random access range into a tuple by applying f to every element of
/// xs. The type of the tuple element at position `n` is supplied for the nth
/// element of xs.
template <class Tuple, class F, class Range>
Tuple tuple_map(Range&& xs, F&& f) {
  constexpr auto tuple_size = std::tuple_size_v<Tuple>;
  VAST_ASSERT(xs.size() == tuple_size);
  return tuple_map_impl<Tuple>(std::forward<Range>(xs), std::forward<F>(f),
                               std::make_index_sequence<tuple_size>{});
}

} // namespace vast::detail
