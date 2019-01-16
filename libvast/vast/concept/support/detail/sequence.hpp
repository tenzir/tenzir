/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <tuple>

#include "vast/concept/support/unused_type.hpp"
#include "vast/detail/type_traits.hpp"

namespace vast::detail {

template <typename T>
using has_lhs_type_t = typename T::lhs_type;

template <typename T>
inline constexpr bool has_lhs_type_v = is_detected_v<has_lhs_type_t, T>;

template <typename T>
using has_rhs_type_t = typename T::rhs_type;

template <typename T>
inline constexpr bool has_rhs_type_v = is_detected_v<has_rhs_type_t, T>;

template <typename T>
inline constexpr bool is_sequencer_v = has_lhs_type_v<T> && has_rhs_type_v<T>;

template <class T>
constexpr size_t compute_right_tuple_index() {
  if constexpr (!is_sequencer_v<T>)
    return 0;
  else if constexpr (is_unused_type_v<typename T::lhs_attribute>
                     || is_unused_type_v<typename T::rhs_attribute>)
    return compute_right_tuple_index<typename T::lhs_type>();
  else
    return 1 + compute_right_tuple_index<typename T::lhs_type>();
}

template <class Sequencer, class Tuple>
constexpr decltype(auto) access_left(Tuple&& x) {
  if constexpr (is_sequencer_v<typename Sequencer::lhs_type>)
    return std::forward<Tuple>(x);
  else 
    return std::get<0>(std::forward<Tuple>(x));
}

template <class Sequencer, class Tuple>
constexpr decltype(auto) access_right(Tuple&& x) {
  constexpr auto i = compute_right_tuple_index<Sequencer>();
  return std::get<i>(std::forward<Tuple>(x));
}

} // namespace vast::detail
