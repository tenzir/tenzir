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

#include <array>
#include <utility>

namespace vast::detail {

namespace impl {

template <typename T, std::size_t... Is>
constexpr std::array<T, sizeof...(Is)>
generate_array(const T& value, std::index_sequence<Is...>) {
  return {{(static_cast<void>(Is), value)...}};
}

} // namespace impl

template <std::size_t N, typename T>
constexpr std::array<T, N> generate_array(const T& value) {
  return impl::generate_array(value, std::make_index_sequence<N>());
}

} // namespace vast::detail
