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

#ifndef VAST_CONCEPT_SUPPORT_DETAIL_GUARD_TRAITS_HPP
#define VAST_CONCEPT_SUPPORT_DETAIL_GUARD_TRAITS_HPP

#include <type_traits>

#include <caf/detail/type_traits.hpp>

namespace vast {
namespace detail {

template <class Guard>
struct guard_traits {
  using traits = caf::detail::get_callable_trait<Guard>;
  using result_type = typename traits::result_type;

  static constexpr auto arity = traits::num_args;
  static constexpr bool returns_bool = std::is_same<result_type, bool>::value;
  static constexpr bool no_args_returns_bool = arity == 0 && returns_bool;
  static constexpr bool one_arg_returns_bool = arity == 1 && returns_bool;
};

} // namespace detail
} // namespace vast

#endif

