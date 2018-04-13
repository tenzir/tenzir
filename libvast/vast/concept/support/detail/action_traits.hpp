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

#include <type_traits>

#include <caf/detail/type_traits.hpp>

#include "vast/detail/type_list.hpp"

namespace vast::detail {

template <class Action>
struct action_traits {
  using traits = caf::detail::get_callable_trait<Action>;

  using first_arg_type =
    std::remove_reference_t<tl_head_t<typename traits::arg_types>>;

  using result_type = typename traits::result_type;

  static constexpr auto arity = traits::num_args;
  static constexpr bool returns_void = std::is_void_v<result_type>;
  static constexpr bool no_args_returns_void = arity == 0 && returns_void;
  static constexpr bool one_arg_returns_void = arity == 1 && returns_void;
  static constexpr bool no_args_returns_non_void = arity == 0 && !returns_void;
  static constexpr bool one_arg_returns_non_void = arity == 1 && !returns_void;
};

} // namespace vast::detail


