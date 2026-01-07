//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/type_list.hpp"

#include <caf/detail/callable_trait.hpp>

#include <type_traits>

namespace tenzir::detail {

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

} // namespace tenzir::detail
