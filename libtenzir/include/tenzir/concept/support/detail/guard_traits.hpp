//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/detail/callable_trait.hpp>

#include <type_traits>

namespace tenzir::detail {

template <class Guard>
struct guard_traits {
  using traits = caf::detail::get_callable_trait<Guard>;
  using result_type = typename traits::result_type;

  static constexpr auto arity = traits::num_args;
  static constexpr bool returns_bool = std::is_same_v<result_type, bool>;
  static constexpr bool no_args_returns_bool = arity == 0 and returns_bool;
  static constexpr bool one_arg_returns_bool = arity == 1 and returns_bool;
};

} // namespace tenzir::detail
