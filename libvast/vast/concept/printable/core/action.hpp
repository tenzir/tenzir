//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/support/detail/action_traits.hpp"

namespace vast {

/// Executes a function before printing the inner attribute.
template <class Printer, class Action>
class action_printer : public printer<action_printer<Printer, Action>> {
public:
  using inner_attribute = typename Printer::attribute;
  using action_traits = detail::action_traits<Action>;
  using action_arg_type = typename action_traits::first_arg_type;

  using attribute = std::conditional_t<action_traits::returns_void, unused_type,
                                       typename action_traits::result_type>;

  action_printer(Printer p, Action fun) : printer_{std::move(p)}, action_(fun) {
    // nop
  }

  template <class Iterator, class Attribute>
  bool print(Iterator& out, const Attribute& attr) const {
    if constexpr (action_traits::no_args_returns_void) {
      action_();
      return printer_.print(out, attr);
    } else if constexpr (action_traits::one_arg_returns_void) {
      action_(attr);
      return printer_.print(out, attr);
    } else if constexpr (action_traits::no_args_returns_non_void) {
      auto x = action_();
      return printer_.print(out, x);
    } else {
      auto x = action_(attr);
      return printer_.print(out, x);
    }
  }

private:
  Printer printer_;
  Action action_;
};

} // namespace vast
