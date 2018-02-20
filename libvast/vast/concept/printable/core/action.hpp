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

#ifndef VAST_CONCEPT_PRINTABLE_CORE_ACTION_HPP
#define VAST_CONCEPT_PRINTABLE_CORE_ACTION_HPP

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

  using attribute =
    std::conditional_t<
      action_traits::returns_void,
      unused_type,
      typename action_traits::result_type
    >;

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

#endif
