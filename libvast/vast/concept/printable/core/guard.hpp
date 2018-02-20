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

#ifndef VAST_CONCEPT_PRINTABLE_CORE_GUARD_HPP
#define VAST_CONCEPT_PRINTABLE_CORE_GUARD_HPP

#include "vast/concept/support/detail/guard_traits.hpp"

namespace vast {

/// Attaches a guard expression to a printer that must succeed before the
/// printer executes.
/// @tparam Printer The printer to augment with a guard expression.
/// @tparam Guard A function that either takes no arguments or the attribute by
///               const-reference and returns `bool`.
template <class Printer, class Guard>
class guard_printer : public printer<guard_printer<Printer, Guard>> {
public:
  using attribute = typename Printer::attribute;

  guard_printer(Printer p, Guard fun) : printer_{std::move(p)}, guard_(fun) {
  }

  template <class Iterator>
  bool print(Iterator& out, unused_type) const {
    return guard_() && printer_.print(out, unused);
  }

  template <class Iterator, class Attribute, class G = Guard>
  auto print(Iterator& out, const Attribute& a) const
  -> std::enable_if_t<detail::guard_traits<G>::no_args_returns_bool, bool> {
    return guard_() && printer_.print(out, a);
  }

  template <class Iterator, class Attribute, class G = Guard>
  auto print(Iterator& out, const Attribute& a) const
  -> std::enable_if_t<detail::guard_traits<G>::one_arg_returns_bool, bool> {
    return guard_(a) && printer_.print(out, a);
  }

private:
  Printer printer_;
  Guard guard_;
};

} // namespace vast

#endif

