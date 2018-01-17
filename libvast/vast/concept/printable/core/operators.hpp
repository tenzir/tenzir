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

#ifndef VAST_CONCEPT_PRINTABLE_CORE_OPERATORS_HPP
#define VAST_CONCEPT_PRINTABLE_CORE_OPERATORS_HPP

#include <type_traits>

#include "vast/concept/printable/detail/as_printer.hpp"

namespace vast {

template <typename>
class and_printer;

template <typename>
class not_printer;

template <typename>
class optional_printer;

template <typename>
class kleene_printer;

template <typename>
class plus_printer;

template <typename>
class maybe_printer;

template <typename, typename>
class list_printer;

template <typename, typename>
class sequence_printer;

template <typename, typename>
class choice_printer;

// -- unary ------------------------------------------------------------------

template <typename T>
auto operator&(T&& x)
-> std::enable_if_t<
     is_printer<std::decay_t<T>>{},
     and_printer<std::decay_t<T>>
   > {
  return and_printer<std::decay_t<T>>{std::forward<T>(x)};
}

template <typename T>
auto operator!(T&& x)
-> std::enable_if_t<
     is_printer<std::decay_t<T>>{},
     not_printer<std::decay_t<T>>
   > {
  return not_printer<std::decay_t<T>>{std::forward<T>(x)};
}

template <typename T>
auto operator-(T&& x)
-> std::enable_if_t<
     is_printer<std::decay_t<T>>{},
     optional_printer<std::decay_t<T>>
   > {
  return optional_printer<std::decay_t<T>>{std::forward<T>(x)};
}

template <typename T>
auto operator*(T&& x)
-> std::enable_if_t<
     is_printer<std::decay_t<T>>{},
     kleene_printer<std::decay_t<T>>
   > {
  return kleene_printer<std::decay_t<T>>{std::forward<T>(x)};
}

template <typename T>
auto operator+(T&& x)
-> std::enable_if_t<
     is_printer<std::decay_t<T>>{},
     plus_printer<std::decay_t<T>>
   > {
  return plus_printer<std::decay_t<T>>{std::forward<T>(x)};
}

template <typename T>
auto operator~(T&& x)
-> std::enable_if_t<
     is_printer<std::decay_t<T>>{},
     maybe_printer<std::decay_t<T>>
   > {
  return maybe_printer<std::decay_t<T>>{std::forward<T>(x)};
}

// -- binary -----------------------------------------------------------------

template <typename LHS, typename RHS>
auto operator%(LHS&& lhs, RHS&& rhs)
  -> decltype(detail::as_printer<list_printer>(lhs, rhs)) {
  return {detail::as_printer(std::forward<LHS>(lhs)),
          detail::as_printer(std::forward<RHS>(rhs))};
}

template <typename LHS, typename RHS>
auto operator<<(LHS&& lhs, RHS&& rhs)
  -> decltype(detail::as_printer<sequence_printer>(lhs, rhs)) {
  return {detail::as_printer(std::forward<LHS>(lhs)),
          detail::as_printer(std::forward<RHS>(rhs))};
}

template <typename LHS, typename RHS>
auto operator|(LHS&& lhs, RHS&& rhs)
  -> decltype(detail::as_printer<choice_printer>(lhs, rhs)) {
  return {detail::as_printer(std::forward<LHS>(lhs)),
          detail::as_printer(std::forward<RHS>(rhs))};
}

} // namespace vast

#endif
