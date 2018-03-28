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

#include "vast/concept/printable/detail/as_printer.hpp"

namespace vast {

template <class>
class and_printer;

template <class>
class not_printer;

template <class>
class optional_printer;

template <class>
class kleene_printer;

template <class>
class plus_printer;

template <class>
class maybe_printer;

template <class, class>
class list_printer;

template <class, class>
class sequence_printer;

template <class, class>
class choice_printer;

// -- unary ------------------------------------------------------------------

template <class T>
auto operator&(T&& x)
-> std::enable_if_t<
     is_printer<std::decay_t<T>>{},
     and_printer<std::decay_t<T>>
   > {
  return and_printer<std::decay_t<T>>{std::forward<T>(x)};
}

template <class T>
auto operator!(T&& x)
-> std::enable_if_t<
     is_printer<std::decay_t<T>>{},
     not_printer<std::decay_t<T>>
   > {
  return not_printer<std::decay_t<T>>{std::forward<T>(x)};
}

template <class T>
auto operator-(T&& x)
-> std::enable_if_t<
     is_printer<std::decay_t<T>>{},
     optional_printer<std::decay_t<T>>
   > {
  return optional_printer<std::decay_t<T>>{std::forward<T>(x)};
}

template <class T>
auto operator*(T&& x)
-> std::enable_if_t<
     is_printer<std::decay_t<T>>{},
     kleene_printer<std::decay_t<T>>
   > {
  return kleene_printer<std::decay_t<T>>{std::forward<T>(x)};
}

template <class T>
auto operator+(T&& x)
-> std::enable_if_t<
     is_printer<std::decay_t<T>>{},
     plus_printer<std::decay_t<T>>
   > {
  return plus_printer<std::decay_t<T>>{std::forward<T>(x)};
}

template <class T>
auto operator~(T&& x)
-> std::enable_if_t<
     is_printer<std::decay_t<T>>{},
     maybe_printer<std::decay_t<T>>
   > {
  return maybe_printer<std::decay_t<T>>{std::forward<T>(x)};
}

// -- binary -----------------------------------------------------------------

template <class LHS, class RHS>
auto operator%(LHS&& lhs, RHS&& rhs)
  -> decltype(detail::as_printer<list_printer>(lhs, rhs)) {
  return {detail::as_printer(std::forward<LHS>(lhs)),
          detail::as_printer(std::forward<RHS>(rhs))};
}

template <class LHS, class RHS>
auto operator<<(LHS&& lhs, RHS&& rhs)
  -> decltype(detail::as_printer<sequence_printer>(lhs, rhs)) {
  return {detail::as_printer(std::forward<LHS>(lhs)),
          detail::as_printer(std::forward<RHS>(rhs))};
}

template <class LHS, class RHS>
auto operator|(LHS&& lhs, RHS&& rhs)
  -> decltype(detail::as_printer<choice_printer>(lhs, rhs)) {
  return {detail::as_printer(std::forward<LHS>(lhs)),
          detail::as_printer(std::forward<RHS>(rhs))};
}

} // namespace vast

