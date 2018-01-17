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

#ifndef VAST_CONCEPT_PARSEABLE_CORE_OPERATORS_HPP
#define VAST_CONCEPT_PARSEABLE_CORE_OPERATORS_HPP

#include <type_traits>

#include "vast/concept/parseable/detail/as_parser.hpp"

namespace vast {

template <typename>
class and_parser;

template <typename>
class maybe_parser;

template <typename>
class not_parser;

template <typename>
class optional_parser;

template <typename>
class kleene_parser;

template <typename>
class plus_parser;

template <typename, typename>
class difference_parser;

template <typename, typename>
class list_parser;

template <typename, typename>
class sequence_parser;

template <typename, typename>
class choice_parser;

//
// Unary
//

template <typename T>
auto operator&(T&& x)
  -> std::enable_if_t<
       is_parser<std::decay_t<T>>{},
       and_parser<std::decay_t<T>>
     > {
  return and_parser<std::decay_t<T>>{std::forward<T>(x)};
}

template <typename T>
auto operator!(T&& x)
  -> std::enable_if_t<
       is_parser<std::decay_t<T>>{},
       not_parser<std::decay_t<T>>
     > {
  return not_parser<std::decay_t<T>>{std::forward<T>(x)};
}

template <typename T>
auto operator-(T&& x)
  -> std::enable_if_t<
       is_parser<std::decay_t<T>>{},
       optional_parser<std::decay_t<T>>
     > {
  return optional_parser<std::decay_t<T>>{std::forward<T>(x)};
}

template <typename T>
auto operator*(T&& x)
  -> std::enable_if_t<
       is_parser<std::decay_t<T>>{},
       kleene_parser<std::decay_t<T>>
     > {
  return kleene_parser<std::decay_t<T>>{std::forward<T>(x)};
}

template <typename T>
auto operator+(T&& x)
  -> std::enable_if_t<
       is_parser<std::decay_t<T>>{},
       plus_parser<std::decay_t<T>>
     > {
  return plus_parser<std::decay_t<T>>{std::forward<T>(x)};
}

template <typename T>
auto operator~(T&& x)
  -> std::enable_if_t<
       is_parser<std::decay_t<T>>{},
       maybe_parser<std::decay_t<T>>
     > {
  return maybe_parser<std::decay_t<T>>{std::forward<T>(x)};
}

//
// Binary
//

template <typename LHS, typename RHS>
auto operator-(LHS&& lhs, RHS&& rhs)
  -> decltype(detail::as_parser<difference_parser>(lhs, rhs)) {
  return {detail::as_parser(std::forward<LHS>(lhs)),
          detail::as_parser(std::forward<RHS>(rhs))};
}

template <typename LHS, typename RHS>
auto operator%(LHS&& lhs, RHS&& rhs)
  -> decltype(detail::as_parser<list_parser>(lhs, rhs)) {
  return {detail::as_parser(std::forward<LHS>(lhs)),
          detail::as_parser(std::forward<RHS>(rhs))};
}

template <typename LHS, typename RHS>
auto operator>>(LHS&& lhs, RHS&& rhs)
  -> decltype(detail::as_parser<sequence_parser>(lhs, rhs)) {
  return {detail::as_parser(std::forward<LHS>(lhs)),
          detail::as_parser(std::forward<RHS>(rhs))};
}

template <typename LHS, typename RHS>
auto operator|(LHS&& lhs, RHS&& rhs)
  -> decltype(detail::as_parser<choice_parser>(lhs, rhs)) {
  return {detail::as_parser(std::forward<LHS>(lhs)),
          detail::as_parser(std::forward<RHS>(rhs))};
}

} // namespace vast

#endif
