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

#ifndef VAST_CONCEPT_CONVERTIBLE_IS_CONVERTIBLE_HPP
#define VAST_CONCEPT_CONVERTIBLE_IS_CONVERTIBLE_HPP

#include <type_traits>

namespace vast {
namespace detail {

struct is_convertible {
  template <class From, class To>
  static auto test(const From* from, To* to)
    -> decltype(convert(*from, *to), std::true_type());

  template <class, class>
  static auto test(...) -> std::false_type;
};

} // namespace detail

/// Type trait that checks whether a type is convertible to another.
template <class From, class To>
struct is_convertible
  : decltype(detail::is_convertible::test<std::decay_t<From>, To>(0, 0)) {};

} // namespace vast

#endif
