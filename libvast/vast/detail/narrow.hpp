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

// This file comes from a 3rd party and has been adapted to fit into the VAST
// code base. Details about the original file:
//
// - Repository: https://github.com/Microsoft/GSL
// - Commit:     d6b26b367b294aca43ff2d28c50293886ad1d5d4
// - Path:       GSL/include/gsl/gsl_util
// - Author:     Microsoft
// - Copyright:  (c) 2015 Microsoft Corporation. All rights reserved.
// - License:    MIT

#pragma once

#include <exception>
#include <type_traits>

namespace vast::detail {

/// A searchable way to do narrowing casts of values.
template <class T, class U>
constexpr T narrow_cast(U&& u) noexcept {
  return static_cast<T>(std::forward<U>(u));
}

/// @relates narrow
struct narrowing_error : public std::exception {};

template <class T, class U>
struct is_same_signedness
  : public std::integral_constant<bool, std::is_signed<T>::value
                                          == std::is_signed<U>::value> {};

/// A checked version of narrow_cast that throws if the cast changed the value.
template <class T, class U>
T narrow(U y) {
  T x = narrow_cast<T>(y);
  if (static_cast<U>(x) != y)
    throw narrowing_error{};
  if (!is_same_signedness<T, U>::value && ((x < T{}) != (y < U{})))
    throw narrowing_error{};
  return x;
}

} // namespace vast::detail
