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

#include "vast/concept/convertible/is_convertible.hpp"
#include "vast/error.hpp"
#include "vast/expected.hpp"

namespace vast {

/// Converts one type to another.
/// @tparam To The type to convert `From` to.
/// @tparam From The type to convert to `To`.
/// @param from The instance to convert.
/// @returns *from* converted to `T`.
template <class To, class From, class... Opts>
auto to(From&& from, Opts&&... opts)
  -> std::enable_if_t<
       is_convertible<std::decay_t<From>, To>{},
       expected<To>
     > {
  expected<To> x{To()};
  if (convert(from, *x, std::forward<Opts>(opts)...))
    return x;
  return make_error(ec::convert_error);
}

template <class To, class From, class... Opts>
auto to_string(From&& from, Opts&&... opts)
  -> std::enable_if_t<
       std::is_same<To, std::string>{}
        && is_convertible<std::decay_t<From>, To>{}, To
     > {
  std::string str;
  if (convert(from, str, std::forward<Opts>(opts)...))
    return str;
  return {}; // TODO: throw?
}

} // namespace vast

