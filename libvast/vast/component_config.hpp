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

#include "vast/concept/parseable/parse.hpp"

#include <caf/config_value.hpp>
#include <caf/settings.hpp>
#include <caf/sum_type.hpp>

namespace vast {

/// Extracts a value from a settings object and assigns it to a variable.
/// @param to The value to assign to.
/// @param from The settings that holds the data.
/// @param path The location of the data inside the settings object.
/// @returns false on a type mismatch, true otherwise.
template <class T>
bool absorb(T& to, const caf::settings& from, std::string_view path) {
  auto cv = caf::get_if(&from, path);
  if (!cv)
    return true;
  if constexpr (caf::detail::tl_contains<caf::config_value::variant_type::types,
                                         T>::value) {
    auto x = caf::get_if<T>(&*cv);
    if (!x)
      return false;
    to = *x;
    return true;
  } else {
    auto x = caf::get_if<std::string>(&*cv);
    if (!x)
      return false;
    auto f = x->begin();
    return parse(f, x->end(), to);
  }
}

} // namespace vast
