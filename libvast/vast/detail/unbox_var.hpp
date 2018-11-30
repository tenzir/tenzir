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

namespace vast::detail {

/// Either declares the local variable `var_name` from `expr` or returns with
/// an error.
#define VAST_UNBOX_VAR(var_name, expr)                                         \
  std::remove_reference_t<decltype(*(expr))> var_name;                         \
  if (auto maybe = expr; !maybe)                                               \
    return std::move(maybe.error());                                           \
  else                                                                         \
    var_name = std::move(*maybe);

} // namespace vast::detail
