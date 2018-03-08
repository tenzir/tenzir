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

#ifndef VAST_DETAIL_OVERLOAD_HPP
#define VAST_DETAIL_OVERLOAD_HPP

namespace vast::detail {

/// Creates a set of overloaded functions. This utility function allows for
/// writing inline visitors without having to result to inversion of control.
/// @param funs A list of function objects.
/// @returns An overloaded function object representing the union of *funs*.
template <class... Functions>
auto overload(Functions... funs) {
  struct lambda : Functions... {
    lambda(Functions... funs) : Functions(std::move(funs))... {}
    using Functions::operator()...;
  };
  return lambda(std::move(funs)...);
}


} // namespace vast::detail

#endif
