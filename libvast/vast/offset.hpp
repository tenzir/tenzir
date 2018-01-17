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

#ifndef VAST_OFFSET_HPP
#define VAST_OFFSET_HPP

#include <cstddef>

#include "vast/detail/stack_vector.hpp"

namespace vast {

/// A sequence of indexes to recursively access a type or value.
struct offset : detail::stack_vector<size_t, 64> {
  using super = detail::stack_vector<size_t, 64>;
  using super::super;
};

} // namespace vast

#endif
