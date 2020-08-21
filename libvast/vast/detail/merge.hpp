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

/// Merges the contents of two containers into the first.
template <class Container>
void merge(Container& xs, Container&& ys) {
  auto begin = std::make_move_iterator(ys.begin());
  auto end = std::make_move_iterator(ys.end());
  xs.insert(xs.end(), begin, end);
}

} // namespace vast::detail
