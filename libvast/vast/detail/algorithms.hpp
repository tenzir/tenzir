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

#include <algorithm>
#include <vector>

namespace vast::detail {

template <class Collection>
auto unique_values(const Collection& xs) {
  std::vector<typename Collection::mapped_type> result;
  result.reserve(xs.size());
  for (auto& x : xs)
    result.emplace_back(x.second);
  std::sort(result.begin(), result.end());
  auto e = std::unique(result.begin(), result.end());
  if (e != result.end())
    result.erase(e, result.end());
  return result;
}

} // namespace vast::detail

