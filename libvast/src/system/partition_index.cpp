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

#include "vast/system/partition_index.hpp"

#include "vast/event.hpp"
#include "vast/expression_visitors.hpp"

namespace vast::system {

caf::optional<partition_index::partition_synopsis> partition_index::
operator[](const uuid& partition) const {
  auto i = partitions_.find(partition);
  if (i != partitions_.end())
    return i->second;
  return caf::none;
}

void partition_index::add_one(interval& rng, const event& x) {
  auto t = x.timestamp();
  rng.from = std::min(rng.from, t);
  rng.to = std::max(rng.to, t);
}

std::vector<uuid> partition_index::lookup(const expression& expr) const {
  std::vector<uuid> result;
  for (auto& x : partitions_)
    if (visit(time_restrictor{x.second.range.from, x.second.range.to}, expr))
      result.push_back(x.first);
  return result;
}

bool operator==(const partition_index::interval& x,
                const partition_index::interval& y) {
  return x.from == y.from && x.to == y.to;
}

} // namespace vast::system
