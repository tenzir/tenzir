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

#include "vast/meta_index.hpp"

#include "vast/event.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/logger.hpp"
#include "vast/table_index.hpp"
#include "vast/table_slice.hpp"

namespace vast {

// -- member types -------------------------------------------------------------

meta_index::interval::interval()
  : from(timestamp::max()),
    to(timestamp::min()) {
  // nop
}

meta_index::interval::interval(timestamp first, timestamp last)
  : from(first),
    to(last) {
  // nop
}

// -- properties ---------------------------------------------------------------

caf::optional<meta_index::partition_synopsis> meta_index::
operator[](const uuid& partition) const {
  auto i = partitions_.find(partition);
  if (i != partitions_.end())
    return i->second;
  return caf::none;
}

void meta_index::add(const uuid& partition,
                     const const_table_slice_ptr& slice) {
  auto& rng = partitions_[partition].range;
  // The first column always contains the timestamp.
  for (size_t row = 0; row < slice->rows(); ++row) {
    auto element = slice->at(row, 0);
    if (!element) {
      VAST_ERROR("cannot access element 0 in row", row);
      continue;
    }
    if (auto tsv = caf::get_if<view<timestamp>>(&(*element))) {
      auto ts = materialize(*tsv);
      rng.from = std::min(rng.from, ts);
      rng.to = std::max(rng.to, ts);
    }
  }
}

std::vector<uuid> meta_index::lookup(const expression& expr) const {
  std::vector<uuid> result;
  for (auto& x : partitions_)
    if (caf::visit(time_restrictor{x.second.range.from, x.second.range.to},
                   expr))
      result.push_back(x.first);
  return result;
}

bool operator==(const meta_index::interval& x, const meta_index::interval& y) {
  return x.from == y.from && x.to == y.to;
}

} // namespace vast
