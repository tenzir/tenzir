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

#include "vast/system/indexer_stage_driver.hpp"

#include "vast/logger.hpp"
#include "vast/meta_index.hpp"
#include "vast/system/index.hpp"
#include "vast/system/partition.hpp"
#include "vast/table_slice.hpp"

#include <caf/downstream.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/outbound_path.hpp>
#include <caf/scheduled_actor.hpp>
#include <caf/stream_manager.hpp>

#include <cstddef>

namespace vast::system {

indexer_stage_driver::indexer_stage_driver(downstream_manager_type& dm,
                                           self_pointer self)
  : super(dm), self_(self) {
  VAST_ASSERT(self_ != nullptr);
}

indexer_stage_driver::~indexer_stage_driver() noexcept {
  // nop
}

void indexer_stage_driver::process(downstream_type&, batch_type& slices) {
  VAST_TRACE(CAF_ARG(slices));
  VAST_ASSERT(!slices.empty());
  auto& st = self_->state;
  for (auto& slice : slices) {
    auto& layout = slice->layout();
    st.stats.layouts[layout.name()].count += slice->rows();
    auto part = st.get_or_add_partition(slice);
    st.meta_idx.add(part->id(), *slice);
    part->add(std::move(slice));
  }
}

} // namespace vast::system
