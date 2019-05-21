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

#include <caf/downstream.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/scheduled_actor.hpp>
#include <caf/stream_manager.hpp>

#include "vast/logger.hpp"
#include "vast/meta_index.hpp"
#include "vast/system/index.hpp"
#include "vast/system/partition.hpp"
#include "vast/table_slice.hpp"

namespace vast::system {

bool indexer_stage_selector::operator()(const indexer_stage_filter& f,
                                        const table_slice_ptr& x) const {
  return f == x->layout();
}

indexer_stage_driver::indexer_stage_driver(downstream_manager_type& dm,
                                           self_pointer self)
  : super(dm), self_(self) {
  VAST_ASSERT(self_ != nullptr);
}

indexer_stage_driver::~indexer_stage_driver() noexcept {
  // nop
}

void indexer_stage_driver::process(downstream_type& out, batch_type& slices) {
  VAST_TRACE(CAF_ARG(slices));
  VAST_ASSERT(!slices.empty());
  auto& st = self_->state;
  for (auto& slice : slices) {
    // Spin up an initial partition if needed.
    if (st.active == nullptr)
      st.reset_active_partition();
    // Update meta index.
    st.meta_idx.add(st.active->id(), *slice);
    // Start new INDEXER actors when needed and add it to the stream.
    auto& layout = slice->layout();
    if (auto ti = st.active->get_or_add(layout)) {
      auto [meta_x, added] = *ti;
      if (added) {
        VAST_DEBUG(st.self, "added a new table_indexer for layout", layout);
        if (auto err = meta_x.init()) {
          VAST_ERROR(st.self, "failed to initialize table_indexer for layout",
                     layout, "-> all incoming logs get dropped!");
        } else {
          meta_x.spawn_indexers();
          for (auto& x : meta_x.indexers()) {
            // We'll have invalid handles at all fields with skip attribute.
            if (x) {
              auto slt = out_.parent()
                           ->add_unchecked_outbound_path<output_type>(x);
              VAST_DEBUG(st.self, "spawned new INDEXER at slot", slt);
              out_.set_filter(slt, layout);
              st.active_partition_indexers++;
            }
          }
        }
      }
      // Add all rows IDs to the meta indexer.
      meta_x.add(slice);
    }
    // Ship event to the INDEXER actors.
    auto slice_size = slice->rows();
    out.push(std::move(slice));
    // Reset the manager and all outbound paths when finalizing a partition.
    if (st.active->capacity() <= slice_size) {
      VAST_DEBUG(st.self, "closes slots on full partition",
                 out_.open_path_slots());
      VAST_ASSERT(out_.buf().size() != 0);
      out_.fan_out_flush();
      VAST_ASSERT(out_.buf().size() == 0);
      out_.force_emit_batches();
      out_.close();
      st.reset_active_partition();
      VAST_ASSERT(st.active->layouts().empty());
    } else {
      st.active->reduce_capacity(slice_size);
    }
  }
}

} // namespace vast::system
