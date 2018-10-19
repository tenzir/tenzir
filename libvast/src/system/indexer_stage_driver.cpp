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
#include <caf/scheduled_actor.hpp>
#include <caf/stream_manager.hpp>

#include "vast/logger.hpp"
#include "vast/meta_index.hpp"
#include "vast/system/partition.hpp"
#include "vast/table_slice.hpp"

namespace vast::system {

indexer_stage_driver::indexer_stage_driver(downstream_manager_type& dm,
                                           meta_index& meta_idx,
                                           partition_factory fac,
                                           size_t max_partition_size)
  : super(dm),
    meta_index_(meta_idx),
    remaining_in_partition_(max_partition_size),
    partition_(fac()),
    factory_(std::move(fac)),
    max_partition_size_(max_partition_size) {
  VAST_ASSERT(max_partition_size_ > 0);
}

indexer_stage_driver::~indexer_stage_driver() noexcept {
  // nop
}

void indexer_stage_driver::process(downstream_type& out, batch_type& slices) {
  VAST_TRACE(CAF_ARG(slices));
  VAST_ASSERT(!slices.empty());
  VAST_ASSERT(partition_ != nullptr);
  for (auto& slice : slices) {
    // Update meta index.
    meta_index_.add(partition_->id(), *slice);
    // Start new INDEXER actors when needed and add it to the stream.
    auto& layout = slice->layout();
    if (auto [hdl, added] = partition_->manager().get_or_add(layout); added) {
      auto slot = out_.parent()->add_unchecked_outbound_path<output_type>(hdl);
      VAST_DEBUG(this, "spawned new INDEXER at slot", slot);
      out_.set_filter(slot, layout);
    }
    // Ship event to the INDEXER actors.
    auto slice_size = slice->rows();
    out.push(std::move(slice));
    // Reset the manager and all outbound paths when finalizing a partition.
    if (remaining_in_partition_ <= slice_size) {
      VAST_DEBUG(this, "closes slots on full partition",
                 out_.open_path_slots());
      VAST_ASSERT(out_.buf().size() != 0);
      out_.fan_out_flush();
      VAST_ASSERT(out_.buf().size() == 0);
      out_.force_emit_batches();
      out_.close();
      partition_ = factory_();
      VAST_ASSERT(partition_->layouts().empty());
      remaining_in_partition_ = max_partition_size_;
    } else {
      remaining_in_partition_ -= slice_size;
    }
  }
}

} // namespace vast::system
