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

namespace vast::system {

indexer_stage_driver::indexer_stage_driver(downstream_manager_type& dm,
                                           partition_factory fac,
                                           size_t max_partition_size)
  : super(dm),
    remaining_in_partition_(max_partition_size),
    partition_(fac()),
    factory_(std::move(fac)),
    max_partition_size_(max_partition_size) {
  VAST_ASSERT(max_partition_size_ > 0);
}

indexer_stage_driver::~indexer_stage_driver() noexcept {
  // nop
}

void indexer_stage_driver::process(caf::downstream<output_type>& out,
                                   std::vector<input_type>& batch) {
  VAST_TRACE(CAF_ARG(batch));
  // Iterate batch to start new INDEXER actors when needed.
  for (auto& x : batch) {
    if (auto [hdl, added] = partition_->manager().get_or_add(x.type()); added) {
      auto slot = out_.parent()->add_unchecked_outbound_path<output_type>(hdl);
      VAST_DEBUG("spawned new INDEXER at slot", slot);
      out_.set_filter(slot, x.type());
    }
    // Dispatch event.
    out.push(std::move(x));
    // Reset the manager and all outbound paths when finalizing a partition.
    if (--remaining_in_partition_ == 0) {
      VAST_DEBUG("partition full, close slots", out_.open_path_slots());
      VAST_ASSERT(out_.buf().size() != 0);
      out_.fan_out_flush();
      VAST_ASSERT(out_.buf().size() == 0);
      out_.force_emit_batches();
      out_.close();
      partition_ = factory_();
      VAST_ASSERT(partition_->types().empty());
      remaining_in_partition_ = max_partition_size_;
    }
  }
}

} // namespace vast::system
