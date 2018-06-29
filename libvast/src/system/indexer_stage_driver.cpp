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

namespace vast::system {

indexer_stage_driver::indexer_stage_driver(downstream_manager_type& dm,
                                           meta_index& pindex,
                                           partition_factory fac,
                                           size_t max_partition_size)
  : super(dm),
    pindex_(pindex),
    remaining_in_partition_(max_partition_size),
    partition_(fac()),
    factory_(std::move(fac)),
    max_partition_size_(max_partition_size) {
  VAST_ASSERT(max_partition_size_ > 0);
}

indexer_stage_driver::~indexer_stage_driver() noexcept {
  // nop
}

void indexer_stage_driver::process(downstream_type& out, batch_type& batch) {
  VAST_TRACE(CAF_ARG(batch));
  VAST_ASSERT(!batch.empty());
  VAST_ASSERT(partition_ != nullptr);
  auto i = batch.begin();
  auto e = batch.end();
  auto n = std::min(remaining_in_partition_,
                    static_cast<size_t>(std::distance(i, e)));
  do {
    // Consume chunk of the batch.
    consume(out, i, i + n);
    // Advance iterator.
    i = i + n;
    // Reset the manager and all outbound paths when finalizing a partition.
    remaining_in_partition_ -= n;
    if (remaining_in_partition_ == 0) {
      VAST_DEBUG("partition full, close slots", out_.open_path_slots());
      VAST_ASSERT(out_.buf().size() != 0);
      out_.fan_out_flush();
      VAST_ASSERT(out_.buf().size() == 0);
      out_.force_emit_batches();
      out_.close();
      partition_ = factory_();
      VAST_ASSERT(partition_->types().empty());
      remaining_in_partition_ = max_partition_size_;
      // Compute size of the next chunk.
      n = std::min(remaining_in_partition_,
                   static_cast<size_t>(std::distance(i, e)));
    }
  } while (i != e);
}

void indexer_stage_driver::consume(downstream_type& out, batch_iterator first,
                                   batch_iterator last) {
  pindex_.add(partition_->id(), first, last);
  std::for_each(first, last, [&](event& x) {
    // Start new INDEXER actors when needed and add it to the stream.
    if (auto [hdl, added] = partition_->manager().get_or_add(x.type()); added) {
      auto slot = out_.parent()->add_unchecked_outbound_path<output_type>(hdl);
      VAST_DEBUG("spawned new INDEXER at slot", slot);
      out_.set_filter(slot, x.type());
    }
    // Ship event to the INDEXER actors.
    out.push(std::move(x));
  });
}

} // namespace vast::system
