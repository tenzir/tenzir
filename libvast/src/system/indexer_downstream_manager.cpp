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

#include "vast/system/indexer_downstream_manager.hpp"

#include "vast/detail/assert.hpp"
#include "vast/logger.hpp"
#include "vast/system/index_common.hpp"
#include "vast/system/partition.hpp"
#include "vast/table_slice.hpp"

#include <caf/outbound_path.hpp>

#include <limits>

namespace vast::system {

size_t indexer_downstream_manager::buffered() const noexcept {
  // We sum up the buffers for all partiitons.
  size_t result = 0;
  for (auto p : partitions)
    result += buffered(*p);
  return result;
}

size_t indexer_downstream_manager::buffered(partition& p) const noexcept {
  // We have a central buffer, but also an additional buffer at each path. We
  // return the maximum size to reflect the current worst case.
  size_t max_path_buf = 0;
  for (auto& ip : p.indexers_) {
    max_path_buf = std::max(max_path_buf, ip.second.buf.size());
  }
  return p.inbound.size() + max_path_buf;
}

/// Returns the number of buffered elements for this specific slot, ignoring
/// the central buffer.
size_t indexer_downstream_manager::buffered(caf::stream_slot slot) const
  noexcept {
  for (auto p : partitions) {
    for (auto& ip : p->indexers_) {
      if (ip.second.slot == slot)
        return ip.second.buf.size();
    }
  }
  return 0u;
}

int32_t indexer_downstream_manager::max_capacity() const noexcept {
  // The maximum capacity is limited by the slowest downstream path.
  auto result = std::numeric_limits<int32_t>::max();
  for (auto& kvp : this->paths_) {
    auto mc = kvp.second->max_capacity;
    // max_capacity is 0 if and only if we didn't receive an ack_batch yet.
    if (mc > 0)
      result = std::min(result, mc);
  }
  return result;
}

// std::pair<std::unordered_set<partition>::iterator, bool>
void indexer_downstream_manager::register_partition(partition* p) {
  partitions.insert(p);
}

bool indexer_downstream_manager::unregister(partition* p) {
  auto it = partitions.find(p);
  if (it == partitions.end())
    return false;
  if (buffered(**it) == 0u)
    cleanup_partition(it);
  else
    pending_partitions.insert(p);
  return true;
}

void indexer_downstream_manager::emit_batches() {
  VAST_TRACE(VAST_ARG_2("buffered", this->buffered())
             << VAST_ARG_2("paths", this->paths_.size()));
  emit_batches_impl(false);
}

void indexer_downstream_manager::force_emit_batches() {
  VAST_TRACE(VAST_ARG_2("buffered", this->buffered())
             << VAST_ARG_2("paths", this->paths_.size()));
  emit_batches_impl(true);
}

indexer_downstream_manager::buffer_type& indexer_downstream_manager::buf() {
  return buf_;
}

// TODO(ch9680): We always erase the paths to an entire partition.
#if 0
  void indexer_downstream_manager::about_to_erase(caf::outbound_path* ptr, bool silent,
                      caf::error* reason) {
    CAF_ASSERT(ptr != nullptr);
    CAF_LOG_TRACE(CAF_ARG2("slot", ptr->slots.sender)
                  << CAF_ARG(silent) << CAF_ARG(reason));
    auto& pths = this->paths_.container();
    auto p
      = std::find_if(pths.rbegin(), pths.rend(), [&](const auto& slt_conn) {
          return slt_conn.second.get() == ptr;
        });
    VAST_ASSERT(p != pths.rend());
    auto offset = std::distance(pths.begin(), p.base());
    std::cerr << "erasing buf at " << offset << std::endl;
    state_map_.erase(ptr->slots.sender);
    selector_.erase(offset);
    super::about_to_erase(ptr, silent, reason);
  }
#endif

void indexer_downstream_manager::cleanup_partition(set_type::iterator& it) {
  (*it)->finalize();
  it = partitions.erase(it);
}

static size_t chunk_size(const partition& p) {
  auto chunk_size = std::numeric_limits<size_t>::max();
  for (auto& x : p.indexers_) {
    auto& outbound = x.second.outbound;
    if (!outbound->closing) {
      auto credit = static_cast<size_t>(outbound->open_credit);
      auto cache_size = x.second.buf.size();
      chunk_size
        = std::min(chunk_size, credit > cache_size ? credit - cache_size : 0u);
    }
  }
  return chunk_size;
}

void indexer_downstream_manager::try_remove_partition(set_type::iterator& it) {
  auto pit = pending_partitions.find(*it);
  if (pit != pending_partitions.end()) {
    if (buffered(**it) == 0u) {
      cleanup_partition(it);
      pending_partitions.erase(pit);
      return;
    }
  }
  ++it;
}

void indexer_downstream_manager::emit_batches_impl(bool force_underfull) {
  if (this->paths_.empty())
    return;
  for (auto it = partitions.begin(); it != partitions.end();) {
    auto pptr = *it;
    // Calculate the chunk size, i.e., how many more items we can put to our
    // caches at the most.
    size_t chunk = chunk_size(*pptr);
    if (chunk == std::numeric_limits<size_t>::max()) {
      // All paths are closing, simply try forcing out more data and return.
      for (auto& x : pptr->indexers_) {
        // Always force batches on closing paths.
        x.second.outbound->emit_batches(this->self(), x.second.buf, true);
      }
      continue;
    }
    auto& buf = pptr->inbound;
    chunk = std::min(chunk, buf.size());
    // If this partition has any inbound slices to handle:
    if (chunk != 0u) {
      // Move the chunk into the destined outbound queues.
      auto last = buf.begin() + chunk;
      for (auto i = buf.begin(); i < last; ++i) {
        auto& slice = *i;
        auto& layout = slice->layout();
        // Split the slice into co-owning columns.
        for (size_t i = 0; i < layout.fields.size(); ++i) {
          // Look up the destination INDEXER for the column.
          auto fqf = to_fully_qualified(layout.name(), layout.fields[i]);
          auto destination = pptr->indexers_.find(fqf);
          if (destination == pptr->indexers_.end()) {
            VAST_WARNING(this, "could not find the target indexer for",
                         fqf.name);
            continue;
          }
          // Place the column into the selected INDEXERs stream queue.
          VAST_ASSERT(!destination->second.outbound->closing);
          destination->second.buf.emplace_back(slice, i);
        }
      }
      buf.erase(buf.begin(), last);
    }
    // Let each indexer consume its inbound buffer.
    for (auto& x : pptr->indexers_) {
      // Always force batches on closing paths.
      x.second.outbound->emit_batches(this->self(), x.second.buf,
                                      force_underfull
                                        || x.second.outbound->closing);
    }
    try_remove_partition(it);
  }
}

} // namespace vast::system
