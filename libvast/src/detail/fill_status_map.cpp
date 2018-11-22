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

#include "vast/detail/fill_status_map.hpp"

#include <caf/config_value.hpp>
#include <caf/downstream_manager.hpp>
#include <caf/inbound_path.hpp>
#include <caf/outbound_path.hpp>
#include <caf/stream_manager.hpp>

namespace vast::detail {

void fill_status_map(caf::dictionary<caf::config_value>& xs,
                     const caf::stream_manager& mgr) {
  // Manager status.
  xs.emplace("idle", mgr.idle());
  xs.emplace("congested", mgr.idle());
  // Downstream status.
  auto& downstream = put_dictionary(xs, "downstream");
  auto& out = mgr.out();
  downstream.emplace("buffered", out.buffered());
  downstream.emplace("max-capacity", out.max_capacity());
  downstream.emplace("paths", out.num_paths());
  downstream.emplace("stalled", out.stalled());
  downstream.emplace("clean", out.stalled());
  // Upstream status.
  auto& upstream = put_dictionary(xs, "upstream-paths");
  auto& ipaths = mgr.inbound_paths();
  for (auto ipath : ipaths) {
    auto name = "slot-" + std::to_string(ipath->slots.receiver);
    auto& slot = put_dictionary(upstream, name);
    slot.emplace("priority", to_string(ipath->prio));
    slot.emplace("assigned-credit", ipath->assigned_credit);
    slot.emplace("last-acked-batch-id", ipath->last_acked_batch_id);
  }
}

} // namespace vast::detail
