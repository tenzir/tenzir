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

#include <caf/broadcast_downstream_manager.hpp>
#include <caf/stream_stage_driver.hpp>

#include "vast/type.hpp"
#include "vast/event.hpp"

#include "vast/system/indexer_manager.hpp"

namespace vast::system {

/// @relates indexer_stage_driver
/// Filter type for dispatching events to INDEXER actors.
using indexer_stage_filter = type;

/// @relates indexer_stage_driver
/// Selects an INDEXER actor based on its filter.
struct indexer_stage_selector {
  inline bool operator()(const indexer_stage_filter& f, const event& x) const {
    return f == x.type();
  }
};

/// @relates indexer_stage_driver
/// A downstream manager type for dispatching data to INDEXER actors.
using indexer_downstream_manager
  = caf::broadcast_downstream_manager<event, indexer_stage_filter,
                                      indexer_stage_selector>;

/// A stream stage for dispatching events to INDEXER actors. One set of INDEXER
/// actors is used per partition.
class indexer_stage_driver
  : public caf::stream_stage_driver<event, indexer_downstream_manager> {
public:
  using super = caf::stream_stage_driver<event, indexer_downstream_manager>;

  using index_manager_factory = std::function<indexer_manager_ptr()>;

  indexer_stage_driver(downstream_manager_type& dm, index_manager_factory fac,
                       size_t max_partition_size);

  ~indexer_stage_driver() noexcept override;

  void process(caf::downstream<output_type>& out,
               std::vector<input_type>& batch) override;

private:
  /// Stores how many events remain in the current partition.
  size_t remaining_in_partition_;
  /// Stores the INDEXER actors for the current partition.
  indexer_manager_ptr im_;
  /// Generates INDEXER actors for the manager.
  index_manager_factory factory_;
  /// Stores how many events form one partition.
  size_t max_partition_size_;
};

} // namespace vast::system
