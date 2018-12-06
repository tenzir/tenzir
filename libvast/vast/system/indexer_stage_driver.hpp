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

#include "vast/fwd.hpp"
#include "vast/system/fwd.hpp"

namespace vast::system {

/// @relates indexer_stage_driver
/// Filter type for dispatching slices to INDEXER actors.
using indexer_stage_filter = type;

/// @relates indexer_stage_driver
/// Selects an INDEXER actor based on its filter.
struct indexer_stage_selector {
  bool operator()(const indexer_stage_filter& f,
                  const table_slice_ptr& x) const;
};

/// @relates indexer_stage_driver
/// A downstream manager type for dispatching data to INDEXER actors.
using indexer_downstream_manager
  = caf::broadcast_downstream_manager<table_slice_ptr,
                                      indexer_stage_filter,
                                      indexer_stage_selector>;

/// A stream stage for dispatching slices to INDEXER actors. One set of INDEXER
/// actors is used per partition.
class indexer_stage_driver
  : public caf::stream_stage_driver<table_slice_ptr,
                                    indexer_downstream_manager> {
public:
  // -- member types -----------------------------------------------------------

  using super = caf::stream_stage_driver<table_slice_ptr,
                                         indexer_downstream_manager>;

  using batch_type = std::vector<input_type>;

  using batch_iterator = batch_type::iterator;

  using downstream_type = caf::downstream<output_type>;

  // -- constructors, destructors, and assignment operators --------------------

  /// @pre `state != nullptr`
  indexer_stage_driver(downstream_manager_type& dm, index_state* state);

  ~indexer_stage_driver() noexcept override;

  // -- interface implementation -----------------------------------------------

  void process(downstream_type& out, batch_type& slices) override;

private:
  // -- member variables -------------------------------------------------------

  /// State of the INDEX actor that owns this stage.
  index_state* state_;
};

} // namespace vast::system
