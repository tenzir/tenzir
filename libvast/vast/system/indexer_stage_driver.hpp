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

#include "vast/fwd.hpp"
#include "vast/logger.hpp"
#include "vast/system/fwd.hpp"
#include "vast/system/indexer.hpp"
#include "vast/system/indexer_downstream_manager.hpp"

#include <caf/outbound_path.hpp>
#include <caf/stream_stage_driver.hpp>

#include <cstddef>
#include <unordered_map>
#include <unordered_set>

/// The indexer_stage_driver is responsible to receive a stream of table slices
/// and demultiplex it into streams of table slice columns that are relayed to
/// the to indexer actors in the target partition. This happens in 2 steps:
/// First, the table id offset is used to determine the target partition.
/// The partition is created if it did not exists beforehand. Second, the
/// layout is used to retrieve the set of downstream slots that the slice shall
/// be passed on to. If the table entry is missing, it is created by retrieving
/// the indexer actors from the target partition and associating them to their
/// matching fields from the layout.
///
///  Example for a partition containing 2 types foo and foo (updated) with the
///  layouts:
///
///  type foo = record {
///     a:          int,       // A
///     b:          string,    // B
///     c:          string,    // C
///     d:          address    // D
///  }
///  type foo = record {
///     a:          int,       // A
///     b:          domain,    // B'
///     c:          enum,      // C'
///     d:          address    // D
///  }
///
///  inbound stream
///        |                              table_slice{ foo }
///        v         table_slice{ foo }--    /   |   |    |
///                     |  ~|~~~~~\ ~~~~~\ ~~    |   |    |
///                     |/  \      ----   ----  /    |    |
///                     v    ---v      v      v      v    v
///   Indexers:         A       B      C      D      B'   C'
///

namespace vast::system {

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

  using self_pointer = caf::stateful_actor<index_state>*;

  // -- constructors, destructors, and assignment operators --------------------

  /// @pre `state != nullptr`
  indexer_stage_driver(downstream_manager_type& dm, self_pointer self);

  ~indexer_stage_driver() noexcept override;

  // -- interface implementation -----------------------------------------------

  void process(downstream_type&, batch_type& slices) override;

  // -- properties -------------------------------------------------------------

  self_pointer self() {
    return self_;
  }

private:
  // -- member variables -------------------------------------------------------

  /// State of the INDEX actor that owns this stage.
  self_pointer self_;
};

} // namespace vast::system
