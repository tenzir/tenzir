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

#include <string>

#include <caf/fwd.hpp>

#include "vast/fwd.hpp"
#include "vast/ids.hpp"
#include "vast/system/query_processor.hpp"

namespace vast::system {

/// Periodically queries the INDEX with a configurable expression and erases
/// all hits from the ARCHIVE.
class eraser_state : public query_processor {
public:
  // -- member types -----------------------------------------------------------

  using super = query_processor;

  // -- constants --------------------------------------------------------------

  static inline constexpr const char* name = "eraser";

  // -- constructors, destructors, and assignment operators --------------------

  eraser_state(caf::event_based_actor* self);

  void init(caf::timespan interval, std::string query, caf::actor index_hdl,
            caf::actor archive_hdl);

protected:
  // -- implementation hooks ---------------------------------------------------

  void transition_to(state_name x) override;

  void process_hits(const ids& hits) override;

  void process_end_of_hits() override;

private:
  // -- member variables -------------------------------------------------------

  /// Configures the time between two query executions.
  caf::timespan interval_;

  /// Selects outdated events. Note that we get the query as string on purpose.
  /// Taking an ::expression here instead would fix any query such as
  /// `#time < 1 week ago` to the time of its parsing and not update properly.
  std::string query_;

  /// Points to the ARCHIVE that needs periodic pruning.
  caf::actor archive_;

  /// Collects hits until all deltas arrived.
  ids hits_;
};

/// Periodically queries `index_hdl` and erases all hits from `archive_hdl`.
/// @param interval The time between two query executions.
/// @param query The periodic query for selecting outdated events. Note that
///              we get the query as string on purpose. Taking an ::expression
///              here instead would fix any query such as `#time < 1 week ago`
///              to the time of its parsing and not update properly.
/// @param index_hdl A handle to the INDEX under investigation.
/// @param archive_hdl A handle to the ARCHIVE that needs periodic pruning.
caf::behavior eraser(caf::stateful_actor<eraser_state>* self,
                     caf::timespan interval, std::string query,
                     caf::actor index_hdl, caf::actor archive_hdl);

} // namespace vast::system
