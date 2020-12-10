// Experimental actor to monitor disk space usage
#pragma once

#include "vast/fwd.hpp"
#include "vast/system/status_client_actor.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

/// The DISK MONITOR actor interface.
using disk_monitor_actor = caf::typed_actor<
  /// FIXME: docs
  caf::reacts_to<atom::ping>,
  /// FIXME: docs
  caf::reacts_to<atom::erase>>
  // Conform to the protocol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>;

} // namespace vast::system
