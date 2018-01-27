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

#ifndef VAST_SYSTEM_SIGNAL_MONITOR_HPP
#define VAST_SYSTEM_SIGNAL_MONITOR_HPP

#include <chrono>

#include <caf/typed_actor.hpp>

#include "vast/system/atoms.hpp"

namespace vast::system {

struct signal_monitor_state {
  static inline const char* name = "signal-monitor";
};

using signal_monitor_type = caf::typed_actor<caf::reacts_to<run_atom>>;

/// Monitors the application for UNIX signals.
/// @note There must not exist more than one instance of this actor per
///       process.
/// @param self The actor handle.
/// @param monitoring_interval The number of milliseconds to wait between
///        checking whether a signal occurred.
/// @param receiver The actor receiving the signals.
signal_monitor_type::behavior_type
signal_monitor(signal_monitor_type::stateful_pointer<signal_monitor_state> self,
               std::chrono::milliseconds monitoring_interval,
               caf::actor receiver);

} // namespace vast::system

#endif
