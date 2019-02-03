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

#include <atomic>
#include <chrono>

#include <caf/fwd.hpp>

namespace vast::system {

/// Monitors the application for UNIX signals.
class signal_monitor {
public:
  /// Stops the signal monitor loop when set to `true`.
  static std::atomic<bool> stop;

  /// Run the signal monitor loop.
  /// @warning it's not safe to run two or more signal monitor loops.
  /// @param monitoring_interval The number of milliseconds to wait between
  ///        checking whether a signal occurred.
  /// @param receiver The actor receiving the signals.
  static void run(std::chrono::milliseconds monitoring_interval,
                  caf::actor receiver);
};

} // namespace vast::system
