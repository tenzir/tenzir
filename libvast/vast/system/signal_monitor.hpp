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
#include <thread>

#include <caf/actor.hpp>
#include <caf/actor_system.hpp>
#include <caf/detail/scope_guard.hpp>
#include <caf/logger.hpp>

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

  /// Run the singal monitor loop in thread `t`, stopping it at scope exit with
  /// the returned scope guard.
  static auto run_guarded(std::thread& t, caf::actor_system& sys,
                          std::chrono::milliseconds monitoring_interval,
                          caf::actor receiver) {
    t = std::thread{[&] {
      CAF_SET_LOGGER_SYS(&sys);
      run(monitoring_interval, std::move(receiver));
    }};
    return caf::detail::make_scope_guard([&] {
      signal_monitor::stop = true;
      t.join();
    });
  }
};

} // namespace vast::system
