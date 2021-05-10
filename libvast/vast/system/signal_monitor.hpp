//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/actor.hpp>
#include <caf/actor_system.hpp>
#include <caf/detail/scope_guard.hpp>
#include <caf/logger.hpp>

#include <atomic>
#include <chrono>
#include <thread>

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

  /// Run the signal monitor loop in thread `t`, stopping it at scope exit with
  /// the returned scope guard.
  static auto run_guarded(std::thread& t,
                          [[maybe_unused]] caf::actor_system& sys,
                          std::chrono::milliseconds monitoring_interval,
                          caf::actor receiver) {
    t = std::thread{[&, monitoring_interval, receiver{std::move(receiver)}] {
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
