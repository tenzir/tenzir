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
#include <condition_variable>
#include <mutex>
#include <thread>

namespace vast::system {

/// Monitors the application for UNIX signals.
class signal_monitor {
public:
  /// Stops the signal monitor loop when set to `true`.
  static std::atomic<bool> stop;
  static std::condition_variable cv;
  static std::mutex m;

  /// Run the signal monitor loop.
  /// @warning it's not safe to run two or more signal monitor loops.
  /// @param receiver The actor receiving the signals.
  static void run(caf::actor receiver);

  /// Run the signal monitor loop in thread `t`, stopping it at scope exit with
  /// the returned scope guard.
  static auto
  run_guarded(std::thread& t, [[maybe_unused]] caf::actor_system& sys,
              caf::actor receiver) {
    t = std::thread{[&, receiver{std::move(receiver)}] {
      CAF_SET_LOGGER_SYS(&sys);
      run(std::move(receiver));
    }};
    return caf::detail::make_scope_guard([&] {
      signal_monitor::stop = true;
      signal_monitor::cv.notify_one();
      t.join();
    });
  }
};

} // namespace vast::system
