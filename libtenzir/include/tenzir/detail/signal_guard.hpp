//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/notify.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/ref.hpp"

#include <fmt/format.h>
#include <folly/CancellationToken.h>

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <fcntl.h>
#include <poll.h>
#include <thread>
#include <unistd.h>

namespace tenzir::detail {

/// Intercepts SIGINT/SIGTERM for graceful two-phase shutdown.
///
/// Uses the self-pipe trick: signal handlers write to a pipe, and a
/// dedicated thread polls it. On first signal the `graceful_stop` Notify
/// is fired so the pipeline can drain. On a second signal (or after the
/// grace period) the `cancel_source` is triggered to force-cancel.
///
/// The destructor joins the thread and restores the original handlers.
class SignalGuard {
public:
  SignalGuard(Notify& graceful_stop, folly::CancellationSource& cancel_source,
              std::chrono::milliseconds grace)
    : graceful_stop_{graceful_stop}, cancel_source_{cancel_source} {
    (void)::pipe(signal_pipe_.data());
    (void)::fcntl(signal_pipe_[1], F_SETFL, O_NONBLOCK);
    (void)::pipe(wake_pipe_.data());
    signal_write_fd_.store(signal_pipe_[1], std::memory_order_relaxed);
    struct sigaction sa = {};
    sa.sa_handler = [](int signum) { // NOLINT
      auto fd = signal_write_fd_.load(std::memory_order_relaxed);
      if (fd >= 0) {
        auto byte = static_cast<char>(signum);
        (void)::write(fd, &byte, 1);
      }
    };
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    sigaction(SIGINT, &sa, &old_sigint_);
    sigaction(SIGTERM, &sa, &old_sigterm_);
    thread_
      = std::thread(&SignalGuard::run, this, static_cast<int>(grace.count()));
  }

  ~SignalGuard() {
    (void)::write(wake_pipe_[1], "x", 1);
    thread_.join();
    signal_write_fd_.store(-1, std::memory_order_relaxed);
    sigaction(SIGINT, &old_sigint_, nullptr);
    sigaction(SIGTERM, &old_sigterm_, nullptr);
    for (auto fd :
         {signal_pipe_[0], signal_pipe_[1], wake_pipe_[0], wake_pipe_[1]}) {
      ::close(fd);
    }
  }

  SignalGuard(const SignalGuard&) = delete;
  auto operator=(const SignalGuard&) -> SignalGuard& = delete;
  SignalGuard(SignalGuard&&) = delete;
  auto operator=(SignalGuard&&) -> SignalGuard& = delete;

private:
  /// Waits for signals on the signal pipe, or for normal exit on the wake
  /// pipe. Called on the dedicated thread.
  auto run(int grace_ms) -> void {
    auto pfds = std::array<pollfd, 2>{{
      {signal_pipe_[0], POLLIN, 0},
      {wake_pipe_[0], POLLIN, 0},
    }};
    auto rc = ::poll(pfds.data(), pfds.size(), -1);
    if (rc <= 0 or (pfds[1].revents & POLLIN)) {
      return;
    }
    // First signal: drain the byte and request graceful stop.
    auto byte = char{};
    (void)::read(signal_pipe_[0], &byte, 1);
    TENZIR_DEBUG("received signal {}, initiating graceful shutdown",
                 static_cast<int>(byte));
    fmt::print(stderr, "\rinitiating graceful shutdown... "
                       "(repeat to terminate immediately)\n");
    graceful_stop_->notify_one();
    // Wait for second signal, grace-period timeout, or normal completion.
    pfds[0].revents = 0;
    pfds[1].revents = 0;
    rc = ::poll(pfds.data(), pfds.size(), grace_ms);
    if (rc > 0 and (pfds[1].revents & POLLIN)) {
      return; // Pipeline finished on its own.
    }
    if (rc > 0 and (pfds[0].revents & POLLIN)) {
      TENZIR_DEBUG("received second signal, force-cancelling pipeline");
    } else {
      TENZIR_DEBUG("grace period expired, force-cancelling pipeline");
    }
    cancel_source_->requestCancellation();
  }

  // NOLINTBEGIN
  static inline std::atomic<int> signal_write_fd_{-1};
  // NOLINTEND
  Ref<Notify> graceful_stop_;
  Ref<folly::CancellationSource> cancel_source_;
  std::array<int, 2> signal_pipe_{-1, -1};
  std::array<int, 2> wake_pipe_{-1, -1};
  struct sigaction old_sigint_ = {};
  struct sigaction old_sigterm_ = {};
  std::thread thread_;
};

} // namespace tenzir::detail
