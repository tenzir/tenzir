//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/arc.hpp"
#include "tenzir/async/push_pull.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/option.hpp"

#include <folly/fibers/Semaphore.h>

namespace tenzir {

/// A special SPSC channel that blocks the sender until the receiver waits for
/// the potential subsequent message.
///
/// This is more strict than a rendezvous channel, which would just block until
/// the message was picked up. Instead, we block longer, specifically until the
/// receiver would be ready to pick up the next item after that.
///
/// When the receiver alternates between receiving and processing messages, this
/// makes it so that the sender is effectively informed when the processing is
/// complete, as this is the next time when the receiver requests the next item.
///
/// This property propagates over chains. If `A -> B -> C` is a chain that
/// communicates over fused channels, then A will block until B receives again,
/// but since B will block until C receives again, A effectively waits for C.
template <class T>
class FusedChannel {
public:
  /// Sends a message, waiting for the receiver to request the one after that.
  ///
  /// When a call to this function is cancelled, or when the channel got closed,
  /// this must not be called again.
  auto send(T x) -> Task<void> {
    TENZIR_ASSERT(not item_);
    item_ = std::move(x);
    receive_ready_.signal();
    co_await receive_called_.co_wait();
  }

  auto receive() -> Task<Option<T>> {
    if (first_receive_) {
      first_receive_ = false;
    } else {
      // Only signal that receive got called the second time around. Otherwise,
      // the first `send()` would return immediately after the first `receive()`
      // is called, but we want it to block until the second call.
      receive_called_.signal();
    }
    co_await receive_ready_.co_wait();
    if (item_.is_none()) {
      // Ensure that subsequent `receive()` calls return immediately.
      receive_ready_.signal();
      co_return None{};
    }
    auto result = std::move(*item_);
    item_ = None{};
    co_return result;
  }

  /// This function may not be called concurrently to `send()`.
  void close_sender() {
    // Make sure that `receive()` wakes up to return `None`. We don't care about
    // `receive_called_` since we forbid concurrent `send()`.
    receive_ready_.signal();
  }

private:
  Option<T> item_;
  bool first_receive_ = true;
  folly::fibers::Semaphore receive_called_{0};
  folly::fibers::Semaphore receive_ready_{0};
};

template <class T>
class FusedPush final : public Push<T> {
public:
  explicit FusedPush(Arc<FusedChannel<T>> shared) : shared_{std::move(shared)} {
  }

  ~FusedPush() override {
    if (shared_.not_moved_from()) {
      shared_->close_sender();
    }
  }

  FusedPush(FusedPush&&) = default;
  FusedPush& operator=(FusedPush&&) = default;
  FusedPush(FusedPush const&) = delete;
  FusedPush& operator=(FusedPush const&) = delete;

  auto operator()(T x) -> Task<void> override {
    return shared_->send(std::move(x));
  }

private:
  Arc<FusedChannel<T>> shared_;
};

template <class T>
class FusedPull final : public Pull<T> {
public:
  explicit FusedPull(Arc<FusedChannel<T>> shared) : shared_{std::move(shared)} {
  }

  ~FusedPull() override = default;
  FusedPull(FusedPull&&) = default;
  auto operator=(FusedPull&&) -> FusedPull& = default;
  FusedPull(FusedPull const&) = delete;
  auto operator=(FusedPull const&) -> FusedPull& = delete;

  auto operator()() -> Task<Option<T>> override {
    return shared_->receive();
  }

private:
  Arc<FusedChannel<T>> shared_;
};

/// Returns a sender-receiver pair for a new `FusedChannel`.
template <class T>
auto fused_channel() -> PushPull<T> {
  auto shared = Arc<FusedChannel<T>>{};
  return {FusedPush<T>{shared}, FusedPull<T>{std::move(shared)}};
}

} // namespace tenzir
