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

/// Shared state for a fused channel.
template <class T>
struct FusedState {
  Option<T> item;
  folly::fibers::Semaphore ack{0};
  folly::fibers::Semaphore item_ready{0};
};

/// The sender half of a fused channel.
template <class T>
class FusedSender {
public:
  explicit FusedSender(Arc<FusedState<T>> state) : state_{std::move(state)} {
  }

  ~FusedSender() {
    if (state_.not_moved_from()) {
      // Make sure that `receive()` wakes up to return `None`.
      state_->item_ready.signal();
    }
  }

  FusedSender(FusedSender&&) = default;
  auto operator=(FusedSender&&) -> FusedSender& = default;
  FusedSender(FusedSender const&) = delete;
  auto operator=(FusedSender const&) -> FusedSender& = delete;

  /// Sends a message, waiting for the receiver to request the one after that.
  ///
  /// If cancelled, the in-flight item is still delivered to the receiver. A
  /// subsequent call to `send()` will first wait for the receiver to
  /// acknowledge the in-flight item before proceeding.
  auto send(T x) -> Task<void> {
    if (awaiting_ack_) {
      // If we were cancelled while waiting for an ack, we continue waiting
      // here since the item is not consumed yet.
      co_await state_->ack.co_wait();
      awaiting_ack_ = false;
    }
    TENZIR_ASSERT(not state_->item);
    state_->item = std::move(x);
    state_->item_ready.signal();
    awaiting_ack_ = true;
    co_await state_->ack.co_wait();
    awaiting_ack_ = false;
  }

private:
  Arc<FusedState<T>> state_;
  bool awaiting_ack_ = false;
};

/// The receiver half of a fused channel.
template <class T>
class FusedReceiver {
public:
  explicit FusedReceiver(Arc<FusedState<T>> state) : state_{std::move(state)} {
  }

  ~FusedReceiver() {
    if (state_.not_moved_from()) {
      // Unblock any sender that may be waiting for an ack.
      state_->ack.signal();
    }
  }

  FusedReceiver(FusedReceiver&&) = default;
  FusedReceiver& operator=(FusedReceiver&&) = default;
  FusedReceiver(FusedReceiver const&) = delete;
  FusedReceiver& operator=(FusedReceiver const&) = delete;

  /// Receives the next message, or `None` if the sender was destroyed.
  ///
  /// Acknowledges the previous message upon entry, which unblocks the sender.
  auto recv() -> Task<Option<T>> {
    if (ack_next_) {
      state_->ack.signal();
      ack_next_ = false;
    }
    co_await state_->item_ready.co_wait();
    if (state_->item.is_none()) {
      // Ensure that subsequent `receive()` calls return immediately.
      state_->item_ready.signal();
      co_return None{};
    }
    auto result = std::move(*state_->item);
    state_->item = None{};
    // Make sure we send the ack the next time we receive.
    ack_next_ = true;
    co_return result;
  }

private:
  Arc<FusedState<T>> state_;
  bool ack_next_ = false;
};

template <class T>
class FusedPush final : public Push<T> {
public:
  explicit FusedPush(FusedSender<T> sender) : sender_{std::move(sender)} {
  }

  auto operator()(T x) -> Task<void> override {
    return sender_.send(std::move(x));
  }

private:
  FusedSender<T> sender_;
};

template <class T>
class FusedPull final : public Pull<T> {
public:
  explicit FusedPull(FusedReceiver<T> receiver)
    : receiver_{std::move(receiver)} {
  }

  auto operator()() -> Task<Option<T>> override {
    return receiver_.recv();
  }

private:
  FusedReceiver<T> receiver_;
};

template <class T>
struct FusedSenderReceiver {
  FusedSender<T> sender;
  FusedReceiver<T> receiver;

  explicit(false) operator PushPull<T>() && {
    return {std::move(sender), std::move(receiver)};
  }
};

/// Returns a sender-receiver pair for a new fused channel.
///
/// A fused channel blocks the sender until the receiver waits for the potential
/// subsequent message. This is more strict than a rendezvous channel, which
/// would just block until the message was picked up. Instead, we block longer,
/// specifically until the receiver would be ready to pick up the next item.
///
/// When the receiver alternates between receiving and processing messages, this
/// makes it so that the sender is effectively informed when the processing is
/// complete, as this is the next time when the receiver requests the next item.
///
/// This property propagates over chains. If `A -> B -> C` is a chain that
/// communicates over fused channels, then A will block until B receives again,
/// but since B will block until C receives again, A effectively waits for C.
template <class T>
auto fused_channel() -> FusedSenderReceiver<T> {
  auto state = Arc<FusedState<T>>{};
  return {
    FusedSender<T>{state},
    FusedReceiver<T>{std::move(state)},
  };
}

} // namespace tenzir
