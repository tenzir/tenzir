//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/arc.hpp"
#include "tenzir/async/semaphore.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/atomic.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/scope_guard.hpp"
#include "tenzir/option.hpp"
#include "tenzir/result.hpp"

#include <cstddef>
#include <cstdint>
#include <folly/concurrency/UnboundedQueue.h>
#include <utility>

namespace tenzir {

enum class TryRecvError {
  empty,
  closed,
};

/// Data shared between senders and receivers.
template <class T>
struct SenderReceiverShared {
  explicit SenderReceiverShared(size_t capacity)
    : slots{capacity},
      items{0} {
  }

  /// The actual concurrent queue storage.
  folly::UMPMCQueue<T, false> data;
  /// Number of remaining queue slots.
  Semaphore slots;
  /// Number of items ready to receive, plus a sticky wakeup after close.
  Semaphore items;
  /// Whether the channel is closed because the last sender got destroyed.
  Atomic<bool> closed{false};
  /// Number of receivers that are about to block on `items`.
  Atomic<size_t> waiting_receivers{0};

  auto is_closed() const -> bool {
    return closed.load(std::memory_order_acquire);
  }

  auto close() -> void {
    auto was_closed = closed.exchange(true, std::memory_order_acq_rel);
    if (was_closed) {
      return;
    }
    auto waiters = waiting_receivers.load(std::memory_order_acquire);
    for (auto i = size_t{0}; i < waiters; ++i) {
      items.add_permit();
    }
    // Keep one permit available so subsequent `recv()` calls return
    // immediately after the channel drained.
    items.add_permit();
  }
};

template <class T>
struct SenderShared {
  explicit SenderShared(Arc<SenderReceiverShared<T>> channel)
    : channel{std::move(channel)} {
  }

  ~SenderShared() {
    channel->close();
  }

  SenderShared(SenderShared const&) = delete;
  auto operator=(SenderShared const&) -> SenderShared& = delete;
  SenderShared(SenderShared&&) = delete;
  auto operator=(SenderShared&&) -> SenderShared& = delete;

  Arc<SenderReceiverShared<T>> channel;
};

/// Handle to the sending end of a channel.
///
/// Dropping the last sender closes the channel.
template <class T>
class Sender {
public:
  explicit Sender(Arc<SenderShared<T>> shared) : shared_{std::move(shared)} {
  }

  Sender(Sender const&) = default;
  auto operator=(Sender const&) -> Sender& = default;
  Sender(Sender&&) noexcept = default;
  auto operator=(Sender&&) noexcept -> Sender& = default;
  ~Sender() = default;

  /// Sends a value to the channel, waiting for capacity.
  auto send(T x) -> Task<void> {
    auto& channel = shared_->channel;
    auto guard = co_await channel->slots.acquire();
    channel->data.enqueue(std::move(x));
    guard.forget();
    channel->items.add_permit();
  }

  /// Sends a value if the channel is not full.
  auto try_send(T x) -> Result<void, T> {
    auto& channel = shared_->channel;
    auto guard = channel->slots.try_acquire();
    if (not guard) {
      return Err{std::move(x)};
    }
    channel->data.enqueue(std::move(x));
    guard->forget();
    channel->items.add_permit();
    return {};
  }

  auto clone() const -> Sender {
    return Sender{*this};
  }

private:
  Arc<SenderShared<T>> shared_;
};

/// Handle to the receiving end of a channel.
///
/// Unlike in Rust, dropping the receiver does not close the channel. The sender
/// might thus eventually block. The outer system needs to be designed such that
/// a dropped receiver eventually leads to cancellation of the sender. This is
/// not an oversight, but a conscious choice.
template <class T>
class Receiver {
public:
  explicit Receiver(Arc<SenderReceiverShared<T>> shared)
    : shared_{std::move(shared)} {
  }

  /// Returns `None` if channel is closed.
  auto recv() -> Task<Option<T>> {
    while (true) {
      if (auto item_guard = shared_->items.try_acquire()) {
        T value;
        if (shared_->data.try_dequeue(value)) {
          item_guard->forget();
          shared_->slots.add_permit();
          co_return value;
        }
        TENZIR_ASSERT(shared_->is_closed());
        // Preserve the sticky close wakeup for future receives.
        co_return None{};
      }
      if (shared_->is_closed()) {
        co_return None{};
      }
      shared_->waiting_receivers.fetch_add(1, std::memory_order_acq_rel);
      auto waiting_guard = detail::scope_guard{[&] noexcept {
        auto previous
          = shared_->waiting_receivers.fetch_sub(1, std::memory_order_acq_rel);
        TENZIR_ASSERT(previous > 0);
      }};
      if (auto item_guard = shared_->items.try_acquire()) {
        T value;
        if (shared_->data.try_dequeue(value)) {
          item_guard->forget();
          shared_->slots.add_permit();
          co_return value;
        }
        TENZIR_ASSERT(shared_->is_closed());
        co_return None{};
      }
      if (shared_->is_closed()) {
        co_return None{};
      }
      auto item_guard = co_await shared_->items.acquire();
      T value;
      if (shared_->data.try_dequeue(value)) {
        item_guard.forget();
        shared_->slots.add_permit();
        co_return value;
      }
      TENZIR_ASSERT(shared_->is_closed());
      co_return None{};
    }
  }

  /// Returns immediately, indicating whether the channel is empty or closed.
  auto try_recv() -> Task<Result<T, TryRecvError>> {
    auto item_guard = shared_->items.try_acquire();
    if (not item_guard) {
      if (shared_->is_closed()) {
        co_return TryRecvError::closed;
      }
      co_return TryRecvError::empty;
    }
    T value;
    if (shared_->data.try_dequeue(value)) {
      item_guard->forget();
      shared_->slots.add_permit();
      co_return std::move(value);
    }
    TENZIR_ASSERT(shared_->is_closed());
    co_return TryRecvError::closed;
  }

private:
  Arc<SenderReceiverShared<T>> shared_;
};

template <class T>
struct SenderReceiver {
  Sender<T> sender;
  Receiver<T> receiver;
};

/// Returns a bounded channel with the given capacity.
template <class T>
auto channel(size_t capacity) -> SenderReceiver<T> {
  auto shared = Arc<SenderReceiverShared<T>>{capacity};
  auto sender_shared = Arc<SenderShared<T>>{std::in_place, shared};
  return {
    Sender<T>{std::move(sender_shared)},
    Receiver<T>{std::move(shared)},
  };
}

} // namespace tenzir
