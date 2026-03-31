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
#include "tenzir/option.hpp"
#include "tenzir/result.hpp"

#include <folly/concurrency/UnboundedQueue.h>

#include <cstddef>
#include <utility>

namespace tenzir {

enum class TryRecvError {
  empty,
  closed,
};

namespace detail {

template <class T>
struct Channel {
  explicit Channel(size_t capacity) : capacity{capacity}, ready{0} {
  }

  /// The thread-safe queue backing our channel.
  folly::UMPMCQueue<T, false> data;
  /// Number of remaining send permits.
  Semaphore capacity;
  /// Number of ready-to-dequeue items, plus a sticky wakeup permit after close.
  Semaphore ready;
  /// Whether the channel is closed because the last sender got destroyed.
  Atomic<bool> closed{false};

  auto is_closed() const -> bool {
    return closed.load(std::memory_order_acquire);
  }
};

template <class T>
struct SenderCore {
  explicit SenderCore(Arc<Channel<T>> channel) : channel{std::move(channel)} {
  }

  ~SenderCore() {
    auto was_closed = channel->closed.exchange(true, std::memory_order_release);
    TENZIR_ASSERT(not was_closed);
    // Receivers that observe closure without dequeuing an item return this
    // permit to wake the next blocked receiver and leave one sticky wakeup for
    // future `recv()` calls.
    channel->ready.add_permit();
  }

  SenderCore(SenderCore const&) = delete;
  auto operator=(SenderCore const&) -> SenderCore& = delete;
  SenderCore(SenderCore&&) = delete;
  auto operator=(SenderCore&&) -> SenderCore& = delete;

  Arc<Channel<T>> channel;
};

} // namespace detail

/// Handle to the sending end of a channel.
///
/// Dropping the last sender closes the channel.
template <class T>
class Sender {
public:
  explicit Sender(Arc<detail::SenderCore<T>> core) : core_{std::move(core)} {
  }

  /// Sends a value to the channel, waiting for capacity.
  auto send(T x) -> Task<void> {
    co_await core_->channel->capacity.consume();
    do_send(std::move(x));
  }

  /// Sends a value if the channel is not full.
  auto try_send(T x) -> Result<void, T> {
    if (not core_->channel->capacity.try_consume()) {
      return Err{std::move(x)};
    }
    do_send(std::move(x));
    return {};
  }

private:
  auto do_send(T x) -> void {
    core_->channel->data.enqueue(std::move(x));
    core_->channel->ready.add_permit();
  }

  Arc<detail::SenderCore<T>> core_;
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
  explicit Receiver(Arc<detail::Channel<T>> shared)
    : shared_{std::move(shared)} {
  }

  /// Returns `None` if channel is closed.
  auto recv() -> Task<Option<T>> {
    co_return do_recv(co_await shared_->ready.acquire());
  }

  /// Returns immediately, indicating whether the channel is empty or closed.
  auto try_recv() -> Task<Result<T, TryRecvError>> {
    auto permit = shared_->ready.try_acquire();
    if (not permit) {
      co_return Err{shared_->is_closed() ? TryRecvError::closed
                                         : TryRecvError::empty};
    }
    co_return do_recv(std::move(*permit)).ok_or(TryRecvError::closed);
  }

private:
  auto do_recv(SemaphorePermit permit) -> Option<T> {
    if (auto value = shared_->data.try_dequeue()) {
      permit.forget();
      shared_->capacity.add_permit();
      return std::move(*value);
    }
    TENZIR_ASSERT(shared_->is_closed());
    return None{};
  }

  Arc<detail::Channel<T>> shared_;
};

/// Returns a bounded channel with the given capacity.
template <class T>
auto channel(size_t capacity) -> std::tuple<Sender<T>, Receiver<T>> {
  auto shared = Arc<detail::Channel<T>>{capacity};
  auto sender_shared = Arc<detail::SenderCore<T>>{std::in_place, shared};
  return {
    Sender<T>{std::move(sender_shared)},
    Receiver<T>{std::move(shared)},
  };
}

} // namespace tenzir
