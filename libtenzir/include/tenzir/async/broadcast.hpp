//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/arc.hpp"
#include "tenzir/async/bounded_queue.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/scope_guard.hpp"

#include <folly/concurrency/AtomicSharedPtr.h>

#include <algorithm>
#include <memory>
#include <mutex>
#include <vector>

namespace tenzir {

enum class BroadcastMode { buffered, latest };

/// Bounded fanout with independently owned, move-only receivers.
///
/// buffered drops new values when a receiver is full and permits concurrent
/// publishers without taking the registry lock.
/// Publishers must have joined before close(); accepted values precede closure.
/// latest serializes delivery, coalesces concurrent publications, and
/// permits close() during delivery, including reentrant close from a receiver.
/// It needs two queue slots: one pending value and one assigned to a suspended
/// receive. Closing replaces unread values in this mode.
///
/// A receiver retains its queue and only a weak link to the registry.
/// Destroying or cancelling it detaches immediately. Destroying the broadcaster
/// closes its receivers; closing with a final value delivers that value before
/// EOF and retains it for subscriptions created after closure.
template <class T, BroadcastMode Mode = BroadcastMode::buffered>
class Broadcast {
  struct Message {
    Option<T> value;
    bool final = false;
  };
  using Queue = BoundedQueue<Message>;
  using Readers = std::vector<std::weak_ptr<Queue>>;

  struct State {
    explicit State(uint32_t capacity) : capacity{capacity} {
      TENZIR_ASSERT(capacity > 0);
      TENZIR_ASSERT(Mode != BroadcastMode::latest or capacity == 2);
    }

    auto subscribe() -> Arc<Queue> {
      auto queue = Arc<Queue>{capacity};
      auto lock = std::unique_lock{mutex};
      if (closed) {
        auto final = Message{terminal, true};
        lock.unlock();
        queue->force_enqueue(std::move(final));
      } else {
        auto current = readers.load();
        auto next = current ? *current : Readers{};
        next.emplace_back(Arc{queue}.into_shared());
        readers.store(std::make_shared<Readers const>(std::move(next)));
      }
      return queue;
    }

    auto detach(Queue const* queue) -> void {
      auto lock = std::unique_lock{mutex};
      if (auto current = readers.load()) {
        auto next = *current;
        std::erase_if(next, [&](auto const& entry) {
          auto value = entry.lock();
          return not value or value.get() == queue;
        });
        readers.store(next.empty()
                        ? nullptr
                        : std::make_shared<Readers const>(std::move(next)));
      }
    }

    static auto deliver(Readers const& readers, Message const& message)
      -> void {
      for (auto const& reader : readers) {
        if (auto queue = reader.lock()) {
          if constexpr (Mode == BroadcastMode::latest) {
            std::ignore = queue->try_dequeue();
          }
          if (message.final) {
            queue->force_enqueue(message);
          } else {
            auto accepted = queue->try_enqueue(message);
            if constexpr (Mode == BroadcastMode::latest) {
              TENZIR_ASSERT(accepted);
            }
          }
        }
      }
    }

    auto publish(T const& value) -> void {
      if constexpr (Mode == BroadcastMode::buffered) {
        if (auto current = readers.load()) {
          deliver(*current, Message{value});
        }
      } else {
        auto lock = std::unique_lock{mutex};
        if (not closed) {
          pending = Message{value};
          drain(lock);
        }
      }
    }

    auto close(Option<T> final) -> void {
      auto lock = std::unique_lock{mutex};
      if (closed) {
        return;
      }
      closed = true;
      terminal = std::move(final);
      auto message = Message{terminal, true};
      if constexpr (Mode == BroadcastMode::latest) {
        pending = std::move(message);
        drain(lock);
      } else {
        auto current = readers.exchange(nullptr);
        lock.unlock();
        if (current) {
          deliver(*current, message);
        }
      }
    }

    auto drain(std::unique_lock<std::mutex>& lock) -> void {
      if (publishing) {
        return;
      }
      publishing = true;
      while (pending) {
        auto message = *std::exchange(pending, None{});
        auto current
          = message.final ? readers.exchange(nullptr) : readers.load();
        lock.unlock();
        if (current) {
          deliver(*current, message);
        }
        lock.lock();
      }
      publishing = false;
    }

    uint32_t capacity;
    // Registry changes and latest-value delivery are serialized, but never hold
    // the mutex while waking receivers: resumed code may detach or publish.
    std::mutex mutex;
    folly::atomic_shared_ptr<Readers const> readers;
    bool closed = false;
    Option<T> terminal;
    bool publishing = false;
    Option<Message> pending;
  };

public:
  class Receiver {
  public:
    Receiver() = default;
    ~Receiver() {
      reset();
    }
    Receiver(Receiver&& other) noexcept
      : queue_{std::exchange(other.queue_, None{})},
        state_{std::move(other.state_)} {
    }
    auto operator=(Receiver&& other) noexcept -> Receiver& {
      if (this != &other) {
        reset();
        queue_ = std::exchange(other.queue_, None{});
        state_ = std::move(other.state_);
      }
      return *this;
    }
    Receiver(Receiver const&) = delete;
    auto operator=(Receiver const&) -> Receiver& = delete;

    /// One receive may be in flight; join it before moving or destroying the
    /// receiver. Cancellation detaches this receiver.
    auto recv() -> Task<Option<T>> {
      if (not queue_) {
        co_return None{};
      }
      auto detach = detail::scope_guard{[&]() noexcept {
        reset();
      }};
      co_await folly::coro::co_safe_point;
      auto message = co_await (*queue_)->dequeue();
      if (not message.final) {
        detach.disable();
      }
      co_return std::move(message.value);
    }

  private:
    friend class Broadcast;
    auto reset() -> void {
      if (queue_) {
        if (auto state = state_.lock()) {
          state->detach(&**queue_);
        }
        queue_ = None{};
        state_.reset();
      }
    }
    Option<Arc<Queue>> queue_;
    std::weak_ptr<State> state_;
  };

  explicit Broadcast(uint32_t capacity = 2) : state_{std::in_place, capacity} {
  }
  ~Broadcast() {
    state_->close(None{});
  }
  Broadcast(Broadcast const&) = delete;
  auto operator=(Broadcast const&) -> Broadcast& = delete;

  auto subscribe() -> Receiver {
    auto receiver = Receiver{};
    receiver.queue_ = state_->subscribe();
    receiver.state_ = Arc{state_}.into_shared();
    return receiver;
  }
  auto has_receivers() const -> bool {
    return state_->readers.load() != nullptr;
  }
  auto publish(T const& value) -> void {
    auto state = state_;
    state->publish(value);
  }
  auto close(T final) -> void {
    auto state = state_;
    state->close(std::move(final));
  }

private:
  // Receivers use weak backlinks to detach safely after the sender is gone.
  Arc<State> state_;
};

} // namespace tenzir
