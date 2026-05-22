//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/semaphore.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/atomic.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/option.hpp"

#include <cstddef>
#include <type_traits>
#include <utility>

namespace tenzir {

class RawSharedMutex;

template <class T>
class SharedMutex;

namespace detail {

enum class SharedMutexMode {
  unique,
  shared,
};

template <SharedMutexMode Mode>
class [[nodiscard]] RawSharedMutexGuard {
public:
  ~RawSharedMutexGuard() noexcept {
    maybe_unlock();
  }

  RawSharedMutexGuard(RawSharedMutexGuard&& other) noexcept
    : locked_{std::exchange(other.locked_, nullptr)},
      turnstile_{std::move(other.turnstile_)},
      room_empty_{std::move(other.room_empty_)} {
  }

  auto operator=(RawSharedMutexGuard&& other) noexcept -> RawSharedMutexGuard& {
    if (this == &other) {
      return *this;
    }
    maybe_unlock();
    locked_ = std::exchange(other.locked_, nullptr);
    turnstile_ = std::move(other.turnstile_);
    room_empty_ = std::move(other.room_empty_);
    return *this;
  }
  RawSharedMutexGuard(RawSharedMutexGuard const&) = delete;
  auto operator=(RawSharedMutexGuard const&) -> RawSharedMutexGuard& = delete;

  auto unlock() -> void;

private:
  auto maybe_unlock() -> void {
    if (locked_) {
      unlock();
    }
  }

  friend class tenzir::RawSharedMutex;

  explicit RawSharedMutexGuard(RawSharedMutex& mutex)
    : locked_{std::addressof(mutex)} {
  }

  RawSharedMutexGuard(RawSharedMutex& mutex, SemaphorePermit turnstile,
                      SemaphorePermit room_empty)
    : locked_{std::addressof(mutex)},
      turnstile_{std::move(turnstile)},
      room_empty_{std::move(room_empty)} {
  }

  RawSharedMutex* locked_ = nullptr;
  Option<SemaphorePermit> turnstile_ = None{};
  Option<SemaphorePermit> room_empty_ = None{};
};

template <class T, SharedMutexMode Mode>
class [[nodiscard]] SharedMutexGuard {
private:
  static constexpr auto is_unique = Mode == SharedMutexMode::unique;
  using RawGuard = RawSharedMutexGuard<Mode>;
  using Value = std::conditional_t<is_unique, T, T const>;

public:
  ~SharedMutexGuard() noexcept {
    maybe_unlock();
  }

  SharedMutexGuard(SharedMutexGuard&& other) noexcept
    : locked_{std::exchange(other.locked_, nullptr)},
      guard_{std::move(other.guard_)} {
  }
  auto operator=(SharedMutexGuard&& other) noexcept -> SharedMutexGuard& {
    if (this == &other) {
      return *this;
    }
    maybe_unlock();
    locked_ = std::exchange(other.locked_, nullptr);
    guard_ = std::move(other.guard_);
    return *this;
  }
  SharedMutexGuard(SharedMutexGuard const&) = delete;
  auto operator=(SharedMutexGuard const&) -> SharedMutexGuard& = delete;

  auto operator*() -> Value& {
    TENZIR_ASSERT(locked_);
    return locked_->value_;
  }

  auto operator->() -> Value* {
    TENZIR_ASSERT(locked_);
    return &locked_->value_;
  }

  auto unlock() -> void {
    TENZIR_ASSERT(locked_);
    guard_.unlock();
    locked_ = nullptr;
  }

private:
  auto maybe_unlock() -> void {
    if (locked_) {
      unlock();
    }
  }

  friend class tenzir::SharedMutex<T>;

  explicit SharedMutexGuard(SharedMutex<T>& mutex, RawGuard guard)
    : locked_{std::addressof(mutex)}, guard_{std::move(guard)} {
  }

  SharedMutex<T>* locked_ = nullptr;
  RawGuard guard_;
};

} // namespace detail

using RawSharedMutexUniqueGuard
  = detail::RawSharedMutexGuard<detail::SharedMutexMode::unique>;
using RawSharedMutexSharedGuard
  = detail::RawSharedMutexGuard<detail::SharedMutexMode::shared>;

template <class T>
using SharedMutexUniqueGuard
  = detail::SharedMutexGuard<T, detail::SharedMutexMode::unique>;

template <class T>
using SharedMutexSharedGuard
  = detail::SharedMutexGuard<T, detail::SharedMutexMode::shared>;

/// A cancellable async shared mutex with writer preference once admitted.
///
/// Unique locks pass through a turnstile before waiting for the room to empty.
/// A unique lock that holds the turnstile prevents later shared locks from
/// entering while current shared locks drain. Writers and readers compete
/// fairly for the turnstile itself, so a writer is not strictly prioritized
/// over readers that are already queued behind it.
class RawSharedMutex {
public:
  RawSharedMutex() = default;
  RawSharedMutex(RawSharedMutex const&) = delete;
  auto operator=(RawSharedMutex const&) -> RawSharedMutex& = delete;
  RawSharedMutex(RawSharedMutex&&) = delete;
  auto operator=(RawSharedMutex&&) -> RawSharedMutex& = delete;
  ~RawSharedMutex() = default;

  auto unique_lock() -> Task<RawSharedMutexUniqueGuard>;

  auto shared_lock() -> Task<RawSharedMutexSharedGuard>;

private:
  auto unlock_shared() -> void;

  template <detail::SharedMutexMode>
  friend class detail::RawSharedMutexGuard;

  Semaphore turnstile_{1};
  Semaphore room_empty_{1};
  Atomic<size_t> active_readers_ = 0;
};

template <class T>
class SharedMutex {
public:
  SharedMutex() = default;

  template <class... Args>
    requires std::constructible_from<T, Args...>
  SharedMutex(std::in_place_t, Args&&... args)
    : value_{std::forward<Args>(args)...} {
  }

  explicit SharedMutex(T x) : value_{std::move(x)} {
  }

  auto unique_lock() -> Task<SharedMutexUniqueGuard<T>>;

  auto shared_lock() -> Task<SharedMutexSharedGuard<T>>;

private:
  template <class, detail::SharedMutexMode>
  friend class detail::SharedMutexGuard;

  RawSharedMutex mutex_;
  T value_ = {};
};

template <detail::SharedMutexMode Mode>
auto detail::RawSharedMutexGuard<Mode>::unlock() -> void {
  TENZIR_ASSERT(locked_);
  if constexpr (Mode == detail::SharedMutexMode::unique) {
    room_empty_ = None{};
    turnstile_ = None{};
  } else {
    locked_->unlock_shared();
  }
  locked_ = nullptr;
}

inline auto RawSharedMutex::unique_lock() -> Task<RawSharedMutexUniqueGuard> {
  auto turnstile = co_await turnstile_.acquire();
  auto room_empty = co_await room_empty_.acquire();
  co_return RawSharedMutexUniqueGuard{
    *this,
    std::move(turnstile),
    std::move(room_empty),
  };
}

inline auto RawSharedMutex::shared_lock() -> Task<RawSharedMutexSharedGuard> {
  auto turnstile = co_await turnstile_.acquire();
  auto const previous_readers
    = active_readers_.fetch_add(1, std::memory_order_acq_rel);
  if (previous_readers == 0) {
    try {
      // A cancelled `co_wait()` in Folly never atomically claims the permit, so
      // the catch block below fully restores state on cancellation.
      co_await room_empty_.consume();
    } catch (...) {
      auto const readers
        = active_readers_.fetch_sub(1, std::memory_order_acq_rel);
      TENZIR_ASSERT(readers > 0);
      throw;
    }
    // At this point `room_empty_` is consumed and `active_readers_` is
    // non-zero. The coroutine is committed to returning a guard. There is no
    // `co_await` between here and `co_return`, so Folly cannot cancel in this
    // window. If that assumption ever breaks, the rollback must be:
    //   active_readers_.fetch_sub(1, acq_rel) + room_empty_.add_permit().
  }
  // Release the turnstile before returning so the next reader or writer can
  // enter immediately rather than waiting for the guard's lifetime to end.
  // This is intentionally explicit rather than relying on the SemaphorePermit
  // RAII destructor, which would run only when the coroutine frame is torn down.
  turnstile.release();
  co_return RawSharedMutexSharedGuard{*this};
}

inline auto RawSharedMutex::unlock_shared() -> void {
  auto const readers = active_readers_.fetch_sub(1, std::memory_order_acq_rel);
  TENZIR_ASSERT(readers > 0);
  if (readers == 1) {
    room_empty_.add_permit();
  }
}

template <class T>
auto SharedMutex<T>::unique_lock() -> Task<SharedMutexUniqueGuard<T>> {
  auto guard = co_await mutex_.unique_lock();
  co_return SharedMutexUniqueGuard<T>{*this, std::move(guard)};
}

template <class T>
auto SharedMutex<T>::shared_lock() -> Task<SharedMutexSharedGuard<T>> {
  auto guard = co_await mutex_.shared_lock();
  co_return SharedMutexSharedGuard<T>{*this, std::move(guard)};
}

} // namespace tenzir
