//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/semaphore.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/logger.hpp"

namespace tenzir {

class [[nodiscard]] RawMutexGuard {
private:
  explicit RawMutexGuard(SemaphorePermit guard) : guard_{std::move(guard)} {
  }

  friend class RawMutex;

  SemaphorePermit guard_;
};

/// A cancellable mutex that can be locked asynchronously using 'co_await'.
///
/// This mutex is similar to folly::coro::Mutex but supports cancellation.
class RawMutex {
public:
  RawMutex() : semaphore_{1} {
  }

  auto lock() -> Task<RawMutexGuard> {
    co_return RawMutexGuard{co_await semaphore_.acquire()};
  }

  auto lock_unscoped() -> Task<void> {
    return semaphore_.consume();
  }

  auto unlock() -> void {
    TENZIR_ASSERT(semaphore_.available_permits() == 0);
    semaphore_.add_permit();
  }

private:
  Semaphore semaphore_;
};

template <class T>
class MutexGuard;

template <class T>
class Mutex {
public:
  explicit Mutex(T x) : value_{std::move(x)} {
  }

  auto lock() -> Task<MutexGuard<T>>;

private:
  friend class MutexGuard<T>;

  RawMutex mutex_;
  T value_;
};

template <class T>
class [[nodiscard]] MutexGuard {
public:
  ~MutexGuard() noexcept {
    maybe_unlock();
  }

  MutexGuard(MutexGuard&& other) noexcept {
    *this = std::move(other);
  }
  auto operator=(MutexGuard&& other) noexcept -> MutexGuard& {
    maybe_unlock();
    locked_ = other.locked_;
    other.locked_ = nullptr;
    return *this;
  }
  MutexGuard(MutexGuard& other) = delete;
  auto operator=(MutexGuard& other) = delete;

  auto operator*() -> T& {
    TENZIR_ASSERT(locked_);
    return locked_->value_;
  }

  auto operator->() -> T* {
    TENZIR_ASSERT(locked_);
    return &locked_->value_;
  }

  auto unlock() -> void {
    TENZIR_ASSERT(locked_);
    locked_->mutex_.unlock();
    locked_ = nullptr;
  }

private:
  auto maybe_unlock() -> void {
    if (locked_) {
      locked_->mutex_.unlock();
    }
  }

  friend class Mutex<T>;

  explicit MutexGuard(Mutex<T>& mutex) : locked_{&mutex} {
  }

  Mutex<T>* locked_ = nullptr;
};

template <class T>
auto Mutex<T>::lock() -> Task<MutexGuard<T>> {
  co_await mutex_.lock_unscoped();
  co_return MutexGuard<T>{*this};
}

} // namespace tenzir
