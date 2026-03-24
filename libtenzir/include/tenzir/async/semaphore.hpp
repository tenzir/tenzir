//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/task.hpp"
#include "tenzir/detail/assert.hpp"

#include <folly/fibers/Semaphore.h>

#include <memory>

namespace tenzir {

class SemaphoreGuard;

/// An async semaphore (that is movable unlike Folly's version).
class Semaphore {
public:
  explicit Semaphore(size_t initial) : impl_{initial} {
  }
  ~Semaphore() = default;
  Semaphore(Semaphore const&) = delete;
  auto operator=(Semaphore const&) -> Semaphore& = delete;
  Semaphore(Semaphore&& other) noexcept
    : impl_{other.impl_.getAvailableTokens()} {
  }
  auto operator=(Semaphore&& other) noexcept -> Semaphore& {
    // This assumes exclusive ownership over both semaphores.
    if (this != &other) {
      std::destroy_at(&impl_);
      std::construct_at(&impl_, other.impl_.getAvailableTokens());
    }
    return *this;
  }

  /// Returns the number of permits in the pool.
  auto available_permits() const -> size_t {
    return impl_.getAvailableTokens();
  }

  /// Add a new permit to the pool.
  auto add_permit() -> void {
    impl_.signal();
  }

  /// Acquires a permit and returns a guard that releases it.
  auto acquire() -> Task<SemaphoreGuard>;

  /// Consumes a permit without returning a guard that restores it.
  auto consume() -> Task<void> {
    co_await impl_.co_wait();
  }

private:
  friend class SemaphoreGuard;

  folly::fibers::Semaphore impl_;
};

class [[nodiscard]] SemaphoreGuard {
public:
  ~SemaphoreGuard() noexcept {
    release();
  }

  SemaphoreGuard(SemaphoreGuard&& other) noexcept
    : acquired_{std::exchange(other.acquired_, nullptr)} {
  }
  auto operator=(SemaphoreGuard&& other) noexcept -> SemaphoreGuard& {
    acquired_ = std::exchange(other.acquired_, nullptr);
    return *this;
  }
  SemaphoreGuard(SemaphoreGuard const& other) = delete;
  auto operator=(SemaphoreGuard const& other) -> SemaphoreGuard& = delete;

  /// Releases this guard if it's still held.
  auto release() -> void {
    if (acquired_) {
      acquired_->add_permit();
      forget();
    }
  }

  /// Releases this guard without adding its permit back.
  auto forget() -> void {
    TENZIR_ASSERT(acquired_);
    acquired_ = nullptr;
  }

private:
  friend class Semaphore;

  explicit SemaphoreGuard(Semaphore& acquired) : acquired_{&acquired} {
  }

  Semaphore* acquired_;
};

inline auto Semaphore::acquire() -> Task<SemaphoreGuard> {
  co_await consume();
  co_return SemaphoreGuard{*this};
}

} // namespace tenzir
