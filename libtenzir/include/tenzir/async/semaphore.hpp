//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/task.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/option.hpp"

#include <folly/fibers/Semaphore.h>

#include <memory>
#include <utility>

namespace tenzir {

class SemaphorePermit;

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

  /// Tries to consume a permit without blocking.
  auto try_consume() -> bool {
    return impl_.try_wait();
  }

  /// Acquires a permit and returns a guard that releases it.
  auto acquire() -> Task<SemaphorePermit>;

  /// Tries to acquire a permit and returns a guard that releases it.
  auto try_acquire() -> Option<SemaphorePermit>;

  /// Consumes a permit without returning a guard that restores it.
  auto consume() -> Task<void> {
    co_await impl_.co_wait();
  }

private:
  friend class SemaphorePermit;

  folly::fibers::Semaphore impl_;
};

class [[nodiscard]] SemaphorePermit {
public:
  ~SemaphorePermit() noexcept {
    release();
  }

  SemaphorePermit(SemaphorePermit&& other) noexcept
    : acquired_{std::exchange(other.acquired_, nullptr)} {
  }
  auto operator=(SemaphorePermit&& other) noexcept -> SemaphorePermit& {
    if (this != &other) {
      release();
      acquired_ = std::exchange(other.acquired_, nullptr);
    }
    return *this;
  }
  SemaphorePermit(SemaphorePermit const& other) = delete;
  auto operator=(SemaphorePermit const& other) -> SemaphorePermit& = delete;

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

  explicit SemaphorePermit(Semaphore& acquired) : acquired_{&acquired} {
  }

  Semaphore* acquired_;
};

inline auto Semaphore::acquire() -> Task<SemaphorePermit> {
  co_await consume();
  co_return SemaphorePermit{*this};
}

inline auto Semaphore::try_acquire() -> Option<SemaphorePermit> {
  if (not try_consume()) {
    return None{};
  }
  return SemaphorePermit{*this};
}

} // namespace tenzir
