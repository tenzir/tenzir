//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/task.hpp"
#include "tenzir/detail/assert.hpp"

#include <folly/fibers/Semaphore.h>

namespace tenzir {

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
    auto target = other.impl_.getAvailableTokens();
    auto current = impl_.getAvailableTokens();
    while (current < target) {
      impl_.signal();
      current += 1;
    }
    while (current > target) {
      // This is noexcept, but we still want to sanity check.
      TENZIR_ASSERT(impl_.try_wait());
      current -= 1;
    }
    TENZIR_ASSERT(impl_.getAvailableTokens()
                  == other.impl_.getAvailableTokens());
    return *this;
  }

  auto available_permits() const -> size_t {
    return impl_.getAvailableTokens();
  }

  auto signal() -> void {
    impl_.signal();
  }

  auto acquire() -> Task<void> {
    return impl_.co_wait();
  }

private:
  folly::fibers::Semaphore impl_{0};
};

} // namespace tenzir
