//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/arc.hpp"
#include "tenzir/async/mutex.hpp"
#include "tenzir/async/shared_mutex.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/option.hpp"

#include <functional>
#include <utility>

namespace tenzir {

template <class T>
class PublishedUpdate;

/// Immutable snapshots that readers get without waiting for an update.
template <class T>
class Published {
public:
  /// Calls `on_publish` with the old and the new value after each change.
  explicit Published(T initial,
                     std::function<void(T const&, T const&)> on_publish = {})
    : current_{Arc<T const>{std::in_place, std::move(initial)}},
      on_publish_{std::move(on_publish)} {
  }

  /// The last published value.
  auto get() -> Task<Arc<T const>> {
    auto current = co_await current_.shared_lock();
    co_return *current;
  }

  /// Starts an update, waiting for the one in progress to end.
  auto begin_update() -> Task<PublishedUpdate<T>> {
    auto updating = co_await updating_.lock();
    auto current = co_await get();
    co_return PublishedUpdate<T>{*this, std::move(updating),
                                 std::move(current)};
  }

private:
  friend class PublishedUpdate<T>;

  RawMutex updating_;
  SharedMutex<Arc<T const>> current_;
  std::function<void(T const&, T const&)> on_publish_;
};

/// A change to a `Published` value that takes effect only when published.
template <class T>
class [[nodiscard]] PublishedUpdate {
public:
  PublishedUpdate(PublishedUpdate&&) noexcept = default;
  auto operator=(PublishedUpdate&&) noexcept -> PublishedUpdate& = default;
  PublishedUpdate(PublishedUpdate const&) = delete;
  auto operator=(PublishedUpdate const&) -> PublishedUpdate& = delete;
  ~PublishedUpdate() = default;

  /// The value this update started from.
  auto current() const -> T const& {
    return *current_;
  }

  /// The value `publish()` would install: `next()` if used, else `current()`.
  auto staged() const -> T const& {
    return next_ ? *next_ : *current_;
  }

  /// The value to publish, a copy of `current()` made on first use.
  auto next() -> T& {
    if (not next_) {
      next_ = *current_;
    }
    return *next_;
  }

  /// Makes `next()` the published value, if it was used, and ends the update.
  auto publish() && -> Task<void> {
    TENZIR_ASSERT(published_);
    // Taken out, so that neither the lock nor the old value outlives this.
    auto published = std::exchange(published_, nullptr);
    auto updating = std::move(updating_);
    auto current = std::move(current_);
    if (not next_) {
      co_return;
    }
    auto next = Arc<T const>{std::in_place, std::move(*next_)};
    next_ = None{};
    *co_await published->current_.unique_lock() = next;
    if (published->on_publish_) {
      published->on_publish_(*current, *next);
    }
  }

private:
  friend class Published<T>;

  PublishedUpdate(Published<T>& published, RawMutexGuard updating,
                  Arc<T const> current)
    : published_{&published},
      updating_{std::move(updating)},
      current_{std::move(current)} {
  }

  Published<T>* published_;
  RawMutexGuard updating_;
  Arc<T const> current_;
  Option<T> next_ = None{};
};

} // namespace tenzir
