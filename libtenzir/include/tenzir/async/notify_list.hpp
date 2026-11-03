//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/arc.hpp"
#include "tenzir/async/notify.hpp"

#include <folly/Synchronized.h>

#include <algorithm>
#include <mutex>
#include <utility>
#include <vector>

namespace tenzir {

/// A set of `Notify`s, one per subscriber, that are woken together.
///
/// Each subscriber waits on its own signal, so a slow one never holds up the
/// others, and notifications coalesce per subscriber the way `Notify` does.
///
/// Subscribing and unsubscribing replace the list, so `notify_all` only takes
/// a reference under the lock and notifies without it: a notification may
/// resume a waiter inline, and that waiter may subscribe or notify in turn.
/// This favours lists that change rarely and are notified often.
class NotifyList {
public:
  /// A subscriber's signal, registered while this lives.
  ///
  /// Must not outlive the list it came from.
  class Subscription {
  public:
    Subscription(Subscription&& other) noexcept
      : list_{std::exchange(other.list_, nullptr)},
        signal_{std::move(other.signal_)} {
    }

    auto operator=(Subscription&& other) noexcept -> Subscription& {
      if (this != &other) {
        unsubscribe();
        list_ = std::exchange(other.list_, nullptr);
        signal_ = std::move(other.signal_);
      }
      return *this;
    }

    Subscription(Subscription const&) = delete;
    auto operator=(Subscription const&) -> Subscription& = delete;

    ~Subscription() {
      unsubscribe();
    }

    /// The signal `notify_all` notifies. Others may hold and notify it too.
    auto signal() const -> Arc<Notify> {
      return signal_;
    }

  private:
    friend class NotifyList;

    Subscription(NotifyList& list, Arc<Notify> signal)
      : list_{&list}, signal_{std::move(signal)} {
    }

    auto unsubscribe() noexcept -> void {
      if (list_) {
        std::exchange(list_, nullptr)->remove(*signal_);
      }
    }

    NotifyList* list_;
    Arc<Notify> signal_;
  };

  NotifyList() = default;
  NotifyList(NotifyList const&) = delete;
  auto operator=(NotifyList const&) -> NotifyList& = delete;
  NotifyList(NotifyList&&) = delete;
  auto operator=(NotifyList&&) -> NotifyList& = delete;

  /// Registers a new signal, which `notify_all` notifies from now on.
  auto subscribe() -> Subscription {
    auto signal = Arc<Notify>{std::in_place};
    signals_.withLock([&](Signals& signals) {
      auto next = *signals;
      next.push_back(signal);
      signals = Signals{std::in_place, std::move(next)};
    });
    return Subscription{*this, std::move(signal)};
  }

  /// Notifies every subscriber's signal.
  auto notify_all() -> void {
    auto signals = signals_.copy();
    for (auto& signal : *signals) {
      signal->notify_one();
    }
  }

private:
  /// Immutable once shared: a change replaces the list rather than editing it.
  /// Not `const` in the type, because `Arc` passes constness on to the
  /// `Notify`s, and notifying one is not `const`.
  using Signals = Arc<std::vector<Arc<Notify>>>;

  auto remove(Notify const& signal) noexcept -> void {
    signals_.withLock([&](Signals& signals) {
      auto next = *signals;
      std::erase_if(next, [&](Arc<Notify> const& candidate) {
        return &*candidate == &signal;
      });
      signals = Signals{std::in_place, std::move(next)};
    });
  }

  folly::Synchronized<Signals, std::mutex> signals_{std::in_place,
                                                    std::in_place};
};

} // namespace tenzir
