//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/broadcast.hpp"
#include "tenzir/async/signal.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/table_slice.hpp"

#include <folly/concurrency/AtomicSharedPtr.h>

#include <memory>

namespace tenzir {

using EventBatch = variant<table_slice, nova::Events>;

enum class ExecutionOutcome {
  completed,
  failed,
  cancelled,
};

namespace detail {
struct InspectionState;
using TapBroadcast = Broadcast<variant<EventBatch, ExecutionOutcome>>;
} // namespace detail

/// Capture at a physical channel's receive boundary. Subscribers share native
/// batches; a full inspection queue never blocks the pipeline.
class ChannelTap {
public:
  auto capture(table_slice const& batch) const -> void;
  auto capture(nova::Events const& batch) const -> void;

private:
  friend class PipelineObserver;
  friend struct detail::InspectionState;
  // Physical instances share their logical channel's broadcast, which owns
  // subscriptions and handles capture without taking the observer lock.
  folly::atomic_shared_ptr<detail::TapBroadcast> broadcast_;
};

template <class T>
auto capture_channel(ChannelTap const& tap, T const& message) -> void {
  if constexpr (std::same_as<T, OperatorMsg<table_slice>>) {
    if (auto* batch = try_as<table_slice>(message)) {
      tap.capture(*batch);
    }
  } else if constexpr (std::same_as<T, OperatorMsg<nova::Events>>) {
    if (auto* batch = try_as<nova::Events>(message)) {
      tap.capture(*batch);
    }
  }
}

} // namespace tenzir
