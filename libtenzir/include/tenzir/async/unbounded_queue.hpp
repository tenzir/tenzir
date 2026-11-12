//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <folly/coro/UnboundedQueue.h>

#include <cstddef>

namespace tenzir {

/// Log2 of the number of slots per segment for our folly-backed queues.
///
/// folly allocates the first segment of an unbounded queue eagerly in its
/// constructor. With folly's default of 256 slots, an idle queue costs tens of
/// kilobytes, which adds up because every operator owns several such queues.
/// 16 slots keep idle queues small, while a busy queue still amortizes one
/// segment allocation over 16 items.
inline constexpr size_t queue_lg_segment_size = 4;

template <class T, bool SingleProducer = false, bool SingleConsumer = false>
using UnboundedQueue
  = folly::coro::UnboundedQueue<T, SingleProducer, SingleConsumer,
                                /*MayBlock=*/false, queue_lg_segment_size>;

} // namespace tenzir
