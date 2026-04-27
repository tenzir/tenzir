//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <zmq.hpp>

namespace tenzir::plugins::zmq {

/// This is the 0mq context singleton. There exists exactly one context per
/// process so that inproc sockets can be used across pipelines within the same
/// node. Since accessing a 0mq context instance is thread-safe, we can share it
/// globally.
inline auto global_context() -> ::zmq::context_t& {
  static auto ctx = ::zmq::context_t{};
  return ctx;
}

} // namespace tenzir::plugins::zmq
