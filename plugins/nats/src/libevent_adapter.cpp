//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir_plugins/nats/common.hpp"

// The nats.c libevent adapter is header-only and defines non-inline C
// functions. Include it in exactly one translation unit.
#include <folly/io/async/ScopedEventBaseThread.h>
#include <nats/adapters/libevent.h>

#include <mutex>

namespace tenzir::plugins::nats {

auto configure_folly_event_loop(natsOptions* options,
                                folly::EventBase& event_base) -> natsStatus {
  return natsOptions_SetEventLoop(options, event_base.getLibeventBase(),
                                  natsLibevent_Attach, natsLibevent_Read,
                                  natsLibevent_Write, natsLibevent_Detach);
}

auto initialize_folly_event_loop_adapter() -> void {
  static std::once_flag once;
  std::call_once(once, [] {
    natsLibevent_Init();
  });
}

auto nats_event_base() -> folly::Executor::KeepAlive<folly::EventBase> {
  // Never destroyed, so that it outlives every connection.
  static auto* thread = [] {
    // libevent gives a base locks only when threading support was enabled
    // before the base was created. nats.c adds and removes events from
    // publishing threads, so this base needs them.
    initialize_folly_event_loop_adapter();
    return new folly::ScopedEventBaseThread{"nats-io"};
  }();
  return folly::getKeepAliveToken(thread->getEventBase());
}

} // namespace tenzir::plugins::nats
