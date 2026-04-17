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
#include <nats/adapters/libevent.h>

namespace tenzir::plugins::nats {

auto configure_folly_event_loop(natsOptions* options,
                                folly::EventBase& event_base) -> natsStatus {
  return natsOptions_SetEventLoop(options, event_base.getLibeventBase(),
                                  natsLibevent_Attach, natsLibevent_Read,
                                  natsLibevent_Write, natsLibevent_Detach);
}

auto initialize_folly_event_loop_adapter() -> void {
  natsLibevent_Init();
}

} // namespace tenzir::plugins::nats
