/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/fwd.hpp"
#include "vast/system/accountant.hpp"
#include "vast/system/instrumentation.hpp"

#include <caf/behavior.hpp>
#include <caf/fwd.hpp>
#include <caf/stateful_actor.hpp>

#include <chrono>
#include <cstdint>

namespace vast::system {

// The base class for SINK actors.
struct sink_state {
  std::chrono::steady_clock::duration flush_interval = std::chrono::seconds(1);
  std::chrono::steady_clock::time_point last_flush;
  uint64_t processed = 0;
  uint64_t max_events = 0;
  caf::event_based_actor* self;
  caf::actor statistics_subscriber;
  accountant_actor accountant;
  vast::system::measurement measurement;
  format::writer_ptr writer;
  const char* name = "writer";

  explicit sink_state(caf::event_based_actor* self_ptr);

  void send_report();
};

caf::behavior sink(caf::stateful_actor<sink_state>* self,
                   format::writer_ptr&& writer, uint64_t max_events);

} // namespace vast::system
