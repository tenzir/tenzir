//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/default_configuration.hpp"

#include "vast/atoms.hpp"
#include "vast/defaults.hpp"

#include <caf/config_value.hpp>

#include <chrono>

namespace vast::system {

default_configuration::default_configuration() {
  // Tweak default logging options.
  using namespace std::chrono_literals;
  set("caf.logger.file.excluded-components",
      caf::make_config_value_list("caf", "caf_flow", "caf_stream"));
  set("caf.logger.console.excluded-components",
      caf::make_config_value_list("caf", "caf_flow", "caf_stream"));
  // TODO: Move part of this text into the commit message
  // The `max-batch-delay` is the maximum amount of time that caf waits until
  // it attempts to send an underfull batch along the stream.
  // However, there is a possible deadlock: When an actor has a non-empty
  // outgoing stream and advancing the stream managers for that actor in total
  // takes longer then the `max_batch_delay`, then by the time it is done the
  // stream has generated a `timeout_msg` which is then handled by setting
  // another stream timeout, advancing the streams, etc., basically keeping the
  // actor fully occupied until either the stream is emptied or the
  // `max-throughput` of this actor was reached. The max throughput is by
  // default infinite, and if there are more than N actors in this state (where
  // N is the number of worker threads) then the other end of the stream will
  // never be scheduled and the stream is never emptied.
  // There are multiple possible workarounds:
  //  1) Increase `stream.max-batch-delay`.
  //  2) Set a finite, small `scheduler.max-throughput`. This should empirically
  //     be enough, but it relies on the CAF scheduler to actually schedule the
  //     downstream actor, which empirically can take quite a while at times.
  //     Also, if this is too small, there is a chance that a very busy stream
  //     can drown out an actor completely so that no other work can take place.
  //  3) Increase `scheduler.max-threads`. When there are more threads than
  //     deadlocked actors, eventually the downstream actors will be able to
  //     make progress and the deadlock can be resolved.
  // We do a combination of (1) and (2).
  set("caf.stream.max-batch-delay", caf::timespan{15ms});
  set("caf.scheduler.max-throughput", 500);
  set("caf.stream.credit-policy", "token-based");
  set("caf.stream.token-based-policy.batch-size", 1);
  set("caf.stream.token-based-policy.buffer-size", 32);
}

} // namespace vast::system
