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

#include "vast/system/pcap_reader_command.hpp"

#include <string>
#include <string_view>

#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/error.hpp"
#include "vast/format/pcap.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/source.hpp"
#include "vast/system/source_command.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"

namespace vast::system {

caf::message pcap_reader_command(const command& cmd, caf::actor_system& sys,
                                 caf::config_value_map& options,
                                 command::argument_iterator first,
                                 command::argument_iterator last) {
  VAST_TRACE(VAST_ARG(options), VAST_ARG("args", first, last));
  auto input = get_or(options, "read", defaults::command::read_path);
  auto cutoff = get_or(options, "cutoff", defaults::command::cutoff);
  auto flow_max = get_or(options, "flow-max", defaults::command::max_flows);
  auto flow_age = get_or(options, "flow-age", defaults::command::max_flow_age);
  auto flow_expiry = get_or(options, "flow-expiry",
                            defaults::command::flow_expiry);
  auto pseudo_realtime = get_or(options, "pseudo-realtime",
                                defaults::command::pseudo_realtime_factor);
  format::pcap::reader reader{input,    cutoff,      flow_max,
                              flow_age, flow_expiry, pseudo_realtime};
  auto src = sys.spawn(default_source<format::pcap::reader>, std::move(reader));
  return source_command(cmd, sys, std::move(src), options, first, last);
}

} // namespace vast::system
