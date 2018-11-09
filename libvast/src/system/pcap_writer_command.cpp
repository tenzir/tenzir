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

#include "vast/system/pcap_writer_command.hpp"

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
#include "vast/system/sink.hpp"
#include "vast/system/sink_command.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"

namespace vast::system {

caf::message pcap_writer_command(const command& cmd, caf::actor_system& sys,
                                 caf::config_value_map& options,
                                 command::argument_iterator first,
                                 command::argument_iterator last) {
  using caf::get_or;
  VAST_TRACE(VAST_ARG("args", begin, end));
  auto limit = get_or(options, "events", defaults::command::max_events);
  auto output = get_or(options, "write", defaults::command::write_path);
  auto flush = get_or(options, "flush", defaults::command::flush_interval);
  format::pcap::writer writer{output, flush};
  auto snk = sys.spawn(sink<format::pcap::writer>, std::move(writer), limit);
  return sink_command(cmd, sys, std::move(snk), options, first, last);
}

} // namespace vast::system
