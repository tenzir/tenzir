//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/config.hpp"

#if VAST_ENABLE_PCAP

#  include "vast/command.hpp"
#  include "vast/defaults.hpp"
#  include "vast/detail/assert.hpp"
#  include "vast/error.hpp"
#  include "vast/format/pcap.hpp"
#  include "vast/logger.hpp"
#  include "vast/scope_linked.hpp"
#  include "vast/system/pcap_writer_command.hpp"
#  include "vast/system/signal_monitor.hpp"
#  include "vast/system/sink.hpp"
#  include "vast/system/sink_command.hpp"
#  include "vast/system/spawn_or_connect_to_node.hpp"

#  include <caf/event_based_actor.hpp>
#  include <caf/scoped_actor.hpp>
#  include <caf/settings.hpp>
#  include <caf/stateful_actor.hpp>
#  include <caf/typed_event_based_actor.hpp>

#  include <string>
#  include <string_view>

namespace vast::system {

caf::message
pcap_writer_command(const invocation& inv, caf::actor_system& sys) {
  VAST_TRACE_SCOPE("{}", inv);
  auto& options = inv.options;
  auto limit = caf::get_or(options, "vast.export.max-events",
                           defaults::export_::max_events);
  auto writer = std::make_unique<format::pcap::writer>(options);
  auto snk = sys.spawn(sink, std::move(writer), limit);
  return sink_command(inv, sys, std::move(snk));
}

} // namespace vast::system

#endif // VAST_ENABLE_PCAP
