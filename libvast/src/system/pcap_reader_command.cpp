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

#include <caf/scoped_actor.hpp>

#include "vast/defaults.hpp"
#include "vast/logger.hpp"

#include "vast/format/pcap.hpp"

#include "vast/system/source.hpp"

namespace vast::system {

pcap_reader_command::pcap_reader_command(command* parent, std::string_view name)
  : super(parent, name) {
  add_opt<std::string>("read,r", "path to input where to read events from");
  add_opt<std::string>("schema,s", "path to alternate schema");
  add_opt<bool>("uds,d", "treat -r as listening UNIX domain socket");
  add_opt<size_t>("cutoff,c", "skip flow packets after this many bytes");
  add_opt<size_t>("flow-max,m", "number of concurrent flows to track");
  add_opt<size_t>("flow-age,a", "max flow lifetime before eviction");
  add_opt<size_t>("flow-expiry,e", "flow table expiration interval");
  add_opt<size_t>("pseudo-realtime,p", "factor c delaying packets by 1/c");
}

expected<caf::actor>
pcap_reader_command::make_source(caf::scoped_actor& self,
                                 const caf::config_value_map& options,
                                 argument_iterator begin,
                                 argument_iterator end) {
  VAST_UNUSED(begin, end);
  VAST_TRACE(VAST_ARG("args", begin, end));
  VAST_DEBUG(VAST_ARG(options));
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
  return self->spawn(default_source<format::pcap::reader>, std::move(reader));
}

} // namespace vast::system
