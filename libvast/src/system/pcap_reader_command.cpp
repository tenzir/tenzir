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

#include <memory>
#include <string>
#include <string_view>

#include <caf/scoped_actor.hpp>
#include <caf/typed_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

#include "vast/expression.hpp"
#include "vast/logger.hpp"
#include "vast/defaults.hpp"

#include "vast/system/reader_command_base.hpp"
#include "vast/system/reader_command_base.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/source.hpp"
#include "vast/system/tracker.hpp"

#include "vast/format/pcap.hpp"

#include "vast/concept/parseable/to.hpp"

#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/schema.hpp"

#include "vast/detail/make_io_stream.hpp"

namespace vast::system {

pcap_reader_command::pcap_reader_command(command* parent, std::string_view name)
  : super(parent, name) {
  using namespace vast::defaults;
  add_opt("read,r", "path to input where to read events from",
          pcap_reader_command_read);
  add_opt("schema,s", "path to alternate schema", pcap_reader_command_schema);
  add_opt("uds,d", "treat -r as listening UNIX domain socket",
          pcap_reader_command_uds);
  add_opt("cutoff,c", "skip flow packets after this many bytes",
          pcap_reader_command_cutoff);
  add_opt("flow-max,m", "number of concurrent flows to track",
          pcap_reader_command_flow_max);
  add_opt("flow-age,a", "max flow lifetime before eviction",
          pcap_reader_command_flow_age);
  add_opt("flow-expiry,e", "flow table expiration interval",
          pcap_reader_command_flow_expiry);
  add_opt("pseudo-realtime,p", "factor c delaying trace packets by 1/c",
          pcap_reader_command_pseudo_realtime);
}

expected<caf::actor> pcap_reader_command::make_source(caf::scoped_actor& self,
                                                      const option_map& options,
                                                      argument_iterator begin,
                                                      argument_iterator end) {
  VAST_UNUSED(begin, end);
  VAST_TRACE(VAST_ARG("args", begin, end));
  VAST_DEBUG(VAST_ARG(options));
  using namespace vast::defaults;
  auto input = get_or(options, "read", pcap_reader_command_read);
  auto cutoff = get_or(options, "cutoff", pcap_reader_command_cutoff);
  auto flow_max = get_or(options, "flow-max", pcap_reader_command_flow_max);
  auto flow_age = get_or(options, "flow-age", pcap_reader_command_flow_age);
  auto flow_expiry
    = get_or(options, "flow-expiry", pcap_reader_command_flow_expiry);
  auto pseudo_realtime
    = get_or(options, "pseudo-realtime", pcap_reader_command_pseudo_realtime);
  format::pcap::reader reader{input,    cutoff,      flow_max,
                              flow_age, flow_expiry, pseudo_realtime};
  return self->spawn(source<format::pcap::reader>, std::move(reader));
}

} // namespace vast::system
