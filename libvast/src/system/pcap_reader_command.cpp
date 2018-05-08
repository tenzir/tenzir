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
  add_opt("read,r", "path to input where to read events from", "-");
  add_opt("schema,s", "path to alternate schema", "");
  add_opt("uds,d", "treat -r as listening UNIX domain socket", false);
  add_opt("cutoff,c", "skip flow packets after this many bytes",
          std::numeric_limits<size_t>::max());
  add_opt("flow-max,m", "number of concurrent flows to track",
          uint64_t{1} << 20);
  add_opt("flow-age,a", "max flow lifetime before eviction", 60u);
  add_opt("flow-expiry,e", "flow table expiration interval", 10u);
  add_opt("pseudo-realtime,p", "factor c delaying trace packets by 1/c", 0);
}

expected<caf::actor> pcap_reader_command::make_source(caf::scoped_actor& self,
                                                      const option_map& options,
                                                      argument_iterator begin,
                                                      argument_iterator end) {
  VAST_UNUSED(begin, end);
  VAST_TRACE(VAST_ARG("args", begin, end));
  auto input = get<std::string>(options, "input");
  VAST_ASSERT(input);
  auto cutoff = get<uint64_t>(options, "cutoff");
  VAST_ASSERT(cutoff);
  auto flow_max = get<size_t>(options, "flow-max");
  VAST_ASSERT(flow_max);
  auto flow_age = get<size_t>(options, "flow-age");
  VAST_ASSERT(flow_age);
  auto flow_expiry = get<size_t>(options, "flow-expiry");
  VAST_ASSERT(flow_expiry);
  auto pseudo_realtime = get<int64_t>(options, "pseudo-realtime");
  VAST_ASSERT(pseudo_realtime);
  format::pcap::reader reader{*input,    *cutoff,      *flow_max,
                              *flow_age, *flow_expiry, *pseudo_realtime};
  return self->spawn(source<format::pcap::reader>, std::move(reader));
}

} // namespace vast::system
