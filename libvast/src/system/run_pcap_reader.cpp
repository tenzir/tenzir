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

#include "vast/system/run_pcap_reader.hpp"

#include <memory>
#include <string>
#include <string_view>

#include <caf/scoped_actor.hpp>
#include <caf/typed_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

#include "vast/expression.hpp"
#include "vast/logger.hpp"

#include "vast/system/run_reader_base.hpp"
#include "vast/system/run_reader_base.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/source.hpp"
#include "vast/system/tracker.hpp"

#include "vast/format/pcap.hpp"

#include "vast/concept/parseable/to.hpp"

#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/schema.hpp"

#include "vast/detail/make_io_stream.hpp"

namespace vast::system {

run_pcap_reader::run_pcap_reader(command* parent, std::string_view name)
  : super(parent, name),
    uds(false),
    flow_max(uint64_t{1} << 20),
    flow_age(60u),
    flow_expiry(10u),
    cutoff(std::numeric_limits<size_t>::max()),
    pseudo_realtime(0) {
  add_opt("read,r", "path to input where to read events from", input);
  add_opt("schema,s", "path to alternate schema", schema_file);
  add_opt("uds,d", "treat -r as listening UNIX domain socket", uds);
  add_opt("cutoff,c", "skip flow packets after this many bytes", cutoff);
  add_opt("flow-max,m", "number of concurrent flows to track", flow_max);
  add_opt("flow-age,a", "max flow lifetime before eviction", flow_age);
  add_opt("flow-expiry,e", "flow table expiration interval", flow_expiry);
  add_opt("pseudo-realtime,p", "factor c delaying trace packets by 1/c",
          pseudo_realtime);
}

expected<caf::actor> run_pcap_reader::make_source(caf::scoped_actor& self,
                                                  caf::message args) {
  CAF_LOG_TRACE(CAF_ARG(args));
  format::pcap::reader reader{input,    cutoff,      flow_max,
                              flow_age, flow_expiry, pseudo_realtime};
  return self->spawn(source<format::pcap::reader>, std::move(reader));
}

} // namespace vast::system
