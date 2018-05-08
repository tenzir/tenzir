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

#include <memory>
#include <string>
#include <string_view>

#include <caf/scoped_actor.hpp>
#include <caf/typed_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

#include "vast/expression.hpp"
#include "vast/logger.hpp"

#include "vast/system/sink.hpp"

#include "vast/format/pcap.hpp"

namespace vast::system {

pcap_writer_command::pcap_writer_command(command* parent, std::string_view name)
  : super(parent, name) {
  add_opt("write,w", "path to write events to", "-");
  add_opt("uds,d", "treat -w as UNIX domain socket to connect to", false);
  add_opt("flush,f", "flush to disk after this many packets", 10000u);
}

expected<caf::actor> pcap_writer_command::make_sink(caf::scoped_actor& self,
                                                    option_map& options,
                                                    argument_iterator begin,
                                                    argument_iterator end) {
  VAST_UNUSED(begin, end);
  VAST_TRACE(VAST_ARG("args", begin, end));
  auto limit = get<uint64_t>(options, "events");
  VAST_ASSERT(limit);
  auto output = get<std::string>(options, "write");
  VAST_ASSERT(output);
  auto flush = get<unsigned>(options, "flush");
  VAST_ASSERT(flush);
  format::pcap::writer writer{*output, *flush};
  return self->spawn(sink<format::pcap::writer>, std::move(writer), *limit);
}

} // namespace vast::system
