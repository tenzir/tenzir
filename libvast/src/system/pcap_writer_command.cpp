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

#include "vast/defaults.hpp"
#include "vast/expression.hpp"
#include "vast/format/pcap.hpp"
#include "vast/logger.hpp"
#include "vast/system/sink.hpp"

using std::string;

namespace vast::system {

pcap_writer_command::pcap_writer_command(command* parent, std::string_view name)
  : super(parent, name) {
  add_opt<string>("write,w", "path to write events to");
  add_opt<bool>("uds,d", "treat -w as UNIX domain socket to connect to");
  add_opt<size_t>("flush,f", "flush to disk after this many packets");
}

expected<caf::actor>
pcap_writer_command::make_sink(caf::scoped_actor& self,
                               const caf::config_value_map& options,
                               argument_iterator begin,
                               argument_iterator end) {
  VAST_UNUSED(begin, end);
  VAST_TRACE(VAST_ARG("args", begin, end));
  auto limit = get_or(options, "events", defaults::command::max_events);
  auto output = get_or(options, "write", defaults::command::write_path);
  auto flush = get_or(options, "flush", defaults::command::flush_interval);
  format::pcap::writer writer{output, flush};
  return self->spawn(sink<format::pcap::writer>, std::move(writer), limit);
}

} // namespace vast::system
