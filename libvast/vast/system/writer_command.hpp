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

#pragma once

#include <string>
#include <string_view>
#include <utility>

#include <caf/make_message.hpp>
#include <caf/scoped_actor.hpp>

#include "vast/detail/make_io_stream.hpp"
#include "vast/logger.hpp"
#include "vast/system/sink.hpp"
#include "vast/system/sink_command.hpp"

namespace vast::system {

/// Default implementation for export sub-commands. Compatible with Bro and MRT
/// formats.
template <class Writer>
caf::message writer_command(const command& cmd, caf::actor_system& sys,
                            caf::settings& options,
                            command::argument_iterator first,
                            command::argument_iterator last) {
  VAST_TRACE(VAST_ARG(options), VAST_ARG("args", first, last));
  using ostream_ptr = std::unique_ptr<std::ostream>;
  auto limit = get_or(options, "events", defaults::command::max_events);
  caf::actor snk;
  if constexpr (std::is_constructible_v<Writer, ostream_ptr>) {
    auto output = get_or(options, "write", defaults::command::write_path);
    auto uds = get_or(options, "uds", false);
    auto out = detail::make_output_stream(output, uds);
    if (!out)
      return caf::make_message(out.error());
    Writer writer{std::move(*out)};
    snk = sys.spawn(sink<Writer>, std::move(writer), limit);
  } else {
    Writer writer;
    snk = sys.spawn(sink<Writer>, std::move(writer), limit);
  }
  return sink_command(cmd, sys, std::move(snk), options, first, last);
}

} // namespace vast::system
