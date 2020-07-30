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
#include <caf/settings.hpp>

#include "vast/defaults.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/logger.hpp"
#include "vast/system/sink.hpp"
#include "vast/system/sink_command.hpp"

namespace vast::system {

/// Default implementation for export sub-commands. Compatible with Bro and MRT
/// formats.
template <class Writer, class Defaults = typename Writer::defaults>
caf::message writer_command(const invocation& inv, caf::actor_system& sys) {
  VAST_TRACE(inv);
  auto& options = inv.options;
  std::string category = Defaults::category;
  using ostream_ptr = std::unique_ptr<std::ostream>;
  auto max_events
    = get_or(options, "export.max-events", defaults::export_::max_events);
  caf::actor snk;
  if constexpr (std::is_constructible_v<Writer, ostream_ptr>) {
    auto out = detail::make_output_stream<Defaults>(options);
    if (!out)
      return caf::make_message(out.error());
    Writer writer{std::move(*out)};
    snk = sys.spawn(sink<Writer>, std::move(writer), max_events);
  } else {
    Writer writer;
    snk = sys.spawn(sink<Writer>, std::move(writer), max_events);
  }
  return sink_command(inv, sys, std::move(snk));
}

} // namespace vast::system
