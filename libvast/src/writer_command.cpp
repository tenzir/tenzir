//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/writer_command.hpp"

#include "vast/logger.hpp"
#include "vast/make_sink.hpp"
#include "vast/sink_command.hpp"

#include <caf/actor.hpp>
#include <caf/make_message.hpp>

#include <string>

namespace vast {

command::fun make_writer_command(std::string_view format) {
  return [format = std::string{format}](const invocation& inv,
                                        caf::actor_system& sys) {
    VAST_TRACE_SCOPE("{}", inv);
    auto snk = make_sink(sys, format, inv.options);
    if (!snk)
      return make_message(snk.error());
    return sink_command(inv, sys, std::move(*snk));
  };
}

} // namespace vast
