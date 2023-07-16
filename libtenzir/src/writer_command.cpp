//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/writer_command.hpp"

#include "tenzir/logger.hpp"
#include "tenzir/make_sink.hpp"
#include "tenzir/sink_command.hpp"

#include <caf/actor.hpp>
#include <caf/make_message.hpp>

#include <string>

namespace tenzir {

command::fun make_writer_command(std::string_view format) {
  return [format = std::string{format}](const invocation& inv,
                                        caf::actor_system& sys) {
    TENZIR_TRACE_SCOPE("{}", inv);
    auto snk = make_sink(sys, format, inv.options);
    if (!snk)
      return make_message(snk.error());
    return sink_command(inv, sys, std::move(*snk));
  };
}

} // namespace tenzir
