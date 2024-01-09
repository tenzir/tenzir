//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/writer_command.hpp"

#include "tenzir/concept/parseable/tenzir/expression.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/default_formatter.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/exec_pipeline.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/make_sink.hpp"
#include "tenzir/sink_command.hpp"

#include <caf/actor.hpp>
#include <caf/make_message.hpp>
#include <caf/message.hpp>
#include <caf/settings.hpp>

#include <iostream>
#include <string>

namespace tenzir {

command::fun make_writer_command(std::string_view format) {
  return [format = std::string{format}](const invocation& inv,
                                        caf::actor_system& sys) {
    TENZIR_TRACE_SCOPE("{}", inv);
    if (format == "json") {
      auto printer = make_diagnostic_printer("<input>", "",
                                             color_diagnostics::yes, std::cerr);
      auto const* json_opts = caf::get_if<caf::config_value::dictionary>(
        &inv.options, "tenzir.export.json");
      if (json_opts) {
        for (auto& opt : *json_opts) {
          diagnostic::error("deprecated argument `--{}` no longer available",
                            opt.first)
            .emit(*printer);
          return caf::make_message(ec::silent);
        }
      }
      auto pipe = std::string{"export\n"};
      auto inner = std::string{};
      if (inv.arguments.size() == 1) {
        if (parsers::expr(inv.arguments[0])) {
          pipe += fmt::format("| where {}\n", inv.arguments[0]);
        } else if (not inv.arguments.empty()) {
          pipe += fmt::format("| {}\n", inv.arguments[0]);
        }
      } else if (not inv.arguments.empty()) {
        diagnostic::error("expected at most 1 argument, but got {}",
                          inv.arguments.size())
          .emit(*printer);
        return caf::make_message(ec::silent);
      }
      auto max_events = get_or(inv.options, "tenzir.export.max-events",
                               defaults::export_::max_events);
      pipe += fmt::format("| head {}\n", max_events);
      pipe += "| to stdout write json -c";
      auto flat_pipe = pipe;
      std::replace(flat_pipe.begin(), flat_pipe.end(), '\n', ' ');
      TENZIR_WARN("command `{}` is deprecated: please use `tenzir '{}'` "
                  "instead",
                  inv.full_name, flat_pipe);
      printer = make_diagnostic_printer("<input>", pipe, color_diagnostics::yes,
                                        std::cerr);
      auto result = exec_pipeline(pipe, std::move(printer), exec_config{}, sys);
      if (not result) {
        return make_message(result.error());
      }
      return caf::message{};
    }
    auto snk = make_sink(sys, format, inv.options);
    if (!snk)
      return make_message(snk.error());
    return sink_command(inv, sys, std::move(*snk));
  };
}

} // namespace tenzir
