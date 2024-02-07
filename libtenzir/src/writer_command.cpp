//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/writer_command.hpp"

#include "tenzir/detail/default_formatter.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/exec_pipeline.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/make_sink.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/sink_command.hpp"
#include "tenzir/tql/parser.hpp"

#include <caf/actor.hpp>
#include <caf/make_message.hpp>
#include <caf/message.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>

#include <iostream>
#include <string>

namespace tenzir {

command::fun make_writer_command(std::string_view format) {
  return [format = std::string{format}](const invocation& inv,
                                        caf::actor_system& sys) {
    TENZIR_WARN("`tenzir-ctl export` is deprecated, please use `tenzir 'export "
                "| ...' instead`");
    TENZIR_TRACE_SCOPE("{}", inv);
    if (format == "json") {
      auto printer = make_diagnostic_printer(std::nullopt,
                                             color_diagnostics::yes, std::cerr);
      auto const* json_opts = caf::get_if<caf::config_value::dictionary>(
        &inv.options, "tenzir.export.json");
      auto pipe = fmt::format(
        "export{}\n", get_or(inv.options, "tenzir.export.low-priority", false)
                        ? " --low-priority"
                        : "");
      auto inner = std::string{};
      if (inv.arguments.size() == 1) {
        if (tql::parse_internal(inv.arguments[0])) {
          pipe += fmt::format("| {}\n", inv.arguments[0]);
        } else if (not inv.arguments.empty()) {
          pipe += fmt::format("| where {}\n", inv.arguments[0]);
        }
      } else if (not inv.arguments.empty()) {
        diagnostic::error("expected at most 1 argument, but got {}",
                          inv.arguments.size())
          .emit(*printer);
        return caf::make_message(ec::silent);
      }
      if (auto const* max_events
          = get_if(&inv.options, "tenzir.export.max-events")) {
        pipe += fmt::format("| head {}\n", *max_events);
      }
      pipe += "| to stdout write json -c";
      if (json_opts) {
        for (auto const& [name, value] : *json_opts) {
          if (not value.to_boolean()) {
            continue;
          }
          if (name == "omit-nulls") {
            pipe += " --omit-nulls";
          } else if (name == "omit-empty-records") {
            pipe += " --omit-empty-objects";
          } else if (name == "omit-empty-lists") {
            pipe += " --omit-empty-lists";
          } else if (name == "omit-empty") {
            pipe += " --omit-empty";
          } else {
            diagnostic::error("deprecated argument `--{}` no longer available",
                              name)
              .emit(*printer);
            return caf::make_message(ec::silent);
          }
        }
      }
      printer = make_diagnostic_printer(location_origin{"<input>", pipe},
                                        color_diagnostics::yes, std::cerr);
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
