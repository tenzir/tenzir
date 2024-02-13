//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/spawn_exporter.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/concept/parseable/string/char_class.hpp"
#include "tenzir/concept/parseable/tenzir/expression.hpp"
#include "tenzir/concept/printable/tenzir/expression.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/exporter.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/node.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/query_options.hpp"
#include "tenzir/spawn_arguments.hpp"
#include "tenzir/table_slice.hpp"

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/expected.hpp>
#include <caf/send.hpp>
#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace tenzir {

auto spawn_exporter(node_actor::stateful_pointer<node_state> self,
                    spawn_arguments& args) -> caf::expected<caf::actor> {
  TENZIR_TRACE_SCOPE("{}", TENZIR_ARG(args));
  auto parse_result = std::invoke([&]() -> caf::expected<pipeline> {
    if (args.inv.arguments.empty()) {
      return pipeline{};
    }
    if (args.inv.arguments.size() != 1) {
      return caf::make_error(ec::invalid_argument,
                             "exporter expected at most 1 argument, but got {}",
                             args.inv.arguments.size());
    }
    auto& query = args.inv.arguments[0];
    auto result = pipeline::internal_parse(query);
    if (!result) {
      if (auto as_expr
          = pipeline::internal_parse(fmt::format("where {}", query))) {
        TENZIR_WARN(
          "`tenzir export <expr>` is deprecated, please use `tenzir export "
          "'where <expr>'` instead");
        result = std::move(as_expr);
      }
    }
    return result;
  });
  if (!parse_result) {
    return std::move(parse_result.error());
  }
  auto pipe = std::move(*parse_result);
  // Parse query options.
  auto query_opts = no_query_options;
  if (get_or(args.inv.options, "tenzir.export.continuous", false))
    query_opts = query_opts + continuous;
  if (get_or(args.inv.options, "tenzir.export.unified", false))
    query_opts = unified;
  // Default to historical if no options provided.
  if (query_opts == no_query_options)
    query_opts = historical;
  // Mark the query as low priority if explicitly requested.
  if (get_or(args.inv.options, "tenzir.export.low-priority", false))
    query_opts = query_opts + low_priority;
  auto [accountant, importer, index]
    = self->state.registry.find<accountant_actor, importer_actor, index_actor>();
  auto handle
    = self->spawn(exporter, query_opts, std::move(pipe), std::move(index));
  TENZIR_VERBOSE("{} spawned an exporter for '{:?}'", *self, pipe);
  // Wire the exporter to all components.
  if (accountant)
    self->send(handle, atom::set_v, accountant);
  if (importer && has_continuous_option(query_opts))
    self
      ->request(importer, caf::infinite,
                static_cast<stream_sink_actor<table_slice>>(handle))
      .then(
        [=]() {
          // nop
        },
        [=, importer = importer](caf::error err) {
          TENZIR_ERROR("{} failed to connect to importer {}: {}", *self,
                       importer, err);
        });
  return caf::actor_cast<caf::actor>(handle);
}

} // namespace tenzir
