//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/spawn_exporter.hpp"

#include "vast/concept/parseable/string/char_class.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/defaults.hpp"
#include "vast/expression.hpp"
#include "vast/legacy_pipeline.hpp"
#include "vast/logger.hpp"
#include "vast/pipeline.hpp"
#include "vast/query_options.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/exporter.hpp"
#include "vast/system/make_legacy_pipelines.hpp"
#include "vast/system/node.hpp"
#include "vast/system/parse_query.hpp"
#include "vast/system/spawn_arguments.hpp"
#include "vast/table_slice.hpp"

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/expected.hpp>
#include <caf/send.hpp>
#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

auto spawn_exporter(node_actor::stateful_pointer<node_state> self,
                    spawn_arguments& args) -> caf::expected<caf::actor> {
  VAST_TRACE_SCOPE("{}", VAST_ARG(args));
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
    auto result = pipeline::parse(query);
    if (!result) {
      if (auto as_expr = pipeline::parse(fmt::format("where {}", query))) {
        VAST_WARN("`vast export <expr>` is deprecated, please use `vast export "
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
  if (get_or(args.inv.options, "vast.export.continuous", false))
    query_opts = query_opts + continuous;
  if (get_or(args.inv.options, "vast.export.unified", false))
    query_opts = unified;
  // Default to historical if no options provided.
  if (query_opts == no_query_options)
    query_opts = historical;
  // Mark the query as low priority if explicitly requested.
  if (get_or(args.inv.options, "vast.export.low-priority", false))
    query_opts = query_opts + low_priority;
  auto [accountant, importer, index]
    = self->state.registry.find<accountant_actor, importer_actor, index_actor>();
  auto handle
    = self->spawn(exporter, query_opts, std::move(pipe), std::move(index));
  VAST_VERBOSE("{} spawned an exporter for '{}'", *self, pipe.to_string());
  // Wire the exporter to all components.
  if (accountant)
    self->send(handle, atom::set_v, accountant);
  if (importer && has_continuous_option(query_opts))
    self
      ->request(importer, caf::infinite,
                static_cast<stream_sink_actor<table_slice>>(handle))
      .then(
        [=](caf::outbound_stream_slot<table_slice>) {
          // nop
        },
        [=, importer = importer](caf::error err) {
          VAST_ERROR("{} failed to connect to importer {}: {}", *self, importer,
                     err);
        });
  return caf::actor_cast<caf::actor>(handle);
}

} // namespace vast::system
