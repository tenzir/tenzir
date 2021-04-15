//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/spawn_exporter.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/defaults.hpp"
#include "vast/logger.hpp"
#include "vast/query_options.hpp"
#include "vast/system/exporter.hpp"
#include "vast/system/node.hpp"
#include "vast/system/spawn_arguments.hpp"

#include <caf/actor.hpp>
#include <caf/expected.hpp>
#include <caf/send.hpp>
#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

caf::expected<caf::actor>
spawn_exporter(node_actor::stateful_pointer<node_state> self,
               spawn_arguments& args) {
  VAST_TRACE_SCOPE("{}", VAST_ARG(args));
  // Parse given expression.
  auto expr = get_expression(args);
  if (!expr)
    return expr.error();
  // Parse query options.
  auto query_opts = no_query_options;
  if (get_or(args.inv.options, "vast.export.continuous", false))
    query_opts = query_opts | continuous;
  if (get_or(args.inv.options, "vast.export.historical-with-ids", false))
    query_opts = query_opts | historical_with_ids;
  if (get_or(args.inv.options, "vast.export.unified", false)) {
    if (has_historical_with_ids_option(query_opts))
      query_opts = query_opts | continuous;
    else
      query_opts = unified;
  }
  // Default to historical if no options provided.
  if (query_opts == no_query_options)
    query_opts = historical;
  auto handle = self->spawn(exporter, *expr, query_opts);
  VAST_VERBOSE("{} spawned an exporter for {}", self, to_string(*expr));
  // Wire the exporter to all components.
  auto [accountant, importer, index]
    = self->state.registry.find<accountant_actor, importer_actor, index_actor>();
  if (accountant)
    self->send(handle, accountant);
  if (importer && has_continuous_option(query_opts))
    self
      ->request(importer, caf::infinite,
                static_cast<stream_sink_actor<table_slice>>(handle))
      .then(
        [=](caf::outbound_stream_slot<table_slice>) {
          // nop
        },
        [=, importer = importer](caf::error err) {
          VAST_ERROR("{} failed to connect to importer {}: {}", self, importer,
                     err);
        });
  if (index) {
    VAST_DEBUG("{} connects index to new exporter", self);
    self->send(handle, index);
  }
  // Setting max-events to 0 means infinite.
  auto max_events = get_or(args.inv.options, "vast.export.max-events",
                           defaults::export_::max_events);
  if (max_events > 0)
    self->send(handle, atom::extract_v, static_cast<uint64_t>(max_events));
  else
    self->send(handle, atom::extract_v);
  return caf::actor_cast<caf::actor>(handle);
}

} // namespace vast::system
