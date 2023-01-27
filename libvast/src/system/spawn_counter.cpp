//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/spawn_counter.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/defaults.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/pipeline.hpp"
#include "vast/system/counter.hpp"
#include "vast/system/node.hpp"
#include "vast/system/node_control.hpp"
#include "vast/system/parse_query.hpp"

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/expected.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>
#include <caf/settings.hpp>

namespace vast::system {

caf::expected<caf::actor>
spawn_counter(node_actor::stateful_pointer<node_state> self,
              spawn_arguments& args) {
  VAST_TRACE_SCOPE("{}", VAST_ARG(args));
  // Parse given expression.
  auto query_result = parse_query(args);
  if (!query_result)
    return query_result.error();
  if (query_result->second) {
    return caf::make_error(ec::invalid_configuration,
                           fmt::format("{} failed to spawn counter: the "
                                       "provided query must not contain a "
                                       "pipeline",
                                       *self));
  }
  auto expr = query_result->first;
  // pipeline should be impossible here.
  auto [index] = self->state.registry.find<index_actor>();
  if (!index)
    return caf::make_error(ec::missing_component, "index");
  auto estimate = caf::get_or(args.inv.options, "vast.count.estimate", false);
  auto handle = self->spawn(counter, expr, index, estimate);
  VAST_VERBOSE("{} spawned a counter for {}", *self, to_string(expr));
  return handle;
}

} // namespace vast::system
