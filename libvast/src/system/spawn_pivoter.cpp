//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/spawn_pivoter.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/logger.hpp"
#include "vast/pipeline.hpp"
#include "vast/system/node.hpp"
#include "vast/system/parse_query.hpp"
#include "vast/system/pivoter.hpp"

#include <caf/event_based_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

caf::expected<caf::actor>
spawn_pivoter(node_actor::stateful_pointer<node_state> self,
              spawn_arguments& args) {
  VAST_DEBUG("{}", VAST_ARG(args));
  auto& arguments = args.inv.arguments;
  if (arguments.size() < 2)
    return unexpected_arguments(args);
  auto target_name = arguments[0];
  // Parse given expression.
  auto query_begin = std::next(arguments.begin());
  expression expr;
  auto query_result_ = system::parse_query(query_begin, arguments.end());
  if (!query_result_)
    return query_result_.error();
  if (query_result_->second) {
    return caf::make_error(ec::invalid_configuration,
                           fmt::format("{} failed to spawn pivoter: the "
                                       "provided query must not contain a "
                                       "pipeline",
                                       *self));
  }
  expr = query_result_->first;
  auto handle = self->spawn(pivoter, self, target_name, expr);
  VAST_VERBOSE("{} spawned a pivoter for {}", *self, to_string(expr));
  return handle;
}

} // namespace vast::system
