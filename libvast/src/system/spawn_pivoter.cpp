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
#include "vast/system/node.hpp"
#include "vast/system/pivoter.hpp"
#include "vast/system/spawn_arguments.hpp"

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
  auto expr_ = system::parse_expression(query_begin, arguments.end());
  if (!expr_)
    return expr_.error();
  expr = *expr_;
  auto handle = self->spawn(pivoter, self, target_name, expr);
  VAST_VERBOSE("{} spawned a pivoter for {}", *self, to_string(expr));
  return handle;
}

} // namespace vast::system
