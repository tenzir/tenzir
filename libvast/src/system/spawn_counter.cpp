//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/spawn_counter.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/defaults.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/pipeline.hpp"
#include "vast/system/counter.hpp"
#include "vast/system/node.hpp"
#include "vast/system/node_control.hpp"

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
  auto expr = trivially_true_expression();
  if (not args.inv.arguments.empty()) {
    if (args.inv.arguments.size() > 1) {
      return caf ::make_error(ec::invalid_argument,
                              fmt::format("expected at most one argument, but "
                                          "got [{}]",
                                          fmt::join(args.inv.arguments, ", ")));
    }
    if (not args.inv.arguments[0].empty()) {
      auto parse_result = to<expression>(args.inv.arguments[0]);
      if (!parse_result)
        return caf::make_error(
          ec::parse_error,
          fmt::format("failed to parse expression '{}': {}",
                      args.inv.arguments[0], parse_result.error()));
      parse_result = normalize_and_validate(std::move(*parse_result));
      if (!parse_result)
        return parse_result.error();
      expr = std::move(*parse_result);
    }
  }
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
