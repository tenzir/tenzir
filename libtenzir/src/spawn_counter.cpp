//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/spawn_counter.hpp"

#include "tenzir/concept/parseable/tenzir/expression.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/concept/printable/tenzir/expression.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/counter.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/node.hpp"
#include "tenzir/node_control.hpp"
#include "tenzir/pipeline.hpp"

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/expected.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>
#include <caf/settings.hpp>

namespace tenzir {

caf::expected<caf::actor>
spawn_counter(node_actor::stateful_pointer<node_state> self,
              spawn_arguments& args) {
  TENZIR_TRACE_SCOPE("{}", TENZIR_ARG(args));
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
  auto estimate = caf::get_or(args.inv.options, "tenzir.count.estimate", false);
  auto handle = self->spawn(counter, expr, index, estimate);
  TENZIR_VERBOSE("{} spawned a counter for {}", *self, to_string(expr));
  return handle;
}

} // namespace tenzir
