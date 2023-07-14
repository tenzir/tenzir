//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/spawn_eraser.hpp"

#include "tenzir/concept/parseable/tenzir/expression.hpp"
#include "tenzir/concept/parseable/tenzir/time.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/eraser.hpp"
#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/node.hpp"
#include "tenzir/spawn_arguments.hpp"

#include <caf/event_based_actor.hpp>
#include <caf/settings.hpp>

namespace tenzir {

caf::expected<caf::actor>
spawn_eraser(node_actor::stateful_pointer<node_state> self,
             spawn_arguments& args) {
  using namespace std::string_literals;
  TENZIR_TRACE_SCOPE("{} {}", TENZIR_ARG(*self), TENZIR_ARG(args));
  // Parse options.
  auto eraser_query = caf::get_or(args.inv.options, "tenzir.aging-query", ""s);
  if (eraser_query.empty()) {
    TENZIR_VERBOSE("{} has no aging-query and skips starting the eraser",
                   *self);
    return ec::no_error;
  }
  if (auto expr = to<expression>(eraser_query); !expr) {
    TENZIR_WARN("{} got an invalid aging-query {}", *self, eraser_query);
    return expr.error();
  }
  auto aging_frequency = defaults::aging_frequency;
  if (auto str = caf::get_if<std::string>(&args.inv.options, "tenzir.aging-"
                                                             "frequency")) {
    auto parsed = to<duration>(*str);
    if (!parsed)
      return parsed.error();
    aging_frequency = *parsed;
  }
  // Ensure component dependencies.
  auto [index] = self->state.registry.find<index_actor>();
  if (!index)
    return caf::make_error(ec::missing_component, "index");
  // Spawn the eraser.
  auto handle = self->spawn(eraser, aging_frequency, eraser_query, index);
  TENZIR_VERBOSE("{} spawned an eraser for {}", *self, eraser_query);
  return caf::actor_cast<caf::actor>(handle);
}

} // namespace tenzir
