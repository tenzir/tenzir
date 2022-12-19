//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/spawn_explorer.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast.hpp"
#include "vast/defaults.hpp"
#include "vast/logger.hpp"
#include "vast/optional.hpp"
#include "vast/si_literals.hpp"
#include "vast/system/explorer.hpp"
#include "vast/system/node.hpp"
#include "vast/system/spawn_arguments.hpp"

#include <caf/actor.hpp>
#include <caf/actor_cast.hpp>
#include <caf/config_value.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/expected.hpp>
#include <caf/local_actor.hpp>
#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <optional>

using namespace std::chrono_literals;

namespace vast::system {

caf::error explorer_validate_args(const caf::settings& args) {
  auto before_arg = caf::get_if<std::string>(&args, "vast.explore.before");
  auto after_arg = caf::get_if<std::string>(&args, "vast.explore.after");
  auto by_arg = caf::get_if(&args, "vast.explore.by");
  if (!before_arg && !after_arg && !by_arg)
    return caf::make_error(ec::invalid_configuration,
                           "At least one of '--before', "
                           "'--after', or '--by' must be "
                           "present.");
  vast::duration before = vast::duration{0s};
  vast::duration after = vast::duration{0s};
  if (before_arg) {
    auto d = to<vast::duration>(*before_arg);
    if (!d)
      return caf::make_error(ec::invalid_argument, "Could not parse",
                             *before_arg, "as duration.");
    before = *d;
  }
  if (after_arg) {
    auto d = to<vast::duration>(*after_arg);
    if (!d)
      return caf::make_error(ec::invalid_argument, "Could not parse",
                             *after_arg, "as duration.");
    after = *d;
  }
  if (!by_arg) {
    if (before <= 0s && after <= 0s)
      return caf::make_error(ec::invalid_argument,
                             "At least one of '--before' or '--after' must be "
                             "greater than 0 "
                             "if no spatial constraint was specified.");
  }
  return caf::none;
}

caf::expected<caf::actor>
spawn_explorer(node_actor::stateful_pointer<node_state> self,
               spawn_arguments& args) {
  if (!args.empty())
    return unexpected_arguments(args);
  if (auto error = explorer_validate_args(args.inv.options))
    return error;
  auto maybe_parse = [](auto&& str) -> std::optional<vast::duration> {
    if (!str)
      return {};
    auto parsed = to<vast::duration>(*str);
    if (!parsed)
      return {};
    return *parsed;
  };
  const auto& options = args.inv.options;
  auto before
    = maybe_parse(caf::get_if<std::string>(&options, "vast.explore.before"));
  auto after
    = maybe_parse(caf::get_if<std::string>(&options, "vast.explore.after"));
  auto by = to_optional(caf::get_if<std::string>(&options, "vast.explore.by"));
  explorer_state::event_limits limits{};
  limits.total = caf::get_or(options, "vast.explore.max-events",
                             defaults::explore::max_events);
  limits.per_result = caf::get_or(options, "vast.explore.max-events-context",
                                  defaults::explore::max_events_context);
  limits.initial_query = caf::get_or(options, "vast.explore.max-events-query",
                                     defaults::explore::max_events_query);
  auto handle = self->spawn(explorer, self, limits, before, after, by);
  VAST_VERBOSE("{} spawned an explorer", *self);
  return handle;
}

} // namespace vast::system
