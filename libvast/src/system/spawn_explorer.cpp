/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/system/spawn_explorer.hpp"

#include "vast/defaults.hpp"
#include "vast/filesystem.hpp"
#include "vast/si_literals.hpp"
#include "vast/system/explorer.hpp"
#include "vast/system/node.hpp"
#include "vast/system/spawn_arguments.hpp"

#include <caf/actor.hpp>
#include <caf/actor_cast.hpp>
#include <caf/config_value.hpp>
#include <caf/expected.hpp>
#include <caf/local_actor.hpp>
#include <caf/settings.hpp>

#include <optional>

using namespace std::chrono_literals;

namespace vast::system {

caf::error explorer_validate_args(const caf::settings& args) {
  auto before_arg = caf::get_if<caf::timespan>(&args, "explore.before");
  auto after_arg = caf::get_if<caf::timespan>(&args, "explore.after");
  auto by_arg = caf::get_if(&args, "explore.by");
  if (!before_arg && !after_arg && !by_arg)
    return make_error(ec::invalid_configuration, "At least one of '--before', "
                                                 "'--after', or '--by' must be "
                                                 "present.");
  if (!by_arg) {
    auto before = before_arg ? *before_arg : vast::duration{0s};
    auto after = after_arg ? *after_arg : vast::duration{0s};
    if (before <= 0s && after <= 0s)
      return make_error(ec::invalid_configuration,
                        "At least one of '--before' or '--after' must be "
                        "greater than 0 "
                        "if no spatial constraint was specified.");
  }
  return caf::none;
}

maybe_actor spawn_explorer(node_actor* self, spawn_arguments& args) {
  if (!args.empty())
    return unexpected_arguments(args);
  if (auto error = explorer_validate_args(args.invocation.options))
    return error;
  std::optional<vast::duration> before = to_std(
    caf::get_if<caf::timespan>(&args.invocation.options, "explore.before"));
  std::optional<vast::duration> after = to_std(
    caf::get_if<caf::timespan>(&args.invocation.options, "explore.after"));
  std::optional<std::string> by
    = to_std(caf::get_if<std::string>(&args.invocation.options, "explore.by"));
  auto expl = self->spawn(explorer, self, before, after, by);
  return expl;
}

} // namespace vast::system
