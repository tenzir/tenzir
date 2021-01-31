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

#include "vast/system/spawn_eraser.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/defaults.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/system/eraser.hpp"
#include "vast/system/node.hpp"
#include "vast/system/spawn_arguments.hpp"

#include <caf/settings.hpp>

namespace vast::system {

maybe_actor
spawn_eraser(system::node_actor* self, system::spawn_arguments& args) {
  using namespace std::string_literals;
  VAST_TRACE("{}  {}", detail::id_or_name(VAST_ARG(self)), VAST_ARG(args));
  // Parse options.
  auto eraser_query = caf::get_or(args.inv.options, "vast.aging-query", ""s);
  if (eraser_query.empty()) {
    VAST_VERBOSE("{} has no aging-query and skips starting the eraser",
                 detail::id_or_name(self));
    return ec::no_error;
  }
  if (auto expr = to<expression>(eraser_query); !expr) {
    VAST_WARN("{} got an invalid aging-query {}", detail::id_or_name(self),
              eraser_query);
    return expr.error();
  }
  auto aging_frequency = defaults::system::aging_frequency;
  if (auto str = caf::get_if<std::string>(&args.inv.options, "vast.aging-"
                                                             "frequency")) {
    auto parsed = to<duration>(*str);
    if (!parsed)
      return parsed.error();
    aging_frequency = *parsed;
  }
  // Ensure component dependencies.
  auto [index, archive]
    = self->state.registry.find<index_actor, archive_actor>();
  if (!index)
    return caf::make_error(ec::missing_component, "index");
  if (!archive)
    return caf::make_error(ec::missing_component, "archive");
  // Spawn the eraser.
  auto handle
    = self->spawn(eraser, aging_frequency, eraser_query, index, archive);
  VAST_VERBOSE("{} spawned an eraser for {}", detail::id_or_name(self),
               eraser_query);
  return handle;
}

} // namespace vast::system
