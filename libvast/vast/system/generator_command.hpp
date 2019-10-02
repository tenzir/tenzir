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

#pragma once

#include <random>
#include <string>
#include <string_view>
#include <utility>

#include <caf/scoped_actor.hpp>

#include "vast/defaults.hpp"
#include "vast/logger.hpp"
#include "vast/system/source.hpp"
#include "vast/system/source_command.hpp"

namespace vast::system {

/// Default implementation for import sub-commands. Compatible with Bro and MRT
/// formats.
/// @relates application
template <class Generator, class Defaults>
caf::message generator_command(const command::invocation& invocation,
                               caf::actor_system& sys) {
  VAST_TRACE("");
  std::string category = Defaults::category;
  auto table_slice
    = defaults::import::table_slice_type(sys, invocation.options);
  auto num = get_or(invocation.options, "import.max-events",
                    defaults::import::max_events);
  auto schema = get_schema(invocation.options, category);
  if (!schema)
    return caf::make_message(schema.error());
  auto seed = Defaults::seed(invocation.options);
  Generator generator{table_slice, seed, num};
  if (auto err = generator.schema(*schema))
    return caf::make_message(err);
  auto src = sys.spawn(default_source<Generator>, std::move(generator));
  return source_command(invocation, sys, std::move(src));
}

} // namespace vast::system
