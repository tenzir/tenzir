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
template <class Generator>
caf::message generator_command(const command& cmd, caf::actor_system& sys,
                               caf::settings& options,
                               command::argument_iterator first,
                               command::argument_iterator last) {
  VAST_TRACE("");
  auto global_table_slice = get_or(sys.config(), "vast.table-slice-type",
                                   defaults::system::table_slice_type);
  auto table_slice = get_or(options, "table-slice", global_table_slice);
  auto num = get_or(options, "num", defaults::command::generated_events);
  auto seed_opt = caf::get_if<size_t>(&options, "seed");
  auto seed = seed_opt ? *seed_opt : std::random_device{}();
  Generator generator{table_slice, seed, num};
  auto src = sys.spawn(default_source<Generator>, std::move(generator));
  return source_command(cmd, sys, std::move(src), options, first, last);
}

} // namespace vast::system
