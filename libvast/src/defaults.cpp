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

#include "vast/defaults.hpp"

#include <caf/actor_system.hpp>        // for actor_system
#include <caf/actor_system_config.hpp> // for get_or
#include <caf/settings.hpp>            // for get_if, settings

#include <random> // for random_device
#include <string> // for string

namespace vast::defaults::import {

caf::atom_value table_slice_type(const caf::actor_system& sys,
                                 const caf::settings& options) {
  // The parameter import.table-slice-type overrides system.table-slice-type.
  if (auto val = caf::get_if<caf::atom_value>(&options,
                                              "import.table-slice-type"))
    return *val;
  return get_or(sys.config(), "system.table-slice-type",
                system::table_slice_type);
}

size_t test::seed(const caf::settings& options) {
  std::string cat = category;
  if (auto val = caf::get_if<size_t>(&options, cat + ".seed"))
    return *val;
  std::random_device rd;
  return rd();
}

} // namespace vast::defaults::import
