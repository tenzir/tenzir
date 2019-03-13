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

#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/settings.hpp>

namespace vast::defaults::command {

caf::atom_value table_slice_type(caf::actor_system& sys,
                                 caf::settings& options) {
  if (auto val = caf::get_if<caf::atom_value>(&options, "table-slice"))
    return *val;
  return get_or(sys.config(), "vast.table-slice-type",
                system::table_slice_type);
}

} // namespace vast::defaults::command
