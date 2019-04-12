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

#include "vast/system/spawn_importer.hpp"

#include <caf/actor.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/expected.hpp>
#include <caf/settings.hpp>

#include "vast/defaults.hpp"
#include "vast/detail/unbox_var.hpp"
#include "vast/system/importer.hpp"
#include "vast/system/node.hpp"
#include "vast/system/spawn_arguments.hpp"

namespace vast::system {

maybe_actor spawn_importer(node_actor* self, spawn_arguments& args) {
  if (!args.empty())
    return unexpected_arguments(args);
  // FIXME: Notify exporters with a continuous query.
  return self->spawn(importer, args.dir / args.label,
                     caf::get_or(self->system().config(),
                                 "system.table-slice-size",
                                 defaults::system::table_slice_size));
}

} // namespace vast::system
