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

#ifndef VAST_SYSTEM_SPAWN_HPP
#define VAST_SYSTEM_SPAWN_HPP

#include <string>

#include <caf/local_actor.hpp>

#include "vast/expected.hpp"
#include "vast/filesystem.hpp"

#include "vast/system/node_state.hpp"

namespace vast::system {

struct options {
  caf::message params;
  path dir;
  std::string label;
};

expected<caf::actor> spawn_archive(caf::local_actor* self,
                                   options& opts);

expected<caf::actor> spawn_exporter(caf::stateful_actor<node_state>* self,
                                    options& opts);

expected<caf::actor> spawn_importer(caf::stateful_actor<node_state>* self,
                                    options& opts);

expected<caf::actor> spawn_index(caf::local_actor* self, options& opts);

expected<caf::actor> spawn_metastore(caf::local_actor* self, options& opts);

expected<caf::actor> spawn_profiler(caf::local_actor* self, options& opts);

expected<caf::actor> spawn_source(caf::local_actor* self, options& opts);

expected<caf::actor> spawn_sink(caf::local_actor* self, options& opts);

} // namespace vast::system

#endif
