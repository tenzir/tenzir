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

#include "vast/system/spawn_exporter.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/defaults.hpp"
#include "vast/logger.hpp"
#include "vast/query_options.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/exporter.hpp"
#include "vast/system/node.hpp"
#include "vast/system/spawn_arguments.hpp"

#include <caf/actor.hpp>
#include <caf/expected.hpp>
#include <caf/send.hpp>
#include <caf/settings.hpp>

namespace vast::system {

maybe_actor spawn_exporter(node_actor* self, spawn_arguments& args) {
  VAST_TRACE(VAST_ARG(args));
  // Parse given expression.
  auto expr = system::normalized_and_validated(args);
  if (!expr)
    return expr.error();
  // Parse query options.
  auto query_opts = no_query_options;
  if (get_or(args.inv.options, "vast.export.continuous", false))
    query_opts = query_opts + continuous;
  if (get_or(args.inv.options, "vast.export.unified", false))
    query_opts = unified;
  // Default to historical if no options provided.
  if (query_opts == no_query_options)
    query_opts = historical;
  auto handle = self->spawn(exporter, *expr, query_opts);
  VAST_VERBOSE(self, "spawned an exporter for", to_string(*expr));
  // Wire the exporter to all components.
  if (auto accountant = self->state.registry.find_by_label("accountant"))
    self->send(handle, caf::actor_cast<accountant_type>(accountant));
  if (has_continuous_option(query_opts))
    if (auto importer = self->state.registry.find_by_label("importer"))
      self->send(handle, atom::importer_v, std::vector{importer});
  for (auto& a : self->state.registry.find_by_type("archive")) {
    VAST_DEBUG(self, "connects archive to new exporter");
    self->send(handle, caf::actor_cast<archive_type>(a));
  }
  for (auto& a : self->state.registry.find_by_type("index")) {
    VAST_DEBUG(self, "connects index to new exporter");
    self->send(handle, atom::index_v, a);
  }
  // Setting max-events to 0 means infinite.
  auto max_events = get_or(args.inv.options, "vast.export.max-events",
                           defaults::export_::max_events);
  if (max_events > 0)
    self->send(handle, atom::extract_v, static_cast<uint64_t>(max_events));
  else
    self->send(handle, atom::extract_v);
  return handle;
}

} // namespace vast::system
