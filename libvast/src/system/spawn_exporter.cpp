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

#include <caf/actor.hpp>
#include <caf/expected.hpp>
#include <caf/send.hpp>
#include <caf/settings.hpp>

#include "vast/defaults.hpp"
#include "vast/detail/unbox_var.hpp"
#include "vast/logger.hpp"
#include "vast/query_options.hpp"
#include "vast/system/exporter.hpp"
#include "vast/system/node.hpp"
#include "vast/system/spawn_arguments.hpp"

namespace vast::system {

maybe_actor spawn_exporter(node_actor* self, spawn_arguments& args) {
  VAST_TRACE(VAST_ARG(args));
  // Parse given expression.
  VAST_UNBOX_VAR(expr, normalized_and_validated(args));
  // Parse query options.
  auto query_opts = no_query_options;
  if (get_or(args.inv.options, "export.continuous", false))
    query_opts = query_opts + continuous;
  if (get_or(args.inv.options, "export.unified", false))
    query_opts = unified;
  // Default to historical if no options provided.
  if (query_opts == no_query_options)
    query_opts = historical;
  auto exp = self->spawn(exporter, std::move(expr), query_opts);
  // Setting max-events to 0 means infinite.
  auto max_events = get_or(args.inv.options, "export.max-events",
                           defaults::export_::max_events);
  if (max_events > 0)
    caf::anon_send(exp, atom::extract_v, static_cast<uint64_t>(max_events));
  else
    caf::anon_send(exp, atom::extract_v);
  // Send the running IMPORTERs to the EXPORTER if it handles a continous query.
  if (has_continuous_option(query_opts) && self->state.importer != nullptr)
    self->send(exp, atom::importer_v, std::vector{self->state.importer});
  return exp;
}

} // namespace vast::system
