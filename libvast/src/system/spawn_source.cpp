//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/spawn_source.hpp"

#include "vast/format/reader.hpp"
#include "vast/logger.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/make_pipelines.hpp"
#include "vast/system/make_source.hpp"
#include "vast/system/parse_query.hpp"
#include "vast/system/spawn_arguments.hpp"
#include "vast/uuid.hpp"

#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

caf::expected<caf::actor>
spawn_source(node_actor::stateful_pointer<node_state> self,
             spawn_arguments& args) {
  VAST_TRACE_SCOPE("{} {}", VAST_ARG("node", *self), VAST_ARG(args));
  const auto& options = args.inv.options;
  // Bail out early for bogus invocations.
  if (caf::get_or(options, "vast.node", false))
    return caf::make_error(ec::invalid_configuration,
                           "unable to spawn a remote source when spawning a "
                           "node locally instead of connecting to one; please "
                           "unset the option vast.node");
  auto pipelines
    = make_pipelines(pipelines_location::server_import, args.inv.options);
  if (!pipelines)
    return pipelines.error();
  if (!args.inv.arguments.empty()) {
    auto parse_result = parse_query(args.inv.arguments);
    if (!parse_result) {
      return parse_result.error();
    }
    auto [expr, pipeline] = std::move(*parse_result);
    if (pipeline) {
      pipelines->push_back(std::move(*pipeline));
    }
  }
  VAST_DEBUG("{} parsed {} pipelines for source", *self, pipelines->size());
  auto [accountant, importer, catalog]
    = self->state.registry
        .find<accountant_actor, importer_actor, catalog_actor>();
  if (!importer)
    return caf::make_error(ec::missing_component, "importer");
  if (!catalog)
    return caf::make_error(ec::missing_component, "catalog");
  const auto format = std::string{args.inv.name()};
  auto src_result = make_source(self->system(), format, args.inv,
                                caf::actor_cast<accountant_actor>(accountant),
                                caf::actor_cast<catalog_actor>(catalog),
                                caf::actor_cast<importer_actor>(importer),
                                std::move(*pipelines), true);
  if (!src_result)
    return src_result.error();
  auto src = *src_result;
  VAST_INFO("{} spawned a {} source", *self, format);
  src->attach_functor([=](const caf::error& reason) {
    if (!reason || reason == caf::exit_reason::user_shutdown)
      VAST_INFO("{} source shut down", format);
    else
      VAST_WARN("{} source shut down with error: {}", format, reason);
  });
  return src;
}

} // namespace vast::system
