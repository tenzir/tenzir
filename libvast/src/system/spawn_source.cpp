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
#include "vast/system/make_source.hpp"
#include "vast/system/make_transforms.hpp"
#include "vast/system/spawn_arguments.hpp"

#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

caf::expected<caf::actor>
spawn_source(node_actor::stateful_pointer<node_state> self,
             spawn_arguments& args) {
  VAST_TRACE_SCOPE("{} {}", VAST_ARG("node", self), VAST_ARG(args));
  const auto& options = args.inv.options;
  // Bail out early for bogus invocations.
  if (caf::get_or(options, "vast.node", false))
    return caf::make_error(ec::invalid_configuration,
                           "unable to spawn a remote source when spawning a "
                           "node locally instead of connecting to one; please "
                           "unset the option vast.node");
  auto transforms
    = make_transforms(transforms_location::server_import, args.inv.options);
  if (!transforms)
    return transforms.error();
  VAST_DEBUG("{} parsed {} transforms for source", self, transforms->size());
  auto [accountant, importer, type_registry]
    = self->state.registry
        .find<accountant_actor, importer_actor, type_registry_actor>();
  if (!importer)
    return caf::make_error(ec::missing_component, "importer");
  if (!type_registry)
    return caf::make_error(ec::missing_component, "type-registry");
  const auto format = std::string{args.inv.name()};
  auto src_result
    = make_source(self->system(), format, args.inv,
                  caf::actor_cast<accountant_actor>(accountant),
                  caf::actor_cast<type_registry_actor>(type_registry),
                  caf::actor_cast<importer_actor>(importer), std::nullopt,
                  std::move(*transforms), true);
  if (!src_result)
    return src_result.error();
  auto src = *src_result;
  VAST_INFO("{} spawned a {} source", self, format);
  src->attach_functor([=](const caf::error& reason) {
    if (!reason || reason == caf::exit_reason::user_shutdown)
      VAST_INFO("{} source shut down", detail::pretty_type_name(format));
    else
      VAST_WARN("{} source shut down with error: {}",
                detail::pretty_type_name(format), reason);
  });
  return src;
}

} // namespace vast::system
