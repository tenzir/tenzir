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

#include "vast/aliases.hpp"
#include "vast/config.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/detail/unbox_var.hpp"
#include "vast/fwd.hpp"
#include "vast/logger.hpp"
#include "vast/system/node.hpp"
#include "vast/system/source.hpp"
#include "vast/system/spawn_arguments.hpp"

namespace vast::system {

/// Tries to spawn a new SOURCE for the specified format.
/// @tparam Reader the format-specific reader
/// @param self Points to the parent actor.
/// @param args Configures the new actor.
/// @returns a handle to the spawned actor on success, an error otherwise
template <class Reader, class Defaults = typename Reader::defaults>
maybe_actor spawn_source(node_actor* self, spawn_arguments& args) {
  VAST_TRACE(VAST_ARG("node", self), VAST_ARG(args));
  auto& st = self->state;
  auto& options = args.inv.options;
  // Bail out early for bogus invocations.
  if (caf::get_or(options, "system.node", false))
    return make_error(ec::invalid_configuration,
                      "unable to spawn a remote source when spawning a node "
                      "locally instead of connecting to one; please unset "
                      "the option system.node");
  VAST_UNBOX_VAR(in, detail::make_input_stream<Defaults>(args.inv.options));
  VAST_UNBOX_VAR(sch, read_schema(args));
  auto table_slice_type = caf::get_or(options, "source.spawn.table-slice-type",
                                      defaults::import::table_slice_type);
  auto slice_size = get_or(options, "source.spawn.table-slice-size",
                           defaults::import::table_slice_size);
  auto max_events = caf::get_if<size_t>(&options, "source.spawn.max-events");
  auto type = caf::get_if<std::string>(&options, "source.spawn.type");
  auto type_filter = type ? std::move(*type) : std::string{};
  auto schema = get_schema(options, "spawn.source");
  if (!schema)
    return schema.error();
  if (slice_size == 0)
    return make_error(ec::invalid_configuration, "table-slice-size can't be 0");
  Reader reader{table_slice_type, options, std::move(in)};
  VAST_INFO(self, "spawned a", reader.name(), "source");
  auto src
    = self->spawn<caf::detached>(source<Reader>, std::move(reader), slice_size,
                                 max_events, st.type_registry, vast::schema{},
                                 std::move(type_filter), accountant_type{});
  src->attach_functor([=, name = reader.name()](const caf::error& reason) {
    if (!reason || reason == caf::exit_reason::user_shutdown)
      VAST_INFO(name, "source shut down");
    else
      VAST_WARNING(name, "source shut down with error:", reason);
  });
  self->send(src, atom::sink_v, st.importer);
  return src;
}

} // namespace vast::system
