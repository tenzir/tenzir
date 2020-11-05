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

#include "vast/system/get_command.hpp"

#include "vast/aliases.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/si.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/format/writer.hpp"
#include "vast/fwd.hpp"
#include "vast/ids.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/node_control.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"

#include <caf/expected.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>

#include <vector>

namespace vast::system {

namespace {

ids to_ids(const count& x) {
  ids result;
  result.append_bits(false, x);
  result.append_bit(true);
  return result;
}

caf::error
run(caf::scoped_actor& self, archive_type archive, const invocation& inv) {
  using namespace std::string_literals;
  auto output_format = get_or(inv.options, "vast.get.format", "json"s);
  auto writer = format::writer::make(output_format, inv.options);
  if (!writer)
    return writer.error();
  // TODO: Sending one id at a time is overly pessimistic. A smarter algorithm
  // would request all ids at once and reorder the results for printing.
  // Introduce an option to get the current behavior when implemnting this.
  for (auto& c : inv.arguments) {
    auto i = to<count>(c);
    if (!i)
      return make_error(ec::parse_error, c, "is not a positive integer");
    self->send(archive, to_ids(*i));
    bool waiting = true;
    self->receive_while(waiting)
      // Message handlers.
      ([&](table_slice_ptr slice) { (*writer)->write(slice); },
       [&](atom::done, const caf::error& err) {
         if (err)
           VAST_WARNING_ANON("failed to get table slice:", render(err));
         waiting = false;
       });
  }
  return caf::none;
}

} // namespace

caf::message
get_command(const invocation& inv, caf::actor_system& sys) {
  VAST_TRACE(inv);
  caf::scoped_actor self{sys};
  // Get VAST node.
  auto node_opt
    = spawn_or_connect_to_node(self, inv.options, content(sys.config()));
  if (auto err = caf::get_if<caf::error>(&node_opt))
    return caf::make_message(std::move(*err));
  auto& node = caf::holds_alternative<caf::actor>(node_opt)
                 ? caf::get<caf::actor>(node_opt)
                 : caf::get<scope_linked_actor>(node_opt).get();
  VAST_ASSERT(node != nullptr);
  auto components = get_node_components(self, node, "archive");
  if (!components)
    return caf::make_message(std::move(components.error()));
  auto archive = caf::actor_cast<archive_type>((*components)[0]);
  VAST_ASSERT(archive);
  self->send(archive, atom::exporter_v, self);
  auto err = run(self, archive, inv);
  return caf::make_message(err);
}

} // namespace vast::system
