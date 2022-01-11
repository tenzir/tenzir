//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/get_command.hpp"

#include "vast/fwd.hpp"

#include "vast/aliases.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/si.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/format/writer.hpp"
#include "vast/ids.hpp"
#include "vast/logger.hpp"
#include "vast/query.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/node_control.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"
#include "vast/table_slice.hpp"
#include "vast/uuid.hpp"

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
run(caf::scoped_actor& self, index_actor index, const invocation& inv) {
  using namespace std::string_literals;
  auto output_format = get_or(inv.options, "vast.get.format", "json"s);
  auto writer = format::writer::make(output_format, inv.options);
  if (!writer)
    return writer.error();
  // TODO: Sending one id at a time is overly pessimistic. A smarter algorithm
  // would request all ids at once and reorder the results for printing.
  // Introduce an option to get the current behavior when implementing this.
  for (const auto& c : inv.arguments) {
    auto i = to<count>(c);
    if (!i)
      return caf::make_error(ec::parse_error, c, "is not a positive integer");
    vast::ids ids = to_ids(*i);
    // The caf::actor_cast here is necessary because a scoped actor cannot be a
    // typed actor. The message handlers below reflect those of the
    // receiver_actor<table_slice> exactly, but there's no way to verify that at
    // compile time. We can improve upon this situation when changing the
    // archive to stream its results.
    auto q = query::make_extract(self, query::extract::drop_ids, expression{});
    q.ids = std::move(ids);
    self->send(index, atom::evaluate_v, std::move(q));
    bool waiting = true;
    self->receive_while(waiting)
      // Message handlers.
      (
        [&](const table_slice& slice) {
          (*writer)->write(slice);
        },
        [&](atom::done) {
          waiting = false;
        });
  }
  return caf::none;
}

} // namespace

caf::message get_command(const invocation& inv, caf::actor_system& sys) {
  VAST_TRACE_SCOPE("{}", inv);
  caf::scoped_actor self{sys};
  // Get VAST node.
  auto node_opt
    = spawn_or_connect_to_node(self, inv.options, content(sys.config()));
  if (auto err = std::get_if<caf::error>(&node_opt))
    return caf::make_message(std::move(*err));
  const auto& node = std::holds_alternative<node_actor>(node_opt)
                       ? std::get<node_actor>(node_opt)
                       : std::get<scope_linked<node_actor>>(node_opt).get();
  VAST_ASSERT(node != nullptr);
  auto components = get_node_components<index_actor>(self, node);
  if (!components)
    return caf::make_message(std::move(components.error()));
  auto&& [index] = *components;
  VAST_ASSERT(index);
  auto err = run(self, index, inv);
  return caf::make_message(err);
}

} // namespace vast::system
