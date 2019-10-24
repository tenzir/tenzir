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

#include "vast/system/pivoter.hpp"

#include "vast/command.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/detail/string.hpp"
#include "vast/expression.hpp"
#include "vast/logger.hpp"
#include "vast/system/atoms.hpp"
#include "vast/system/exporter.hpp"
#include "vast/table_slice.hpp"

#include <caf/event_based_actor.hpp>

namespace vast::system {

namespace {

/// Returns the field that shall be used to extract values from for
/// the pivot membership query.
caf::optional<record_field>
common_field(const pivoter_state& st, const record_type& indicator) {
  auto f = st.cache.find(indicator);
  if (f != st.cache.end())
    return f->second;
    // TODO: This algorithm can be enabled once we have a live updated
    //       type registry. (Switch the type of target to record_type.)
#if 0
  for (auto& t : target.fields) {
    for (auto& i : indicator.fields) {
      if (t.name == i.name) {
        st.cache.insert({indicator, i});
        return i;
      }
    }
  }
#else
  // This is a heuristic to find the field for pivoting until a runtime
  // updated type registry is available to feed the algorithm above.
  std::string edge;
  VAST_TRACE(st.self, VAST_ARG(st.target), VAST_ARG(indicator.name()));
  if (detail::starts_with(st.target, "suricata") || st.target == "pcap.packet")
    edge = "community_id";
  else if (detail::starts_with(st.target, "zeek")) {
    if (detail::starts_with(indicator.name(), "zeek"))
      edge = "uid";
    else
      edge = "community_id";
  }
  for (auto& i : indicator.fields) {
    if (i.name == edge) {
      st.cache.insert({indicator, i});
      return i;
    }
  }
#endif
  st.cache.insert({indicator, caf::none});
  VAST_WARNING(st.self, "got slice without shared column:", indicator.name());
  return caf::none;
}

} // namespace

pivoter_state::pivoter_state(caf::event_based_actor*) {
  // nop
}

caf::behavior pivoter(caf::stateful_actor<pivoter_state>* self, caf::actor node,
                      std::string target, expression expr) {
  auto& st = self->state;
  st.self = self;
  st.node = node;
  st.expr = std::move(expr);
  st.target = std::move(target);
  auto quit_if_done = [=]() {
    auto& st = self->state;
    if (st.initial_query_completed && st.running_exporters == 0)
      self->quit();
  };
  self->set_down_handler([=](const caf::down_msg& msg) {
    // Only the spawned EXPORTERs are expected to send down messages.
    auto& st = self->state;
    st.running_exporters--;
    VAST_DEBUG(self, "received DOWN from", msg.source,
               "outstanding requests:", st.running_exporters);
    quit_if_done();
  });
  return {[=](vast::table_slice_ptr slice) {
            auto& st = self->state;
            auto pivot_field = common_field(st, slice->layout());
            if (!pivot_field)
              return;
            VAST_DEBUG(self, "uses", *pivot_field, "to extract", st.target,
                       "events");
            auto column = slice->column(pivot_field->name);
            auto xs = set{};
            for (size_t i = 0; i < column->rows(); ++i) {
              auto data = materialize((*column)[i]);
              auto x = caf::get_if<std::string>(&data);
              // Skip if no value
              if (!x)
                continue;
              // Skip if id was already requested
              if (st.requested_ids.count(*x) > 0)
                continue;
              xs.insert(*x);
              st.requested_ids.insert(*x);
            }
            if (xs.empty()) {
              VAST_DEBUG(self, "already queried for all", pivot_field->name);
              return;
            }
            auto expr = conjunction{
              predicate{attribute_extractor{type_atom::value}, equal,
                        data{st.target}},
              predicate{key_extractor{pivot_field->name}, in, data{xs}}};
            // TODO(ch9411): Drop the conversion to a string when node actors can
            //               be spawned without going through an invocation.
            auto query = to_string(expr);
            VAST_DEBUG(self, "queries for", xs.size(), pivot_field->name);
            VAST_TRACE(self, "spawns new exporter with query", query);
            auto exporter_invocation
              = command::invocation{{}, "spawn exporter", {query}};
            self->send(st.node, exporter_invocation);
            st.running_exporters++;
          },
          [=](caf::actor exp) {
            VAST_DEBUG(self, "registers exporter", exp);
            auto& st = self->state;
            self->monitor(exp);
            self->send(exp, system::sink_atom::value, st.sink);
            self->send(exp, system::run_atom::value);
          },
          [=](std::string name, query_status) {
            VAST_DEBUG(self, "received final status from", name);
            self->state.initial_query_completed = true;
            quit_if_done();
          },
          [=](sink_atom, const caf::actor& sink) {
            VAST_DEBUG(self, "registers sink", sink);
            auto& st = self->state;
            st.sink = sink;
          },
          [=](const uuid& id, const query_status&) {
            VAST_IGNORE_UNUSED(id);
            VAST_INFO(self, "got query statistics from", id);
          }};
}

} // namespace vast::system
