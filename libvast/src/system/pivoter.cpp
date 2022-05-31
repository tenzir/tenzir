//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/pivoter.hpp"

#include "vast/fwd.hpp"

#include "vast/command.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/detail/string.hpp"
#include "vast/expression.hpp"
#include "vast/logger.hpp"
#include "vast/system/query_status.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_column.hpp"
#include "vast/uuid.hpp"

#include <caf/event_based_actor.hpp>
#include <caf/settings.hpp>

namespace vast::system {

namespace {

/// Returns the field that shall be used to extract values from for
/// the pivot membership query.
std::optional<record_type::field_view>
common_field(const pivoter_state& st, const type& t) {
  VAST_ASSERT(caf::holds_alternative<record_type>(t));
  const auto& indicator = caf::get<record_type>(t);
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
  VAST_TRACE_SCOPE("{} {} {}", *st.self, VAST_ARG(st.target),
                   VAST_ARG(indicator));
  if (st.target.starts_with("zeek") && t.name().starts_with("zeek"))
    edge = "uid";
  else
    edge = "community_id";
  for (auto i : indicator.fields()) {
    if (i.name == edge) {
      st.cache.insert({indicator, i});
      return i;
    }
  }
#endif
  st.cache.insert({indicator, std::nullopt});
  VAST_WARN("{} got slice without shared column: {}", *st.self, t.name());
  return {};
}

} // namespace

pivoter_state::pivoter_state(caf::event_based_actor*) {
  // nop
}

caf::behavior pivoter(caf::stateful_actor<pivoter_state>* self, node_actor node,
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
  self->set_down_handler([=]([[maybe_unused]] const caf::down_msg& msg) {
    // Only the spawned EXPORTERs are expected to send down messages.
    auto& st = self->state;
    st.running_exporters--;
    VAST_DEBUG("{} received DOWN from {} outstanding requests: {}", *self,
               msg.source, st.running_exporters);
    quit_if_done();
  });
  return {
    [=](vast::table_slice slice) {
      auto& st = self->state;
      const auto& layout = slice.layout();
      const auto& layout_rt = caf::get<record_type>(layout);
      auto pivot_field = common_field(st, layout);
      if (!pivot_field)
        return;
      VAST_DEBUG("{} uses {} to extract {} events", *self, *pivot_field,
                 st.target);
      auto column = std::optional<table_slice_column>{};
      for (auto&& index :
           layout_rt.resolve_key_suffix(pivot_field->name, layout.name())) {
        // NOTE: We're intentionally only using the first result here.
        column.emplace(slice, layout_rt.flat_index(index));
        break;
      }
      VAST_ASSERT(column);
      auto xs = list{};
      for (size_t i = 0; i < column->size(); ++i) {
        auto data = materialize((*column)[i]);
        auto x = caf::get_if<std::string>(&data);
        // Skip if no value
        if (!x)
          continue;
        // Skip if ID was already requested
        if (st.requested_ids.count(*x) > 0)
          continue;
        xs.push_back(*x);
        st.requested_ids.insert(*x);
      }
      if (xs.empty()) {
        VAST_DEBUG("{} already queried for all {}", *self, pivot_field->name);
        return;
      }
      auto expr
        = conjunction{predicate{selector{selector::type},
                                relational_operator::equal, data{st.target}},
                      predicate{extractor{std::string{pivot_field->name}},
                                relational_operator::in, data{xs}}};
      // TODO(ch9411): Drop the conversion to a string when node actors can
      //               be spawned without going through an invocation.
      auto query = to_string(expr);
      VAST_DEBUG("{} queries for {} {}", *self, xs.size(), pivot_field->name);
      VAST_TRACE_SCOPE("{} spawns new exporter with query {}", *self, query);
      auto exporter_options = caf::settings{};
      caf::put(exporter_options, "vast.export.disable-taxonomies", true);
      auto exporter_invocation
        = invocation{std::move(exporter_options), "spawn exporter", {query}};
      ++self->state.running_exporters;
      self->request(st.node, caf::infinite, atom::spawn_v, exporter_invocation)
        .then(
          [=](caf::actor handle) {
            auto exporter = caf::actor_cast<exporter_actor>(handle);
            VAST_DEBUG("{} registers exporter {}", *self, exporter);
            self->monitor(exporter);
            self->send(exporter, atom::sink_v, self->state.sink);
            self->send(exporter, atom::run_v);
          },
          [=](caf::error error) {
            --self->state.running_exporters;
            VAST_ERROR("{} failed to spawn exporter: {}", *self, render(error));
          });
      ;
    },
    [=](std::string name, query_status) {
      VAST_DEBUG("{} received final status from {}", *self, name);
      self->state.initial_query_completed = true;
      quit_if_done();
    },
    [=](atom::sink, const caf::actor& sink) {
      VAST_DEBUG("{} registers sink {}", *self, sink);
      auto& st = self->state;
      st.sink = sink;
    },
  };
}

} // namespace vast::system
