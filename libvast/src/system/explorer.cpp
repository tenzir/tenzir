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

#include "vast/system/explorer.hpp"

#include "vast/command.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/string.hpp"
#include "vast/expression.hpp"
#include "vast/logger.hpp"
#include "vast/system/atoms.hpp"
#include "vast/system/exporter.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/table_slice_builder_factory.hpp"

#include <caf/event_based_actor.hpp>

#include <algorithm>

namespace vast::system {

explorer_state::explorer_state(caf::event_based_actor*) {
  // nop
}

void explorer_state::forward_results(vast::table_slice_ptr slice) {
  // Check which of the ids in this slice were already sent to the sink
  // and forward those that were not.
  vast::bitmap unseen;
  for (int i = 0; i < slice->rows(); ++i) {
    auto id = slice->offset() + i;
    auto [_, new_] = returned_ids.insert(id);
    if (new_) {
      vast::ids tmp;
      tmp.append_bits(false, id);
      tmp.append_bits(true, 1);
      unseen |= tmp;
    }
  }
  if (unseen.empty())
    return;
  if (unseen.size() == slice->rows()) {
    self->send(sink, slice);
    return;
  }
  // If a slice was partially known, divide it up and forward only those
  // ids that the source hasn't received yet.
  auto slices = vast::select(slice, unseen);
  for (auto slice : slices) {
    self->send(sink, slice);
  }
  return;
}

caf::behavior
explorer(caf::stateful_actor<explorer_state>* self, caf::actor node,
         vast::duration before, vast::duration after) {
  auto& st = self->state;
  st.self = self;
  st.node = node;
  st.before = before;
  st.after = after;
  auto quit_if_done = [=]() {
    auto& st = self->state;
    if (st.initial_query_completed && st.running_exporters == 0)
      self->quit();
  };
  self->set_down_handler([=]([[maybe_unused]] const caf::down_msg& msg) {
    // Only the spawned EXPORTERs are expected to send down messages.
    auto& st = self->state;
    --st.running_exporters;
    VAST_DEBUG(self, "received DOWN from", msg.source,
               "outstanding requests:", st.running_exporters);
    quit_if_done();
  });
  return {
    [=](vast::table_slice_ptr slice) {
      auto& st = self->state;
      // TODO: Add some cleaner way to distinguish the different input streams,
      // maybe some 'tagged' stream in caf?
      if (self->current_sender() != st.initial_query_exporter) {
        st.forward_results(slice);
        return;
      }
      auto& layout = slice->layout();
      auto it = std::find_if(layout.fields.begin(), layout.fields.end(),
                             [](const record_field& field) {
                               return has_attribute(field.type, "timestamp");
                             });
      if (it == layout.fields.end()) {
        VAST_DEBUG(self, "could not find timestamp field in", layout);
        return;
      }
      VAST_DEBUG(self, "uses", it->name, "to construct timebox");
      auto column = slice->column(it->name);
      VAST_ASSERT(column);
      for (size_t i = 0; i < column->rows(); ++i) {
        auto data_view = (*column)[i];
        auto x = caf::get_if<vast::time>(&data_view);
        // Skip if no value
        if (!x)
          continue;
        std::optional<vast::expression> before_expr;
        if (st.before)
          before_expr = predicate{attribute_extractor{timestamp_atom::value},
                                  greater_equal, data{*x - *st.before}};

        std::optional<vast::expression> after_expr;
        if (st.after)
          after_expr = predicate{attribute_extractor{timestamp_atom::value},
                                 less_equal, data{*x + *st.after}};
        expression expr;
        if (after_expr && before_expr) {
          expr = conjunction{*before_expr, *after_expr};
        } else if (after_expr) {
          expr = *after_expr;
        } else if (before_expr) {
          expr = *before_expr;
        }
        VAST_TRACE(self, "constructed expression", expr);
        auto query = to_string(expr);
        VAST_TRACE(self, "spawns new exporter with query", query);
        auto exporter_invocation
          = command::invocation{{}, "spawn exporter", {query}};
        self->send(st.node, exporter_invocation);
        ++st.running_exporters;
      }
    },
    [=](provision_atom, caf::actor exp) {
      self->state.initial_query_exporter = exp;
    },
    [=](caf::actor exp) {
      VAST_DEBUG(self, "registers exporter", exp);
      auto& st = self->state;
      self->monitor(exp);
      self->send(exp, system::sink_atom::value, self);
      self->send(exp, system::run_atom::value);
    },
    [=]([[maybe_unused]] std::string name, query_status) {
      VAST_DEBUG(self, "received final status from", name);
      self->state.initial_query_completed = true;
      quit_if_done();
    },
    [=](sink_atom, const caf::actor& sink) {
      VAST_DEBUG(self, "registers sink", sink);
      auto& st = self->state;
      st.sink = sink;
    }};
}

} // namespace vast::system
