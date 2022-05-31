//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/explorer.hpp"

#include "vast/fwd.hpp"

#include "vast/bitmap.hpp"
#include "vast/command.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/string.hpp"
#include "vast/expression.hpp"
#include "vast/ids.hpp"
#include "vast/logger.hpp"
#include "vast/system/query_status.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/table_slice_column.hpp"
#include "vast/uuid.hpp"

#include <caf/event_based_actor.hpp>
#include <caf/settings.hpp>

#include <algorithm>
#include <optional>

using namespace std::chrono_literals;

namespace vast::system {

explorer_state::explorer_state(caf::event_based_actor*) {
  // nop
}

void explorer_state::forward_results(vast::table_slice slice) {
  // Discard all if the client already has enough.
  if (num_sent == limits.total)
    return;
  // Check which of the ids in this slice were already sent to the sink
  // and forward those that were not.
  // TODO: This could be made more efficient by creating the hashers for
  // each row at once and go over the underlying record batch in columnar
  // fashion.
  // TODO (alternative): We should consider changing the approach and
  // collect all results from the intial expression to construct a single
  // optimized second expression instead.
  ewah_bitmap unseen = {};
  const auto offset = slice.offset() == invalid_id ? 0 : slice.offset();
  unseen.append_bits(false, offset);
  for (size_t i = 0; i < slice.rows(); ++i) {
    default_hash h;
    for (size_t j = 0; j < slice.columns(); ++j)
      hash_append(h, slice.at(i, j));
    auto digest = h.finish();
    auto [_, new_] = returned_ids.insert(digest);
    unseen.append_bits(new_, 1);
    if (num_sent + rank(unseen) == limits.total)
      break;
  }
  VAST_TRACE("{} forwards {} hits", *self, rank(unseen));
  if (unseen.empty())
    return;
  if (all<0>(unseen))
    return;
  if (unseen.size() != slice.rows()) {
    auto maybe_slice = vast::filter(slice, unseen);
    VAST_ASSERT(maybe_slice);
    slice = *maybe_slice;
  }
  self->send(sink, slice);
  num_sent += slice.rows();
}

caf::behavior
explorer(caf::stateful_actor<explorer_state>* self, node_actor node,
         explorer_state::event_limits limits,
         std::optional<vast::duration> before,
         std::optional<vast::duration> after, std::optional<std::string> by) {
  auto& st = self->state;
  st.self = self;
  st.node = node;
  st.limits = limits;
  st.num_sent = 0;
  // If none of 'before' and 'after' a given we assume an infinite timebox
  // around each result, but if one of them is given the interval should be
  // finite on both sides.
  if (before && !after)
    after = vast::duration{0s};
  if (after && !before)
    before = vast::duration{0s};
  st.before = before;
  st.after = after;
  st.by = by;
  auto quit_if_done = [=]() {
    auto& st = self->state;
    if (st.initial_query_completed && st.running_exporters == 0)
      self->quit();
  };
  self->set_down_handler([=]([[maybe_unused]] const caf::down_msg& msg) {
    // Only the spawned EXPORTERs are expected to send down messages.
    auto& st = self->state;
    --st.running_exporters;
    VAST_DEBUG("{} received DOWN from {} outstanding requests: {}", *self,
               msg.source, st.running_exporters);
    quit_if_done();
  });
  return {
    [=](table_slice slice) {
      auto& st = self->state;
      // TODO: Add some cleaner way to distinguish the different input streams,
      // maybe some 'tagged' stream in caf?
      if (!st.initial_exporter) {
        VAST_ERROR("{} received table slices before an initial "
                   "exporter",
                   *self);
        return;
      }
      if (self->current_sender() != st.initial_exporter) {
        st.forward_results(slice);
        return;
      }
      st.initial_query_results += slice.rows();
      VAST_TRACE("{} got a table slice for the inital query ({}/{})", *self,
                 st.initial_query_results, st.limits.initial_query);
      if (st.initial_query_results >= st.limits.initial_query)
        self->send_exit(self->state.initial_exporter,
                        caf::exit_reason::user_shutdown);
      // Don't bother making new queries if we discard all results anyways.
      if (st.num_sent >= st.limits.total)
        return;
      const auto& layout = slice.layout();
      const auto& layout_rt = caf::get<record_type>(layout);
      auto is_timestamp = [](const auto& leaf) {
        const auto& [field, _] = leaf;
        for (const auto& name : field.type.names())
          if (name == "timestamp")
            return true;
        return false;
      };
      auto timestamp_leaf = std::optional<record_type::leaf_view>{};
      for (auto&& leaf : caf::get<record_type>(layout).leaves()) {
        if (is_timestamp(leaf)) {
          timestamp_leaf = std::move(leaf);
          break;
        }
      }
      if (!timestamp_leaf) {
        VAST_DEBUG("{} could not find timestamp field in {}", *self,
                   layout.name());
        return;
      }
      std::optional<table_slice_column> by_column;
      if (st.by) {
        for (auto&& by_index :
             layout_rt.resolve_key_suffix(*st.by, layout.name())) {
          // NOTE: We're intentionally stopping after the first instance here.
          by_column.emplace(slice, layout_rt.flat_index(by_index));
          break;
        }
        if (!by_column) {
          VAST_TRACE_SCOPE("skipping slice with {} because it has no column {}",
                           layout.name(), *st.by);
          return;
        }
      }
      VAST_DEBUG("{} uses {} to construct timebox", *self,
                 timestamp_leaf->field.name);
      auto column = table_slice_column{
        slice, layout_rt.flat_index(timestamp_leaf->index)};
      for (size_t i = 0; i < column.size(); ++i) {
        auto data_view = column[i];
        auto x = caf::get_if<vast::time>(&data_view);
        // Skip if no value
        if (!x)
          continue;
        std::optional<vast::expression> before_expr;
        const auto timestamp_type = type{"timestamp", time_type{}};
        if (st.before)
          before_expr = predicate{type_extractor{timestamp_type},
                                  relational_operator::greater_equal,
                                  data{*x - *st.before}};
        std::optional<vast::expression> after_expr;
        if (st.after)
          after_expr
            = predicate{type_extractor{timestamp_type},
                        relational_operator::less_equal, data{*x + *st.after}};
        std::optional<vast::expression> by_expr;
        if (st.by) {
          VAST_ASSERT(by_column); // Should have been checked above.
          auto ci = (*by_column)[i];
          if (caf::get_if<caf::none_t>(&ci))
            continue;
          // TODO: Make `predicate` accept a data_view as well to save
          // the call to `materialize()`.
          by_expr = predicate{extractor{*st.by}, relational_operator::equal,
                              materialize(ci)};
        }
        auto build_conjunction
          = [](std::optional<expression>&& lhs,
               std::optional<expression>&& rhs) -> std::optional<expression> {
          if (lhs && rhs)
            return conjunction{std::move(*lhs), std::move(*rhs)};
          if (lhs)
            return std::move(lhs);
          if (rhs)
            return std::move(rhs);
          return std::nullopt;
        };
        auto temporal_expr
          = build_conjunction(std::move(before_expr), std::move(after_expr));
        auto spatial_expr = std::move(by_expr);
        auto expr = build_conjunction(std::move(temporal_expr),
                                      std::move(spatial_expr));
        // We should have checked during argument parsing that `expr` has at
        // least one constraint.
        VAST_ASSERT(expr);
        auto query = to_string(*expr);
        VAST_TRACE("{} spawns new exporter with query {}", *self, query);
        auto exporter_invocation = invocation{{}, "spawn exporter", {query}};
        caf::put(exporter_invocation.options, "vast.export.max-events",
                 st.limits.per_result);
        ++self->state.running_exporters;
        self
          ->request(st.node, caf::infinite, atom::spawn_v, exporter_invocation)
          .then(
            [=](caf::actor handle) {
              auto exporter = caf::actor_cast<exporter_actor>(handle);
              VAST_DEBUG("{} registers exporter {}", *self, exporter);
              self->monitor(exporter);
              self->send(exporter, atom::sink_v, self);
              self->send(exporter, atom::run_v);
            },
            [=](caf::error error) {
              --self->state.running_exporters;
              VAST_ERROR("{} failed to spawn exporter: {}", *self, error);
            });
      }
    },
    [=](atom::provision, exporter_actor exporter) {
      self->state.initial_exporter = exporter.address();
    },
    [=]([[maybe_unused]] std::string name, query_status) {
      VAST_DEBUG("{} received final status from {}", *self, name);
      self->state.initial_query_completed = true;
      quit_if_done();
    },
    [=](atom::sink, const caf::actor& sink) {
      VAST_DEBUG("{} registers sink {}", *self, sink);
      auto& st = self->state;
      st.sink = sink;
    }};
}

} // namespace vast::system
