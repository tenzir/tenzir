//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/exporter.hpp"

#include "vast/fwd.hpp"

#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/fill_status_map.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/error.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/logger.hpp"
#include "vast/query.hpp"
#include "vast/system/query_status.hpp"
#include "vast/system/report.hpp"
#include "vast/system/status_verbosity.hpp"
#include "vast/table_slice.hpp"

#include <caf/settings.hpp>
#include <caf/stream_slot.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

namespace {

void ship_results(exporter_actor::stateful_pointer<exporter_state> self) {
  VAST_TRACE_SCOPE("");
  auto& st = self->state;
  VAST_DEBUG("{} relays {} events", self, st.query.cached);
  while (st.query.requested > 0 && st.query.cached > 0) {
    VAST_ASSERT(!st.results.empty());
    // Fetch the next table slice. Either we grab the entire first slice in
    // st.results or we need to split it up.
    table_slice slice = {};
    if (st.results[0].rows() <= st.query.requested) {
      slice = std::move(st.results[0]);
      st.results.erase(st.results.begin());
    } else {
      auto [first, second] = split(st.results[0], st.query.requested);
      VAST_ASSERT(first.encoding() != table_slice_encoding::none);
      VAST_ASSERT(second.encoding() != table_slice_encoding::none);
      VAST_ASSERT(first.rows() == st.query.requested);
      slice = std::move(first);
      st.results[0] = std::move(second);
    }
    // Ship the slice and update state.
    auto rows = slice.rows();
    VAST_ASSERT(rows <= st.query.cached);
    st.query.cached -= rows;
    st.query.requested -= rows;
    st.query.shipped += rows;
    self->anon_send(st.sink, std::move(slice));
  }
}

void report_statistics(exporter_actor::stateful_pointer<exporter_state> self) {
  auto& st = self->state;
  if (st.statistics_subscriber)
    self->anon_send(st.statistics_subscriber, st.name, st.query);
  if (st.accountant) {
    auto hits = rank(st.hits);
    auto processed = st.query.processed;
    auto shipped = st.query.shipped;
    auto results = shipped + st.results.size();
    auto selectivity = double(results) / processed;
    auto msg = report{
      {"exporter.hits", hits},
      {"exporter.processed", processed},
      {"exporter.results", results},
      {"exporter.shipped", shipped},
      {"exporter.selectivity", selectivity},
      {"exporter.runtime", st.query.runtime},
    };
    self->send(st.accountant, msg);
  }
}

void shutdown(exporter_actor::stateful_pointer<exporter_state> self,
              caf::error err) {
  VAST_DEBUG("{} initiates shutdown with error {}", self, render(err));
  self->send_exit(self, std::move(err));
}

void shutdown(exporter_actor::stateful_pointer<exporter_state> self) {
  if (has_continuous_option(self->state.options))
    return;
  VAST_DEBUG("{} initiates shutdown", self);
  self->send_exit(self, caf::exit_reason::normal);
}

void request_more_hits(exporter_actor::stateful_pointer<exporter_state> self) {
  auto& st = self->state;
  // Sanity check.
  if (!has_historical_option(st.options)) {
    VAST_WARN("{} requested more hits for continuous query", self);
    return;
  }
  // Do nothing if we already shipped everything the client asked for.
  if (st.query.requested == 0) {
    VAST_DEBUG("{} shipped {} results and waits for client to request "
               "more",
               self, self->state.query.shipped);
    return;
  }
  // Do nothing if we received everything.
  if (st.query.received == st.query.expected) {
    VAST_DEBUG("{} received hits for all {} partitions", self,
               st.query.expected);
    return;
  }
  // If the if-statement above isn't true then `received < expected` must hold.
  // Otherwise, we would receive results for more partitions than qualified as
  // hits by the INDEX.
  VAST_ASSERT(st.query.received < st.query.expected);
  auto remaining = st.query.expected - st.query.received;
  // TODO: Figure out right number of partitions to ask for. For now, we
  // bound the number by an arbitrary constant.
  auto n = std::min(remaining, size_t{2});
  // Store how many partitions we schedule with our request. When receiving
  // 'done', we add this number to `received`.
  st.query.scheduled = n;
  // Request more hits from the INDEX.
  VAST_DEBUG("{} asks index to process {} more partitions", self, n);
  self->send(st.index, st.id, detail::narrow<uint32_t>(n));
}

void handle_batch(exporter_actor::stateful_pointer<exporter_state> self,
                  table_slice slice) {
  VAST_ASSERT(slice.encoding() != table_slice_encoding::none);
  VAST_DEBUG("{} got batch of {} events", self, slice.rows());
  // Construct a candidate checker if we don't have one for this type.
  type t = slice.layout();
  auto it = self->state.checkers.find(t);
  if (it == self->state.checkers.end()) {
    auto x = tailor(self->state.expr, t);
    if (!x) {
      VAST_ERROR("{} failed to tailor expression: {}", self, render(x.error()));
      ship_results(self);
      shutdown(self);
      return;
    }
    VAST_DEBUG("{} tailored AST to {}: {}", self, t, x);
    std::tie(it, std::ignore)
      = self->state.checkers.emplace(type{slice.layout()}, std::move(*x));
  }
  auto& checker = it->second;
  // Perform candidate check, splitting the slice into subsets if needed.
  self->state.query.processed += slice.rows();
  auto selection = evaluate(checker, slice);
  auto selection_size = rank(selection);
  if (selection_size == 0) {
    // No rows qualify.
    return;
  }
  self->state.query.cached += selection_size;
  select(self->state.results, slice, selection);
  // Ship slices to connected SINKs.
  ship_results(self);
}

} // namespace

exporter_actor::behavior_type
exporter(exporter_actor::stateful_pointer<exporter_state> self, expression expr,
         query_options options) {
  self->state.options = options;
  self->state.expr = std::move(expr);
  if (has_continuous_option(options))
    VAST_DEBUG("{} has continuous query option", self);
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    VAST_DEBUG("{} received exit from {} with reason: {}", self, msg.source,
               msg.reason);
    auto& st = self->state;
    if (msg.reason != caf::exit_reason::kill)
      report_statistics(self);
    // Sending 0 to the index means dropping further results.
    self->send<caf::message_priority::high>(st.index, st.id,
                                            static_cast<uint32_t>(0));
    self->quit(msg.reason);
  });
  self->set_down_handler([=](const caf::down_msg& msg) {
    VAST_DEBUG("{} received DOWN from {}", self, msg.source);
    if (has_continuous_option(self->state.options)
        && msg.source == self->state.index)
      report_statistics(self);
    // Without sinks and resumable sessions, there's no reason to proceed.
    self->quit(msg.reason);
  });
  return {
    [self](atom::extract) -> caf::result<void> {
      // Sanity check.
      VAST_DEBUG("{} got request to extract all events", self);
      if (self->state.query.requested == max_events) {
        VAST_WARN("{} ignores extract request, already getting all", self);
        return {};
      }
      // Configure state to get all remaining partition results.
      self->state.query.requested = max_events;
      ship_results(self);
      request_more_hits(self);
      return {};
    },
    [self](atom::extract, uint64_t requested_results) -> caf::result<void> {
      // Sanity checks.
      if (requested_results == 0) {
        VAST_WARN("{} ignores extract request for 0 results", self);
        return {};
      }
      if (self->state.query.requested == max_events) {
        VAST_WARN("{} ignores extract request, already getting all", self);
        return {};
      }
      VAST_ASSERT(self->state.query.requested < max_events);
      // Configure state to get up to `requested_results` more events.
      auto n = std::min(max_events - requested_results, requested_results);
      VAST_DEBUG("{} got a request to extract {} more results in "
                 "addition to {} pending results",
                 self, n, self->state.query.requested);
      self->state.query.requested += n;
      ship_results(self);
      request_more_hits(self);
      return {};
    },
    [self](accountant_actor accountant) {
      self->state.accountant = std::move(accountant);
      self->send(self->state.accountant, atom::announce_v, self->name());
    },
    [self](index_actor index) {
      VAST_DEBUG("{} registers index {}", self, index);
      self->state.index = std::move(index);
      if (has_continuous_option(self->state.options))
        self->monitor(self->state.index);
    },
    [self](atom::sink, const caf::actor& sink) {
      VAST_DEBUG("{} registers sink {}", self, sink);
      self->state.sink = sink;
      self->monitor(self->state.sink);
    },
    [self](atom::run) {
      VAST_VERBOSE("{} executes query: {}", self, to_string(self->state.expr));
      self->state.start = std::chrono::system_clock::now();
      if (!has_historical_option(self->state.options))
        return;
      // TODO: The index replies to expressions by manually sending back to the
      // sender, which does not work with request(...).then(...) style of
      // communication for typed actors. Hence, we must actor_cast here.
      // Ideally, we would change that index handler to actually return the
      // desired value.
      auto verb = has_historical_with_ids_option(self->state.options)
                    ? query::verb::extract_with_ids
                    : query::verb::extract;
      self
        ->request(caf::actor_cast<caf::actor>(self->state.index), caf::infinite,
                  query{verb, self->state.expr})
        .then(
          [=](const uuid& lookup, uint32_t partitions, uint32_t scheduled) {
            VAST_VERBOSE("{} got lookup handle {}, scheduled {}/{} partitions",
                         self, lookup, scheduled, partitions);
            self->state.id = lookup;
            if (partitions > 0) {
              self->state.query.expected = partitions;
              self->state.query.scheduled = scheduled;
            } else {
              shutdown(self);
            }
          },
          [=](const caf::error& e) { shutdown(self, e); });
    },
    [self](atom::statistics, const caf::actor& statistics_subscriber) {
      VAST_DEBUG("{} registers statistics subscriber {}", self,
                 statistics_subscriber);
      self->state.statistics_subscriber = statistics_subscriber;
    },
    [self](
      caf::stream<table_slice> in) -> caf::inbound_stream_slot<table_slice> {
      return self
        ->make_sink(
          in,
          [](caf::unit_t&) {
            // nop
          },
          [=](caf::unit_t&, table_slice slice) {
            handle_batch(self, std::move(slice));
          },
          [=](caf::unit_t&, const caf::error& err) {
            if (err)
              VAST_ERROR("{} got error during streaming: {}", self, err);
          })
        .inbound_slot();
    },
    // -- status_client_actor --------------------------------------------------
    [self](atom::status, status_verbosity v) {
      auto result = caf::settings{};
      auto& exporter_status = put_dictionary(result, "exporter");
      if (v >= status_verbosity::info) {
        caf::settings exp;
        put(exp, "expression", to_string(self->state.expr));
        auto& xs = put_list(result, "queries");
        xs.emplace_back(std::move(exp));
      }
      if (v >= status_verbosity::detailed) {
        caf::settings exp;
        put(exp, "expression", to_string(self->state.expr));
        put(exp, "hits", rank(self->state.hits));
        put(exp, "start", caf::deep_to_string(self->state.start));
        auto& xs = put_list(result, "queries");
        xs.emplace_back(std::move(exp));
        detail::fill_status_map(exporter_status, self);
      }
      return result;
    },
    // -- receiver_actor<table_slice>
    // ------------------------------------------------
    [self](table_slice slice) { //
      VAST_ASSERT(slice.encoding() != table_slice_encoding::none);
      VAST_DEBUG("{} got batch of {} events", self, slice.rows());
      self->state.query.processed += slice.rows();
      self->state.query.cached += slice.rows();
      self->state.results.push_back(slice);
      // Ship slices to connected SINKs.
      ship_results(self);
    },
    [self](atom::done) -> caf::result<void> {
      // Figure out if we're done by bumping the counter for `received` and
      // check whether it reaches `expected`.
      caf::timespan runtime
        = std::chrono::system_clock::now() - self->state.start;
      self->state.query.runtime = runtime;
      self->state.query.received += self->state.query.scheduled;
      if (self->state.query.received < self->state.query.expected) {
        VAST_DEBUG("{} received hits from {}/{} partitions", self,
                   self->state.query.received, self->state.query.expected);
        request_more_hits(self);
      } else {
        VAST_DEBUG("{} received all hits from {} partition(s) in {}", self,
                   self->state.query.expected, vast::to_string(runtime));
        if (self->state.accountant)
          self->send(self->state.accountant, "exporter.hits.runtime", runtime);
        shutdown(self);
      }
      return {};
    },
  };
}

} // namespace vast::system
