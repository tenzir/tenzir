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

#include <caf/all.hpp>

#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"
#include "vast/concept/printable/vast/event.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/fill_status_map.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/event.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/logger.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/atoms.hpp"
#include "vast/system/exporter.hpp"
#include "vast/table_slice.hpp"
#include "vast/to_events.hpp"

using namespace std::chrono;
using namespace std::string_literals;
using namespace caf;

namespace vast {
namespace system {

namespace {

void ship_results(stateful_actor<exporter_state>* self) {
  VAST_TRACE("");
  if (self->state.results.empty() || self->state.query.requested == 0) {
    return;
  }
  VAST_DEBUG(self, "relays", self->state.results.size(), "events");
  message msg;
  if (self->state.results.size() <= self->state.query.requested) {
    self->state.query.requested -= self->state.results.size();
    self->state.query.shipped += self->state.results.size();
    msg = make_message(std::move(self->state.results));
    self->state.results = {};
  } else {
    std::vector<event> remainder;
    remainder.reserve(self->state.results.size() - self->state.query.requested);
    auto begin = self->state.results.begin() + self->state.query.requested;
    auto end = self->state.results.end();
    std::move(begin, end, std::back_inserter(remainder));
    self->state.results.resize(self->state.query.requested);
    msg = make_message(std::move(self->state.results));
    self->state.results = std::move(remainder);
    self->state.query.shipped += self->state.query.requested;
    self->state.query.requested = 0;
  }
  self->send(self->state.sink, msg);
}

void report_statistics(stateful_actor<exporter_state>* self) {
  auto& st = self->state;
  timespan runtime = steady_clock::now() - st.start;
  st.query.runtime = runtime;
  VAST_INFO(self, "processed", st.query.processed, "candidates in",
            vast::to_string(runtime), "and shipped", st.query.shipped,
            "results");
  self->send(st.sink, st.id, st.query);
  if (st.accountant) {
    auto hits = rank(st.hits);
    auto processed = st.query.processed;
    auto shipped = st.query.shipped;
    auto results = shipped + st.results.size();
    auto selectivity = double(results) / processed;
    auto msg = report{{"exporter.hits", hits},
                      {"exporter.processed", processed},
                      {"exporter.results", results},
                      {"exporter.shipped", shipped},
                      {"exporter.selectivity", selectivity},
                      {"exporter.runtime", runtime}};
    self->send(st.accountant, msg);
  }
}

void shutdown(stateful_actor<exporter_state>* self, caf::error err) {
  VAST_DEBUG(self, "initiates shutdown with error", self->system().render(err));
  self->send_exit(self, std::move(err));
}

void shutdown(stateful_actor<exporter_state>* self) {
  if (has_continuous_option(self->state.options))
    return;
  VAST_DEBUG(self, "initiates shutdown");
  self->send_exit(self, exit_reason::normal);
}

void request_more_hits(stateful_actor<exporter_state>* self) {
  auto& st = self->state;
  // Sanity check.
  if (!has_historical_option(st.options)) {
    VAST_WARNING(self, "requested more hits for continuous query");
    return;
  }
  // Do nothing if we already shipped everything the client asked for.
  if (st.query.requested == 0) {
    VAST_DEBUG(self, "shipped", self->state.query.shipped,
               "results and waits for client to request more");
    return;
  }
  // Do nothing if we still have requests pending.
  if (st.query.lookups_issued > st.query.lookups_complete) {
    VAST_DEBUG(self, "currently awaits",
               st.query.lookups_issued - st.query.lookups_complete,
               "more lookup results");
    return;
  }
  // If the if-statement above isn't true then the two values must be equal.
  // Otherwise, we would complete more than we issue.
  VAST_ASSERT(st.query.lookups_issued == st.query.lookups_complete);
  // Do nothing if we received everything.
  if (st.query.received == st.query.expected) {
    VAST_DEBUG(self, "received results for all", st.query.expected,
               "partitions");
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
  VAST_DEBUG(self, "asks index to process", n, "more partitions");
  self->send(st.index, st.id, detail::narrow<uint32_t>(n));
}

} // namespace <anonymous>

caf::settings exporter_state::status() {
  caf::settings result;
  put(result, "hits", rank(hits));
  put(result, "start", caf::deep_to_string(start));
  put(result, "id", to_string(id));
  put(result, "expression", to_string(expr));
  return result;
}

behavior exporter(stateful_actor<exporter_state>* self, expression expr,
                  query_options options) {
  auto eu = self->system().dummy_execution_unit();
  self->state.sink = actor_pool::make(eu, actor_pool::broadcast());
  if (auto a = self->system().registry().get(accountant_atom::value)) {
    self->state.accountant = actor_cast<accountant_type>(a);
    self->send(self->state.accountant, announce_atom::value, self->name());
  }
  self->state.options = options;
  self->state.expr = std::move(expr);
  if (has_continuous_option(options))
    VAST_DEBUG(self, "has continuous query option");
  self->set_exit_handler(
    [=](const exit_msg& msg) {
      VAST_DEBUG(self, "received exit from", msg.source, "with reason:", msg.reason);
      self->send<message_priority::high>(self->state.index, self->state.id, 0);
      self->send(self->state.sink, sys_atom::value, delete_atom::value);
      self->send_exit(self->state.sink, msg.reason);
      self->quit(msg.reason);
      if (msg.reason != exit_reason::kill)
        report_statistics(self);
    }
  );
  self->set_down_handler(
    [=](const down_msg& msg) {
      VAST_DEBUG(self, "received DOWN from", msg.source);
      if (has_continuous_option(self->state.options)
          && (msg.source == self->state.archive
              || msg.source == self->state.index))
        report_statistics(self);
    }
  );
  auto finished = [](const query_status& qs) -> bool {
    return qs.received == qs.expected
           && qs.lookups_issued == qs.lookups_complete;
  };
  auto handle_batch = [=](std::vector<event> candidates) {
    auto& st = self->state;
    VAST_DEBUG(self, "got batch of", candidates.size(), "events");
    auto sender = self->current_sender();
    for (auto& candidate : candidates) {
      auto& checker = st.checkers[candidate.type()];
      // Construct a candidate checker if we don't have one for this type.
      if (caf::holds_alternative<caf::none_t>(checker)) {
        auto x = tailor(st.expr, candidate.type());
        if (!x) {
          VAST_ERROR(self, "failed to tailor expression:",
                     self->system().render(x.error()));
          ship_results(self);
          self->send_exit(self, exit_reason::normal);
          return;
        }
        checker = std::move(*x);
        VAST_DEBUG(self, "tailored AST to", candidate.type() << ':', checker);
      }
      // Perform candidate check and keep event as result on success.
      if (caf::visit(event_evaluator{candidate}, checker))
        st.results.push_back(std::move(candidate));
      else
        VAST_DEBUG(self, "ignores false positive:", candidate);
    }
    st.query.processed += candidates.size();
    ship_results(self);
  };
  return {
    // The INDEX (or the EVALUATOR, to be more precise) sends us a series of
    // `ids` in response to an expression (query), terminated by 'done'.
    [=](ids& hits) -> caf::result<void> {
      auto& st = self->state;
      // Skip results that arrive before we got our lookup handle from the
      // INDEX actor.
      if (st.query.expected == 0)
        return caf::skip;
      // Add `hits` to the total result set and update all stats.
      timespan runtime = steady_clock::now() - st.start;
      st.query.runtime = runtime;
      auto count = rank(hits);
      if (st.accountant) {
        auto r = report{};
        if (st.hits.empty())
          r.push_back({"exporter.hits.first", runtime});
        r.push_back({"exporter.hits.arrived", runtime});
        r.push_back({"exporter.hits.count", count});
        self->send(st.accountant, r);
      }
      if (count == 0) {
        VAST_WARNING(self, "got empty hits");
      } else {
        VAST_DEBUG(self, "got", count, "index hits in [", (select(hits, 1)),
                   ',', (select(hits, -1) + 1), ')');
        st.hits |= hits;
        VAST_DEBUG(self, "forwards hits to archive");
        // FIXME: restrict according to configured limit.
        ++st.query.lookups_issued;
        self->send(st.archive, std::move(hits));
      }
      return caf::unit;
    },
    [=](table_slice_ptr slice) {
      handle_batch(to_events(*slice, self->state.hits));
    },
    [=](done_atom) {
      // Figure out if we're done by bumping the counter for `received` and
      // check whether it reaches `expected`.
      auto& st = self->state;
      timespan runtime = steady_clock::now() - st.start;
      st.query.runtime = runtime;
      st.query.received += st.query.scheduled;
      if (st.query.received < st.query.expected) {
        VAST_DEBUG(self, "received", st.query.received, '/',
                   st.query.expected, "ID sets");
        request_more_hits(self);
      } else {
        VAST_DEBUG(self, "received all", st.query.expected,
                   "ID set(s) in", vast::to_string(runtime));
        if (st.accountant)
          self->send(st.accountant, "exporter.hits.runtime", runtime);
        if (finished(st.query))
          shutdown(self);
      }
    },
    [=](done_atom, const caf::error& err) {
      auto& st = self->state;
      VAST_DEBUG(self, "received done:", VAST_ARG(err),
                 VAST_ARG("query", st.query));
      auto sender = self->current_sender();
      if (sender == st.archive) {
        if (err)
          VAST_DEBUG(self, "received error from archive:",
              self->system().render(err));
        ++st.query.lookups_complete;
      }
      if (finished(st.query))
        shutdown(self);
      else
        request_more_hits(self);
    },
    [=](extract_atom) {
      auto& qs = self->state.query;
      // Sanity check.
      VAST_DEBUG(self, "got request to extract all events");
      if (qs.requested == max_events) {
        VAST_WARNING(self, "ignores extract request, already getting all");
        return;
      }
      // Configure state to get all remaining partition results.
      qs.requested = max_events;
      ship_results(self);
      request_more_hits(self);
    },
    [=](extract_atom, uint64_t requested_results) {
      auto& qs = self->state.query;
      // Sanity checks.
      if (requested_results == 0) {
        VAST_WARNING(self, "ignores extract request for 0 results");
        return;
      }
      if (qs.requested == max_events) {
        VAST_WARNING(self, "ignores extract request, already getting all");
        return;
      }
      VAST_ASSERT(qs.requested < max_events);
      // Configure state to get up to `requested_results` more events.
      auto n = std::min(max_events - requested_results, requested_results);
      VAST_DEBUG(self, "got a request to extract", n,
                 "more results in addition to", qs.requested,
                 "pending results");
      qs.requested += n;
      ship_results(self);
      request_more_hits(self);
    },
    [=](status_atom) {
      auto result = self->state.status();
      detail::fill_status_map(result, self);
      return result;
    },
    [=](const archive_type& archive) {
      VAST_DEBUG(self, "registers archive", archive);
      self->state.archive = archive;
      if (has_continuous_option(self->state.options))
        self->monitor(archive);
      // Register self at the archive
      if (has_historical_option(self->state.options))
        self->send(archive, exporter_atom::value, self);
    },
    [=](index_atom, const actor& index) {
      VAST_DEBUG(self, "registers index", index);
      self->state.index = index;
      if (has_continuous_option(self->state.options))
        self->monitor(index);
    },
    [=](sink_atom, const actor& sink) {
      VAST_DEBUG(self, "registers sink", sink);
      self->send(self->state.sink, sys_atom::value, put_atom::value, sink);
      self->monitor(self->state.sink);
    },
    [=](importer_atom, const std::vector<actor>& importers) {
      // Register for events at running IMPORTERs.
      if (has_continuous_option(self->state.options))
        for (auto& x : importers)
          self->send(x, exporter_atom::value, self);
    },
    [=](run_atom) {
      VAST_INFO(self, "executes query:", self->state.expr);
      self->state.start = steady_clock::now();
      if (!has_historical_option(self->state.options))
        return;
      self->request(self->state.index, infinite, self->state.expr).then(
        [=](const uuid& lookup, uint32_t partitions, uint32_t scheduled) {
          VAST_DEBUG(self, "got lookup handle", lookup << ", scheduled",
                     scheduled << '/' << partitions, "partitions");
          self->state.id = lookup;
          if (partitions > 0) {
            self->state.query.expected = partitions;
            self->state.query.scheduled = scheduled;
          } else {
            shutdown(self);
          }
        },
        [=](const error& e) {
          shutdown(self, e);
        }
      );
    },
    [=](caf::stream<table_slice_ptr> in) {
      return self->make_sink(
        in,
        [](caf::unit_t&) {
          // nop
        },
        [=](caf::unit_t&, const table_slice_ptr& slice) {
          // TODO: port to new table slice API
          auto candidates = to_events(*slice);
          handle_batch(std::move(candidates));
        },
        [=](caf::unit_t&, const error& err) {
          VAST_IGNORE_UNUSED(err);
          VAST_ERROR(self, "got error during streaming: ", err);
        }
      );
    },
  };
}

} // namespace system
} // namespace vast
