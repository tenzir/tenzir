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
#include "vast/concept/printable/vast/event.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/detail/assert.hpp"
#include "vast/event.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/logger.hpp"
#include "vast/table_slice.hpp"
#include "vast/to_events.hpp"

#include "vast/system/archive.hpp"
#include "vast/system/atoms.hpp"
#include "vast/system/exporter.hpp"

#include "vast/detail/assert.hpp"

using namespace std::chrono;
using namespace std::string_literals;
using namespace caf;

namespace vast {
namespace system {

namespace {

void ship_results(stateful_actor<exporter_state>* self) {
  VAST_TRACE("");
  if (self->state.results.empty() || self->state.stats.requested == 0) {
    return;
  }
  VAST_INFO(self, "relays", self->state.results.size(), "events");
  message msg;
  if (self->state.results.size() <= self->state.stats.requested) {
    self->state.stats.requested -= self->state.results.size();
    self->state.stats.shipped += self->state.results.size();
    msg = make_message(std::move(self->state.results));
    self->state.results = {};
  } else {
    std::vector<event> remainder;
    remainder.reserve(self->state.results.size() - self->state.stats.requested);
    auto begin = self->state.results.begin() + self->state.stats.requested;
    auto end = self->state.results.end();
    std::move(begin, end, std::back_inserter(remainder));
    self->state.results.resize(self->state.stats.requested);
    msg = make_message(std::move(self->state.results));
    self->state.results = std::move(remainder);
    self->state.stats.shipped += self->state.stats.requested;
    self->state.stats.requested = 0;
  }
  self->send(self->state.sink, msg);
}

void report_statistics(stateful_actor<exporter_state>* self) {
  timespan runtime = steady_clock::now() - self->state.start;
  self->state.stats.runtime = runtime;
  VAST_INFO(self, "completed in", runtime);
  self->send(self->state.sink, self->state.id, self->state.stats);
  if (self->state.accountant) {
    auto hits = rank(self->state.hits);
    auto processed = self->state.stats.processed;
    auto shipped = self->state.stats.shipped;
    auto results = shipped + self->state.results.size();
    auto selectivity = double(results) / hits;
    self->send(self->state.accountant, "exporter.hits", hits);
    self->send(self->state.accountant, "exporter.processed", processed);
    self->send(self->state.accountant, "exporter.results", results);
    self->send(self->state.accountant, "exporter.shipped", shipped);
    self->send(self->state.accountant, "exporter.selectivity", selectivity);
    self->send(self->state.accountant, "exporter.runtime", runtime);
  }
}

void shutdown(stateful_actor<exporter_state>* self, caf::error err) {
  VAST_DEBUG(self, "initiates shutdown with error", self->system().render(err));
  self->send_exit(self, std::move(err));
}

void shutdown(stateful_actor<exporter_state>* self) {
  if (rank(self->state.unprocessed) > 0 || !self->state.results.empty()
      || has_continuous_option(self->state.options))
    return;
  VAST_DEBUG(self, "initiates shutdown");
  self->send_exit(self, exit_reason::normal);
}

void request_more_hits(stateful_actor<exporter_state>* self) {
  if (!has_historical_option(self->state.options))
    return;
  auto waiting_for_hits =
    self->state.stats.received == self->state.stats.scheduled;
  auto need_more_results = self->state.stats.requested > 0;
  auto have_no_inflight_requests = any<1>(self->state.unprocessed);
  // If we're (1) no longer waiting for index hits, (2) still need more
  // results, and (3) have no inflight requests to the archive, we ask
  // the index for more hits.
  if (!waiting_for_hits && need_more_results && have_no_inflight_requests) {
    auto remaining = self->state.stats.expected - self->state.stats.received;
    VAST_ASSERT(remaining > 0);
    // TODO: Figure out right number of partitions to ask for. For now, we
    // bound the number by an arbitrary constant.
    auto n = std::min(remaining, size_t{2});
    VAST_DEBUG(self, "asks index to process", n, "more partitions");
    self->send(self->state.index, self->state.id, n);
  }
}

} // namespace <anonymous>

behavior exporter(stateful_actor<exporter_state>* self, expression expr,
                  query_options options) {
  auto eu = self->system().dummy_execution_unit();
  self->state.sink = actor_pool::make(eu, actor_pool::broadcast());
  if (auto a = self->system().registry().get(accountant_atom::value))
    self->state.accountant = actor_cast<accountant_type>(a);
  self->state.options = options;
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
  auto handle_batch = [=](std::vector<event>& candidates) {
    VAST_DEBUG(self, "got batch of", candidates.size(), "events");
    // Events can arrive in any order: sort them by ID first. Otherwise, we
    // can't compute the bitmap mask as easily.
    std::sort(candidates.begin(), candidates.end(),
              [](auto& x, auto& y) { return x.id() < y.id(); });
    bitmap mask;
    auto sender = self->current_sender();
    for (auto& candidate : candidates) {
      auto& checker = self->state.checkers[candidate.type()];
      // Construct a candidate checker if we don't have one for this type.
      if (caf::holds_alternative<caf::none_t>(checker)) {
        auto x = tailor(expr, candidate.type());
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
      // Append ID to our bitmap mask.
      if (sender == self->state.archive) {
        mask.append_bits(false, candidate.id() - mask.size());
        mask.append_bit(true);
      }
      // Perform candidate check and keep event as result on success.
      if (caf::visit(event_evaluator{candidate}, checker))
        self->state.results.push_back(std::move(candidate));
      else
        VAST_DEBUG(self, "ignores false positive:", candidate);
    }
    self->state.stats.processed += candidates.size();
    if (sender == self->state.archive)
      self->state.unprocessed -= mask;
    ship_results(self);
    request_more_hits(self);
    if (self->state.stats.received == self->state.stats.expected)
      shutdown(self);
  };
  return {
    // The INDEX (or the EVALUATOR, to be more precise) sends us a series of
    // `ids` in response to an expression (query), terminated by 'done'.
    [=](ids& hits) {
      // Add `hits` to the total result set and update all stats.
      auto& st = self->state;
      timespan runtime = steady_clock::now() - st.start;
      st.stats.runtime = runtime;
      auto count = rank(hits);
      if (st.accountant) {
        if (st.hits.empty())
          self->send(st.accountant, "exporter.hits.first", runtime);
        self->send(st.accountant, "exporter.hits.arrived", runtime);
        self->send(st.accountant, "exporter.hits.count", count);
      }
      if (count == 0) {
        VAST_WARNING(self, "got an empty delta from INDEX lookup");
      } else {
        VAST_DEBUG(self, "got", count, "index hits in [", (select(hits, 1)),
                   ',', (select(hits, -1) + 1), ')');
        st.hits |= hits;
        st.unprocessed |= hits;
        VAST_DEBUG(self, "forwards hits to archive");
        // FIXME: restrict according to configured limit.
        self->send(st.archive, std::move(hits));
      }
    },
    [=](done_atom) {
      // Figure out if we're done by bumping the counter for `received` and
      // check whether it reaches `expected`.
      auto& st = self->state;
      timespan runtime = steady_clock::now() - st.start;
      st.stats.runtime = runtime;
      st.stats.received += st.stats.scheduled;
      if (st.stats.received < st.stats.expected) {
        VAST_DEBUG(self, "received", self->state.stats.received, '/',
                   self->state.stats.expected, "ID sets");
        request_more_hits(self);
      } else {
        VAST_DEBUG(self, "received all", self->state.stats.expected,
                   "ID set(s) in", runtime);
        if (self->state.accountant)
          self->send(self->state.accountant, "exporter.hits.runtime", runtime);
        shutdown(self);
      }
    },
    [=](std::vector<event>& candidates) {
      handle_batch(candidates);
    },
    [=](extract_atom) {
      if (self->state.stats.requested == max_events) {
        VAST_WARNING(self, "ignores extract request, already getting all");
        return;
      }
      self->state.stats.requested = max_events;
      ship_results(self);
      request_more_hits(self);
    },
    [=](extract_atom, uint64_t requested) {
      if (self->state.stats.requested == max_events) {
        VAST_WARNING(self, "ignores extract request, already getting all");
        return;
      }
      auto n = std::min(max_events - requested, requested);
      self->state.stats.requested += n;
      VAST_DEBUG(self, "got request to extract", n, "new events in addition to",
                 self->state.stats.requested, "pending results");
      ship_results(self);
      request_more_hits(self);
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
      VAST_INFO(self, "executes query", expr);
      self->state.start = steady_clock::now();
      if (!has_historical_option(self->state.options))
        return;
      self->request(self->state.index, infinite, expr).then(
        [=](const uuid& lookup, size_t partitions, size_t scheduled) {
          VAST_DEBUG(self, "got lookup handle", lookup << ", scheduled",
                     scheduled << '/' << partitions, "partitions");
          self->state.id = lookup;
          if (partitions > 0) {
            self->state.stats.expected = partitions;
            self->state.stats.scheduled = scheduled;
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
          handle_batch(candidates);
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
