#include "vast/event.hpp"
#include "vast/logger.hpp"
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/vast/event.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/detail/assert.hpp"
#include "vast/expression_visitors.hpp"

#include "vast/system/atoms.hpp"
#include "vast/system/exporter.hpp"

using namespace std::chrono;
using namespace std::string_literals;
using namespace caf;

namespace vast {
namespace system {

namespace {

template <class Actor>
void ship_results(Actor* self) {
  if (self->state.results.empty() || self->state.requested == 0)
    return;
  VAST_DEBUG(self, "relays", self->state.results.size(), "events");
  message msg;
  if (self->state.results.size() <= self->state.requested) {
    self->state.requested -= self->state.results.size();
    self->state.shipped += self->state.results.size();
    msg = make_message(std::move(self->state.results));
  } else {
    std::vector<event> remainder;
    remainder.reserve(self->state.results.size() - self->state.requested);
    auto begin = self->state.results.begin() + self->state.requested;
    auto end = self->state.results.end();
    std::move(begin, end, std::back_inserter(remainder));
    self->state.results.resize(self->state.requested);
    msg = make_message(std::move(self->state.results));
    self->state.results = std::move(remainder);
    self->state.shipped += self->state.requested;
    self->state.requested = 0;
  }
  for (auto& s : self->state.sinks)
    self->send(s, msg);
}

template <class Actor>
void complete(Actor* self) {
  timespan runtime = steady_clock::now() - self->state.start;
  for (auto& s : self->state.sinks)
    self->send(s, self->state.id, done_atom::value, runtime);
  VAST_DEBUG(self, "completed in", runtime);
  if (self->state.accountant) {
    auto hits = rank(self->state.hits);
    auto processed = self->state.processed;
    auto shipped = self->state.shipped;
    auto results = shipped + self->state.results.size();
    auto selectivity = double(results) / hits;
    self->send(self->state.accountant, "exporter.hits", hits);
    self->send(self->state.accountant, "exporter.processed", processed);
    self->send(self->state.accountant, "exporter.results", results);
    self->send(self->state.accountant, "exporter.shipped", shipped);
    self->send(self->state.accountant, "exporter.selectivity", selectivity);
    self->send(self->state.accountant, "exporter.runtime", runtime);
  }
  self->quit();
}

} // namespace <anonymous>

behavior exporter(stateful_actor<exporter_state>* self, expression expr,
                  query_options opts) {
  // Register the accountant, if available.
  auto acc = self->system().registry().get(accountant_atom::value);
  if (acc) {
    VAST_DEBUG(self, "registers accountant", acc);
    self->state.accountant = actor_cast<accountant_type>(acc);
  }
  self->set_down_handler(
    [=](down_msg const& msg) {
      VAST_DEBUG("got DOWN from", msg.source);
      if (self->state.archive == msg.source)
        self->state.archive = archive_type{};
      if (self->state.index == msg.source)
        self->state.index = {};
      if (self->state.sinks.erase(actor_cast<actor>(msg.source)) > 0)
        return;
    }
  );
  auto operating = behavior{
    [=](bitmap& hits) {
      timespan runtime = steady_clock::now() - self->state.start;
      auto count = rank(hits);
      if (self->state.accountant) {
        if (self->state.hits.empty())
          self->send(self->state.accountant, "exporter.hits.first", runtime);
        self->send(self->state.accountant, "exporter.hits.arrived", runtime);
        self->send(self->state.accountant, "exporter.hits.count", count);
      }
      VAST_DEBUG(self, "got", rank(hits), "index hits in ["
                 << select(hits, 1) << ',' << (select(hits, -1) + 1) << ')');
      self->state.hits |= hits;
      self->state.unprocessed |= hits;
      VAST_DEBUG(self, "forwards hits to archive");
      // FIXME: restrict according to configured limit.
      self->send(self->state.archive, std::move(hits));
    },
    [=](std::vector<event>& candidates) {
      VAST_DEBUG(self, "got batch of", candidates.size(), "events");
      bitmap mask;
      for (auto& candidate : candidates) {
        auto& checker = self->state.checkers[candidate.type()];
        // Construct a candidate checker if we don't have one for this type.
        if (is<none>(checker)) {
          auto x = visit(key_resolver{candidate.type()}, expr);
          VAST_ASSERT(x);
          checker = visit(type_resolver{candidate.type()}, *x);
          VAST_ASSERT(!is<none>(checker));
          VAST_DEBUG(self, "resolved AST for", candidate.type() << ':',
                     checker);
        }
        // Perform candidate check and keep event as result on success.
        if (visit(event_evaluator{candidate}, checker)) {
          self->state.results.push_back(std::move(candidate));
        } else {
          VAST_DEBUG(self, "ignores false positive:", candidate);
        }
        mask.append_bits(false, candidate.id() - mask.size());
        mask.append_bit(true);
      }
      self->state.processed += candidates.size();
      self->state.unprocessed -= mask;
      ship_results(self);
    },
    [=](extract_atom) {
      if (self->state.requested == max_events) {
        VAST_WARNING(self, "ignores extract request, already getting all");
        return;
      }
      self->state.requested = max_events;
      ship_results(self);
    },
    [=](extract_atom, uint64_t requested) {
      if (self->state.requested == max_events) {
        VAST_WARNING(self, "ignores extract request, already getting all");
        return;
      }
      auto n = std::min(max_events - requested, requested);
      self->state.requested += n;
      VAST_DEBUG(self, "got request to extract", n, "new events in addition to",
                 self->state.requested, "pending results");
      ship_results(self);
    },
    [=](progress_atom, uint64_t remaining, uint64_t total) {
      self->state.progress = (total - double(remaining)) / total;
      for (auto& s : self->state.sinks)
        self->send(s, self->state.id, progress_atom::value,
                   self->state.progress, total);
    },
    [=](done_atom, timespan runtime, expression const&) {
      VAST_DEBUG(self, "completed index interaction in", runtime);
      if (self->state.accountant)
        self->send(self->state.accountant, "exporter.hits.runtime", runtime);
      if (rank(self->state.unprocessed) == 0 && self->state.results.empty())
        complete(self);
    },
  };
  return {
    [=](archive_type const& archive) {
      VAST_DEBUG(self, "registers archive", archive);
      VAST_ASSERT(!self->state.archive);
      self->monitor(archive);
      self->state.archive = archive;
    },
    [=](put_atom, index_atom, actor const& index) {
      VAST_DEBUG(self, "registers index", index);
      VAST_ASSERT(!self->state.index);
      self->monitor(index);
      self->state.index = index;
    },
    [=](put_atom, sink_atom, actor const& sink) {
      VAST_DEBUG(self, "registers sink", sink);
      VAST_ASSERT(self->state.sinks.count(sink) == 0);
      self->monitor(sink);
      self->state.sinks.insert(sink);
    },
    [=](run_atom) {
      VAST_INFO(self, "executes query", expr);
      self->state.start = steady_clock::now();
      if (!self->state.archive || !self->state.index) {
        VAST_ERROR(self, "needs archive and index to perform query");
        self->quit(make_error(ec::unspecified, "archive/index not provided"));
        return;
      }
      self->send(self->state.index, expr, opts, self);
      self->set_default_handler(skip);
      self->become(
        [=](actor const& task) {
          VAST_DEBUG(self, "received task from index");
          self->send(task, subscriber_atom::value, self);
          self->become(operating);
        }
      );
    }
  };
}

} // namespace system
} // namespace vast
