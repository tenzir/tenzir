#include <caf/all.hpp>

#include "vast/bitmap.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/filesystem.hpp"
#include "vast/concept/printable/vast/event.hpp"
#include "vast/detail/assert.hpp"
#include "vast/event.hpp"
#include "vast/expression.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/load.hpp"
#include "vast/logger.hpp"
#include "vast/save.hpp"
#include "vast/time.hpp"

#include "vast/system/accountant.hpp"
#include "vast/system/atoms.hpp"
#include "vast/system/indexer.hpp"
#include "vast/system/partition.hpp"
#include "vast/system/task.hpp"

#include "vast/detail/flat_set.hpp"

using namespace std::chrono;
using namespace caf;

namespace vast {
namespace system {

namespace {

template <class T>
auto to_digest(const T& x) {
  return to_string(std::hash<T>{}(x));
}

struct collector_state {
  bitmap hits;
  size_t got = 0;
  predicate pred;
  std::string name = "collector";
};

// Encapsulates a single predicate that is part of one or more expressions.
// The COLLECTOR receives hits from INDEXERs and relays them to the EVALUATORs
// after having received all hits for the given predicate.
behavior collector(stateful_actor<collector_state>* self, predicate pred,
                   actor evaluator, size_t expected) {
  self->state.name += '[' + to_string(pred) + ']';
  self->state.pred = std::move(pred);
  return {
    [=](const bitmap& hits) {
      VAST_DEBUG(self, "got", rank(hits), "hits,",
                 (self->state.got + 1) << '/' << expected, "bitmaps");
      self->state.hits |= hits;
      if (++self->state.got == expected) {
        VAST_DEBUG(self, "relays", rank(self->state.hits), "to evaluator");
        self->send(evaluator, std::move(self->state.pred), self->state.hits);
        self->quit();
      }
    }
  };
}

struct bitmap_evaluator {
  bitmap_evaluator(const std::unordered_map<predicate, bitmap>& bitmaps)
    : bitmaps{bitmaps} {
    // nop
  }

  bitmap operator()(none) const {
    return {};
  }

  bitmap operator()(conjunction const& c) const {
    auto bm = visit(*this, c[0]);
    if (bm.empty() || all<0>(bm))
      return {};
    for (size_t i = 1; i < c.size(); ++i) {
      bm &= visit(*this, c[i]);
      if (bm.empty() || all<0>(bm)) // short-circuit
        return {};
    }
    return bm;
  }

  bitmap operator()(disjunction const& d) const {
    bitmap bm;
    for (auto& op : d) {
      bm |= visit(*this, op);
      if (all<1>(bm)) // short-circuit
        break;
    }
    return bm;
  }

  bitmap operator()(negation const& n) const {
    auto bm = visit(*this, n.expr());
    bm.flip();
    return bm;
  }

  bitmap operator()(predicate const& pred) const {
    auto i = bitmaps.find(pred);
    return i != bitmaps.end() ? i->second : bitmap{};
  }

  const std::unordered_map<predicate, bitmap>& bitmaps;
};

struct evaluator_state {
  bitmap hits;
  std::unordered_map<predicate, bitmap> predicates;
  const char* name = "evaluator";
};

// Wraps a query expression in an actor. Upon receiving hits from COLLECTORs,
// re-evaluates the expression and relays new hits to its sinks.
behavior evaluator(stateful_actor<evaluator_state>* self,
                   expression expr, size_t num_predicates, actor sink) {
  return {
    [=](predicate& pred, bitmap& hits) {
      self->state.predicates.emplace(std::move(pred), std::move(hits));
      auto expr_hits = visit(bitmap_evaluator{self->state.predicates}, expr);
      auto delta = expr_hits - self->state.hits;
      VAST_DEBUG(self, "evaluated",
                 self->state.predicates.size() << '/' << num_predicates,
                 "predicates, yielding",
                 rank(delta) << '/' << rank(expr_hits),
                 "new/total hits for", expr);
      if (any<1>(delta)) {
        VAST_DEBUG(self, "relays", rank(delta), "new hits to sink");
        self->state.hits |= delta;
        self->send(sink, std::move(delta));
      }
      // We're done with evaluation if all predicates have reported their hits.
      if (self->state.predicates.size() == num_predicates) {
        VAST_DEBUG(self, "completed expression evaluation");
        self->send(sink, done_atom::value);
        self->quit();
      }
    }
  };
}

} // namespace <anonymous>

behavior partition(stateful_actor<partition_state>* self, path dir) {
  auto accountant = accountant_type{};
  if (auto a = self->system().registry().get(accountant_atom::value))
    accountant = actor_cast<accountant_type>(a);
  // If the directory exists already, we must have some state from the past and
  // are pre-loading all INDEXER types we are aware of, so that we can spawn
  // them as we need them.
  if (exists(dir)) {
    std::vector<std::pair<std::string, type>> indexers;
    auto result = load(dir / "meta", indexers);
    if (!result) {
      VAST_ERROR(self, self->system().render(result.error()));
      self->quit(result.error());
      return {};
    }
    self->state.indexers.reserve(indexers.size());
    for (auto& x : indexers)
      self->state.indexers.emplace(x.second, actor{});
  }
  return {
    [=](std::vector<event> const& events) {
      VAST_ASSERT(!events.empty());
      VAST_DEBUG(self, "got", events.size(), "events");
      // Locate relevant indexers.
      vast::detail::flat_set<actor> indexers;
      for (auto& e : events) {
        auto& i = self->state.indexers[e.type()];
        if (!i) {
          VAST_DEBUG(self, "creates event-indexer for type", e.type());
          i = self->spawn(event_indexer, dir / to_digest(e.type()), e.type());
        }
        indexers.insert(i);
      }
      // Forward events to relevant indexers.
      auto msg = self->current_mailbox_element()->move_content_to_message();
      for (auto& indexer : indexers)
        self->send(indexer, msg);
    },
    [=](expression const& expr) {
      VAST_DEBUG(self, "got expression:", expr);
      auto start = steady_clock::now();
      auto rp = self->make_response_promise<bitmap>();
      // For each known type, check whether the expression could match.
      // If so, locate/load the corresponding indexer.
      std::vector<actor> indexers;
      for (auto& x : self->state.indexers) {
        auto resolved = visit(type_resolver{x.first}, expr);
        if (resolved && visit(matcher{x.first}, *resolved)) {
          VAST_DEBUG(self, "found matching type for expression:", x.first);
          if (!x.second) {
            VAST_DEBUG(self, "loads event-indexer for type", x.first);
            auto indexer_dir = dir / to_digest(x.first);
            x.second = self->spawn(event_indexer, indexer_dir, x.first);
          }
          indexers.push_back(x.second);
        }
      }
      if (indexers.empty()) {
        VAST_DEBUG(self, "did not find a matching type in",
                   self->state.indexers.size(), "indexer(s)");
        rp.deliver(bitmap{});
        return;
      }
      // Spawn a sink that accumulates the stream of bitmaps from the evaluator
      // and ultimately responds to the user with the result.
      auto accumulator = self->system().spawn(
        [=](event_based_actor* job) mutable -> behavior {
          auto result = std::make_shared<bitmap>();
          return {
            [=](const bitmap& hits) mutable {
              VAST_ASSERT(any<1>(hits));
              *result |= hits;
            },
            [=](done_atom) mutable {
              auto stop = steady_clock::now();
              rp.deliver(std::move(*result));
              timespan runtime = stop - start;
              VAST_DEBUG(self, "answered", expr, "in", runtime);
              if (accountant)
                job->send(accountant, "partition.query.runtime", runtime);
              job->quit();
            }
          };
        }
      );
      // Spawn a dedicated actor responsible for expression evaluation. This
      // actor re-evaluates the expression whenever it receives new hits from
      // a collector.
      auto predicates = visit(predicatizer{}, expr);
      auto eval = self->spawn(evaluator, expr, predicates.size(), accumulator);
      for (auto& pred : predicates) {
        // FIXME: locate the smallest subset of indexers (checking whether the
        // predicate could match the type of the indexer) instead of querying
        // all indexers.
        auto coll = self->spawn(collector, pred, eval, indexers.size());
        for (auto& x : indexers)
          send_as(coll, x, pred);
      }
    },
    [=](shutdown_atom) {
      for (auto i = self->state.indexers.begin();
           i != self->state.indexers.end(); )
        if (!i->second)
          i = self->state.indexers.erase(i);
        else
          ++i;
      if (self->state.indexers.empty()) {
        self->quit(exit_reason::user_shutdown);
        return;
      }
      for (auto& x : self->state.indexers) {
        self->monitor(x.second);
        self->send(x.second, shutdown_atom::value);
      }
      self->set_down_handler(
        [=](const down_msg& msg) {
          auto pred = [&](auto& x) { return x.second == msg.source; };
          auto i = std::find_if(self->state.indexers.begin(),
                                self->state.indexers.end(), pred);
          VAST_ASSERT(i != self->state.indexers.end());
          self->state.indexers.erase(i);
          if (self->state.indexers.empty())
            self->quit(exit_reason::user_shutdown);
        }
      );
      // Save persistent state.
      // TODO: only do so when the partition got dirty.
      std::vector<std::pair<std::string, type>> indexers;
      indexers.reserve(self->state.indexers.size());
      for (auto& x : self->state.indexers)
        indexers.emplace_back(to_digest(x.first), x.first);
      if (!exists(dir))
        mkdir(dir);
      auto result = save(dir / "meta", indexers);
      if (!result)
        self->quit(result.error());
    },
  };
}

} // namespace system
} // namespace vast
