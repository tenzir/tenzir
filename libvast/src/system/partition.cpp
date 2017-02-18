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
      VAST_DEBUG(self, "got", rank(hits), "hits ("
                 << (self->state.got + 1) << '/' << expected << ')',
                 "bitmaps");
      self->state.hits |= hits;
      if (++self->state.got == expected) {
        self->send(evaluator, std::move(self->state.pred), self->state.hits);
        self->quit();
      }
    }
  };
}

struct evaluator_state {
  bitmap hits;
  std::unordered_map<predicate, std::pair<bitmap, bool>> predicates;
  const char* name = "evaluator";
};

// Wraps a query expression in an actor. Upon receiving hits from predicators,
// re-evaluates the expression and relays new hits to its sinks.
behavior evaluator(stateful_actor<evaluator_state>* self,
                   expression expr, actor sink) {
  for (auto& pred : visit(predicatizer{}, expr))
    self->state.predicates[pred].second = false;
  auto visitor = make_bitmap_evaluator<bitmap>(
    [=](const predicate& pred) -> const bitmap* {
      auto& bm = self->state.predicates[pred];
      return bm.second ? &bm.first : nullptr;
    }
  );
  return {
    [=](const predicate& pred, bitmap& hits) {
      VAST_DEBUG(self, "evaluates", expr);
      self->state.predicates[pred] = {std::move(hits), true};
      auto new_hits = visit(visitor, expr);
      auto delta = new_hits - self->state.hits;
      if (!delta.empty() && !all<0>(delta)) {
        VAST_DEBUG(self, "relays", rank(delta), "hits");
        self->state.hits |= delta;
        self->send(sink, std::move(delta));
      }
      // We're done with evaluation if all predicates are done.
      auto i = std::adjacent_find(self->state.predicates.begin(),
                                  self->state.predicates.end(),
                                  [](auto& x, auto& y) {
                                    return x.second.second != y.second.second;
                                  });
      if (i == self->state.predicates.end()) {
        VAST_DEBUG(self, "completed evaluation of expression", expr);
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
  // If the directory exists already, we must have some state and are loading
  // all INDEXERs.
  if (exists(dir / "meta")) {
    std::vector<std::pair<std::string, type>> indexers;
    auto result = load(dir / "meta", indexers);
    if (!result) {
      VAST_ERROR(self, self->system().render(result.error()));
      self->quit(result.error());
      return {};
    } else {
      self->state.indexers.reserve(indexers.size());
      for (auto& x : indexers) {
        auto indexer = self->spawn(event_indexer, dir / x.first, x.second);
        self->state.indexers.emplace(x.second, indexer);
      }
    }
  }
  return {
    [=](std::vector<event> const& events) {
      VAST_ASSERT(!events.empty());
      VAST_DEBUG(self, "got", events.size(), "events");
      // Locate relevant indexers.
      vast::detail::flat_set<actor> indexers;
      for (auto& e : events) {
        auto& i = self->state.indexers[e.type()];
        if (!i)
          i = self->spawn(event_indexer, dir / to_digest(e.type()), e.type());
        indexers.insert(i);
      }
      // Forward events to all indexers.
      auto msg = self->current_mailbox_element()->move_content_to_message();
      for (auto& indexer : indexers)
        self->send(indexer, msg);
    },
    [=](expression const& expr) {
      VAST_DEBUG(self, "got expression:", expr);
      auto rp = self->make_response_promise<bitmap>();
      auto start = steady_clock::now();
      if (self->state.indexers.empty()) {
        VAST_DEBUG(self, "has no indexers available");
        rp.deliver(bitmap{});
        return;
      }
      // Spawn a sink that accumulates the stream of bitmaps from the evaluator.
      auto accumulator = self->system().spawn(
        [=](event_based_actor* job) mutable -> behavior {
          auto bm = std::make_shared<bitmap>();
          return {
            [=](const bitmap& hits) mutable {
              VAST_ASSERT(!hits.empty() && !all<0>(hits));
              *bm |= hits;
            },
            [=](done_atom) mutable {
              auto stop = steady_clock::now();
              rp.deliver(std::move(*bm));
              auto runtime = stop - start;
              VAST_DEBUG(self, "answered", expr, "in", runtime);
              if (accountant)
                job->send(accountant, "partition.query.runtime", runtime);
            }
          };
        }
      );
      auto eval = self->spawn(evaluator, expr, accumulator);
      // Connect COLLECTORs with INDEXERs and EVALUATOR.
      for (auto& pred : visit(predicatizer{}, expr)) {
        // FIXME: locate the smallest subset of INDEXERs (checking whether the
        // predicate could match the type of the INDEXER) instead of querying
        // all INDEXERs.
        auto n = self->state.indexers.size();
        auto coll = self->spawn(collector, pred, eval, n);
        for (auto& x : self->state.indexers)
          send_as(coll, x.second, pred);
      }
    },
    [=](shutdown_atom) {
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
