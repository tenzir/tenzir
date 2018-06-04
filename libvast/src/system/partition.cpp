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

#include "vast/system/partition.hpp"

#include <caf/event_based_actor.hpp>
#include <caf/local_actor.hpp>
#include <caf/stateful_actor.hpp>

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/detail/assert.hpp"
#include "vast/event.hpp"
#include "vast/ids.hpp"
#include "vast/load.hpp"
#include "vast/logger.hpp"
#include "vast/save.hpp"
#include "vast/system/atoms.hpp"
#include "vast/system/indexer.hpp"
#include "vast/time.hpp"

using namespace std::chrono;
using namespace caf;

namespace vast {
namespace system {

namespace {

struct collector_state {
  ids hits;
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
    [=](const ids& hits) {
      VAST_DEBUG(self, "got", rank(hits), "hits,",
                 (self->state.got + 1) << '/' << expected, "ID sets");
      self->state.hits |= hits;
      if (++self->state.got == expected) {
        VAST_DEBUG(self, "relays", rank(self->state.hits), "hits to evaluator");
        self->send(evaluator, std::move(self->state.pred), self->state.hits);
        self->quit();
      }
    }
  };
}

struct ids_evaluator {
  ids_evaluator(const std::unordered_map<predicate, ids>& xs)
    : xs{xs} {
    // nop
  }

  ids operator()(none) const {
    return {};
  }

  ids operator()(const conjunction& c) const {
    auto result = visit(*this, c[0]);
    if (result.empty() || all<0>(result))
      return {};
    for (size_t i = 1; i < c.size(); ++i) {
      result &= visit(*this, c[i]);
      if (result.empty() || all<0>(result)) // short-circuit
        return {};
    }
    return result;
  }

  ids operator()(const disjunction& d) const {
    ids result;
    for (auto& op : d) {
      result |= visit(*this, op);
      if (all<1>(result)) // short-circuit
        break;
    }
    return result;
  }

  ids operator()(const negation& n) const {
    auto result = visit(*this, n.expr());
    result.flip();
    return result;
  }

  ids operator()(const predicate& pred) const {
    auto i = xs.find(pred);
    return i != xs.end() ? i->second : ids{};
  }

  const std::unordered_map<predicate, ids>& xs;
};

struct evaluator_state {
  ids hits;
  std::unordered_map<predicate, ids> predicates;
  static inline const char* name = "evaluator";
};

// Wraps a query expression in an actor. Upon receiving hits from COLLECTORs,
// re-evaluates the expression and relays new hits to its sinks.
behavior evaluator(stateful_actor<evaluator_state>* self,
                   expression expr, size_t num_predicates, actor sink) {
  return {
    [=](predicate& pred, ids& hits) {
      self->state.predicates.emplace(std::move(pred), std::move(hits));
      auto expr_hits = visit(ids_evaluator{self->state.predicates}, expr);
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

/*
behavior partition(stateful_actor<partition_state>* self, path dir) {
  auto accountant = accountant_type{};
  if (auto a = self->system().registry().get(accountant_atom::value))
    accountant = actor_cast<accountant_type>(a);
  // If the directory exists already, we must have some state from the past and
  // are pre-loading all INDEXER types we are aware of, so that we can spawn
  // them as we need them.
  if (exists(dir)) {
    if (auto result = load(dir / "meta", self->state.meta_data); !result) {
      VAST_ERROR(self, self->system().render(result.error()));
      self->quit(result.error());
    } else {
      for (auto& [str, t] : self->state.meta_data.types)
        self->state.indexers.emplace(t, actor{});
    }
  }
  return {
    [=](const std::vector<event>& events) {
      VAST_ASSERT(!events.empty());
      VAST_DEBUG(self, "got", events.size(), "events");
      // Locate relevant indexers.
      vast::detail::flat_set<actor> indexers;
      for (auto& e : events) {
        auto& a = self->state.indexers[e.type()];
        if (!a) {
          VAST_DEBUG(self, "creates event-indexer for type", e.type());
          auto digest = to_digest(e.type());
          a = self->spawn(indexer, dir / digest, e.type());
          if (self->state.meta_data.types.count(digest) == 0) {
            self->state.meta_data.types.emplace(digest, e.type());
            self->state.meta_data.dirty = true;
          }
        }
        indexers.insert(a);
      }
      // Forward events to relevant indexers.
      auto msg = self->current_mailbox_element()->move_content_to_message();
      for (auto& indexer : indexers)
        self->send(indexer, msg);
    },
    [=](const expression& expr) {
      VAST_DEBUG(self, "got expression:", expr);
      auto start = steady_clock::now();
      auto rp = self->make_response_promise<ids>();
      // For each known type, check whether the expression could match.
      // If so, locate/load the corresponding indexer.
      std::vector<actor> indexers;
      for (auto& [t, a] : self->state.indexers) {
        auto resolved = visit(type_resolver{t}, expr);
        if (resolved && visit(matcher{t}, *resolved)) {
          VAST_DEBUG(self, "found matching type for expression:", t);
          if (!a) {
            VAST_DEBUG(self, "loads event-indexer for type", t);
            auto indexer_dir = dir / to_digest(t);
            a = self->spawn(indexer, indexer_dir, t);
          }
          indexers.push_back(a);
        }
      }
      if (indexers.empty()) {
        VAST_DEBUG(self, "did not find a matching type in",
                   self->state.indexers.size(), "indexer(s)");
        rp.deliver(ids{});
        return;
      }
      // Spawn a sink that accumulates the stream of ids from the evaluator
      // and ultimately responds to the user with the result.
      auto accumulator = self->system().spawn(
        [=](event_based_actor* job) mutable -> behavior {
          auto result = std::make_shared<ids>();
          return {
            [=](const ids& hits) mutable {
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
      if (self->state.indexers.empty()) {
        VAST_ASSERT(self->state.meta_data.types.empty());
        self->quit(exit_reason::user_shutdown);
        return;
      }
      // Save persistent state.
      if (self->state.meta_data.dirty) {
        if (!exists(dir))
          mkdir(dir);
        if (auto result = save(dir / "meta", self->state.meta_data); !result)
          self->quit(result.error());
      }
      // Initiate shutdown.
      auto& xs = self->state.indexers;
      for (auto i = xs.begin(); i != xs.end(); ) {
        if (!i->second) {
          i = xs.erase(i);
        } else {
          self->monitor(i->second);
          self->send(i->second, shutdown_atom::value);
          ++i;
        }
      }
      if (xs.empty()) {
        self->quit(exit_reason::user_shutdown);
        return;
      }
      // Terminate not before after all indexers have terminated.
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
    },
  };
}
*/

partition::partition(const path& base_dir, uuid id,
                     indexer_manager::indexer_factory factory)
  : mgr_(*this, std::move(factory)),
    dir_(base_dir / to_string(id)),
    id_(std::move(id)) {
  // If the directory already exists, we must have some state from the past and
  // are pre-loading all INDEXER types we are aware of.
  if (exists(dir_)) {
    auto res = load(meta_file(), meta_data_);
    if (!res)
      VAST_ERROR("unable to read partition meta data:", res.error());
  }
}

partition::~partition() noexcept {
  // Save persistent state.
  if (meta_data_.dirty) {
    if (!exists(dir_))
      mkdir(dir_);
    save(meta_file(), meta_data_);
  }
}

std::vector<type> partition::types() const {
  std::vector<type> result;
  auto& ts = meta_data_.types;
  result.reserve(ts.size());
  std::transform(ts.begin(), ts.end(), std::back_inserter(result),
                 [](auto& kvp) { return kvp.second; });
  return result;
}

path partition::meta_file() const {
  return dir_ / "meta";
}

// -- free functions -----------------------------------------------------------

partition_ptr make_partition(const path& base_dir, uuid id,
                             indexer_manager::indexer_factory f) {
  return caf::make_counted<partition>(base_dir, std::move(id), std::move(f));
}

partition_ptr make_partition(caf::local_actor* self, const path& base_dir,
                             uuid id) {
  auto f = [self](path indexer_path, type indexer_type) {
    VAST_DEBUG(self, "creates event-indexer for type", indexer_type);
    return self->spawn<caf::lazy_init>(indexer, std::move(indexer_path),
                                       std::move(indexer_type));
  };
  return make_partition(base_dir, std::move(id), f);
}

} // namespace system
} // namespace vast

namespace std {

namespace {

using pptr = vast::system::partition_ptr;

} // namespace <anonymous>

size_t hash<pptr>::operator()(const pptr& ptr) const {
  hash<vast::uuid> f;
  return ptr != nullptr ? f(ptr->id()) : 0u;
}

} // namespace std
