#include <caf/all.hpp>

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
#include "vast/expression_visitors.hpp"
#include "vast/load.hpp"
#include "vast/logger.hpp"
#include "vast/save.hpp"
#include "vast/time.hpp"

#include "vast/system/atoms.hpp"
#include "vast/system/indexer.hpp"
#include "vast/system/partition.hpp"
#include "vast/system/task.hpp"

using namespace std::chrono;
using namespace caf;

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(steady_clock::time_point)

namespace vast {
namespace system {

// namespace {
//
// struct accumulator_state {
//   bitmap evaluate(expression const& expr) const {
//     struct evaluator : bitmap_evaluator<evaluator, bitmap> {
//       evaluator(state const* s) : this_{s} { }
//       bitmap const* lookup(predicate const& pred) const {
//         auto i = this_->hits.find(pred);
//         return i == this_->hits.end() ? nullptr : &i->second;
//       }
//       state const* this_;
//     };
//     return visit(evaluator{this}, expr);
//   }
//
//   std::map<predicate, bitmap> hits;
// };
//
// // Receives hits from INDEXERs and evaluates a set of expression over them.
// behavior accumulator(stateful_actor<accumulator_state>* self,
//                      std::vector<expression> exprs, actor const& sink) {
//   return {
//     [=](predicate& pred, bitmap& hits) {
//       self->state.hits.emplace(std::move(pred), std::move(hits));
//     },
//     [self, exprs=std::move(exprs), sink](done_atom) {
//       for (auto& expr : exprs) {
//         VAST_DEBUG(self, "evalutes continuous query:", expr);
//         self->send(sink, expr, self->state.evaluate(expr));
//       }
//       self->quit();
//       // TODO: relay the hits back to PARTITION if the query is also
//       // historical. Caveat: we should not re-evaluate the historical query
//       // with these hits to avoid that the sink receives duplicate hits.
//     }
//   };
// };
//
// struct proxy_state {
//   detail::flat_set<expression> exprs;
//   detail::flat_set<predicate> preds;
//   char const* name = "cq-proxy";
// };
//
// // Accumulates all hits from an event batch, evalutes a query, and sends the
// // result of the evaluation back to PARTITION.
// behavior cq_proxy(stateful_actor<state>* self, actor const& sink) {
//   return {
//     [=](expression const& expr) {
//       self->state.exprs.insert(expr);
//       for (auto& p : visit(predicatizer{}, expr))
//         self->state.preds.insert(std::move(p));
//     },
//     [=](expression const& expr, disable_atom) {
//       self->state.exprs.erase(expr);
//       self->state.preds.clear();
//       if (self->state.exprs.empty())
//         self->quit();
//       else
//         for (auto& ex : self->state.exprs)
//           for (auto& p : visit(predicatizer{}, ex))
//             self->state.preds.insert(std::move(p));
//     },
//     [=](predicate const&, bitmap const& hits) {
//       VAST_DEBUG(self, "relays", rank(hits), "hits");
//       auto msg = self->current_message() + make_message(continuous_atom::value);
//       self->send(sink, std::move(msg));
//     },
//     [=](std::vector<actor> const& indexers) {
//       VAST_DEBUG(self, "got", indexers.size(), "indexers");
//       if (self->state.exprs.empty()) {
//         VAST_WARNING(self, "got indexers but has no queries");
//         return;
//       }
//       // FIXME: do not stupidly send every predicate to every indexer,
//       // rather, pick the minimal subset intelligently.
//       auto acc = self->spawn(accumulator,
//                              self->state.exprs.as_vector(), self);
//       auto t = self->spawn(task<>);
//       self->send(t, supervisor_atom::value, acc);
//       for (auto& indexer : indexers) {
//         self->send(t, indexer, uint64_t{self->state.preds.size()});
//         for (auto& p : self->state.preds)
//           self->send(indexer, expression{p}, acc, t);
//       }
//     }
//   };
// }
//
// } // namespace <anonymous>

behavior partition(stateful_actor<partition_state>* self, path dir,
                   actor sink) {
  VAST_ASSERT(sink);
  // Register the accountant, if available.
  auto acc = self->system().registry().get(accountant_atom::value);
  if (acc) {
    VAST_DEBUG(self, "registers accountant", acc);
    self->state.accountant = actor_cast<accountant_type>(acc);
  }
  // If the directory exists already, we must have some state and are loading
  // all INDEXERs.
  // TODO: it's cheaper to load them on demand.
  if (exists(dir)) {
    // Load PARTITION meta data.
    auto t = load(dir / "schema", self->state.schema);
    if (!t) {
      VAST_ERROR(self, self->system().render(t.error()));
      self->quit(t.error());
    } else {
      // Load INDXERs for each batch.
      VAST_ASSERT(!self->state.schema.empty());
      for (auto& batch_dir : directory{dir}) {
        auto interval = batch_dir.basename().str();
        if (batch_dir.is_directory()) {
          // Extract the base ID from the path. Directories have the form a-b
          // to represent the batch [a,b).
          auto dash = interval.find('-');
          if (dash < 1 || dash == std::string::npos) {
            VAST_WARNING(self, "ignores invalid batch directory:", interval);
            continue;
          }
          auto left = interval.substr(0, dash);
          auto base = to<event_id>(left);
          if (!base) {
            VAST_WARNING(self, "ignores directory with invalid base ID:", left);
            continue;
          }
          // Load the INDEXERs for each type in the batch.
          for (auto& type_dir : directory{batch_dir}) {
            VAST_DEBUG(self, "loads", interval / type_dir.basename());
            auto t = self->state.schema.find(type_dir.basename().str());
            VAST_ASSERT(t != nullptr);
            auto a = self->spawn<monitored>(event_indexer, type_dir, *t);
            self->state.indexers.emplace(*base, a);
          }
        }
      }
    }
  }
  // Write schema to disk.
  auto flush = [=]() -> expected<void> {
    if (self->state.schema.empty())
      return {};
    VAST_DEBUG(self, "flushes schema");
    if (!exists(dir)) {
      auto result = mkdir(dir);
      if (!result)
        return result.error();
    }
    return save(dir / "schema", self->state.schema);
  };
  // Handler executing after indexing a batch of events.
  auto on_done = [=](done_atom, steady_clock::time_point start,
                     uint64_t events) {
    auto stop = steady_clock::now();
    auto runtime = stop - start;
    auto unit = duration_cast<microseconds>(runtime).count();
    auto rate = events * 1e6 / unit;
    VAST_DEBUG(self, "indexed", events, "events in", runtime,
               '(' << size_t(rate), "events/sec)");
    if (self->state.accountant) {
      static auto to_sec = [](auto t) -> int64_t{
        auto tp = t.time_since_epoch();
        return duration_cast<microseconds>(tp).count();
      };
      self->send(self->state.accountant, "partition.indexing.start",
                 to_sec(start));
      self->send(self->state.accountant, "partition.indexing.stop",
                 to_sec(stop));
      self->send(self->state.accountant, "partition.indexing.events", events);
      self->send(self->state.accountant, "partition.indexing.rate", rate);
    }
    VAST_ASSERT(self->state.pending_events >= events);
    self->state.pending_events -= events;
  };
  // Basic DOWN handler, used again during shutdown.
  auto on_down = [=](down_msg const& msg) {
    if (msg.source == self->state.proxy) {
      self->state.proxy = {};
      return;
    }
    auto pred = [&](auto& p) { return p.second.address() == msg.source; };
    auto i = std::find_if(self->state.indexers.begin(),
                          self->state.indexers.end(), pred);
    if (i != self->state.indexers.end())
      self->state.indexers.erase(i);
  };
  self->set_down_handler(on_down);
  return {
    [=](shutdown_atom) {
      auto msg = self->current_mailbox_element()->move_content_to_message();
      if (self->state.proxy)
        self->send(self->state.proxy, msg);
      for (auto& q : self->state.queries)
        self->send(q.second.task, msg);
      if (self->state.indexers.empty()) {
        self->quit(exit_reason::user_shutdown);
      } else {
        VAST_DEBUG(self, "brings down all indexers");
        for (auto& i : self->state.indexers)
          self->send(i.second, msg);
        self->set_down_handler(
          [=](down_msg const& msg) {
            on_down(msg);
            if (self->state.pending_events == 0 && self->state.indexers.empty())
              self->quit(exit_reason::user_shutdown);
          }
        );
        self->become(
          [=](done_atom, steady_clock::time_point start, uint64_t events) {
            on_done(done_atom::value, start, events);
            if (self->state.pending_events == 0 && self->state.indexers.empty())
              self->quit(exit_reason::user_shutdown);
          }
        );
      }
      flush();
    },
    [=](std::vector<event> const& events, schema const& sch) {
      VAST_ASSERT(!events.empty());
      auto first_id = events.front().id();
      auto last_id = events.back().id();
      auto n = events.size();
      VAST_DEBUG(self, "got", n,
                 "events [" << first_id << ',' << (last_id + 1) << ')');
      auto t = self->spawn(task<steady_clock::time_point, uint64_t>,
                           steady_clock::now(), events.size());
      self->send(t, supervisor_atom::value, self);
      // Merge new schema into the existing one.
      auto result = schema::merge(self->state.schema, sch);
      VAST_ASSERT(result);
      self->state.schema = std::move(*result);
      // Create one indexer per type.
      auto base = first_id;
      auto interval = to_string(base) + "-" + to_string(base + n);
      std::vector<actor> indexers;
      indexers.reserve(sch.size());
      for (auto& t : sch) {
        auto p = dir / interval / t.name();
        auto i = self->spawn<monitored>(event_indexer, p, t);
        self->state.indexers.emplace(base, i);
        indexers.push_back(i);
      }
      // Forward events to all indexers.
      auto msg = self->current_mailbox_element()->move_content_to_message();
      msg = msg.take(1) + make_message(t);
      for (auto indexer : indexers) {
        self->send(t, indexer);
        self->send(indexer, msg);
      }
      // Relay indexers to continuous query proxy.
      if (self->state.proxy)
        self->send(self->state.proxy, std::move(indexers));
      // Update per-partition statistics.
      self->state.pending_events += n;
      VAST_DEBUG(self, "currently indexes", self->state.pending_events,
                 "events");
    },
    on_done,
    //[=](expression const& expr, continuous_atom) {
    //  VAST_DEBUG(self, "got continuous query:", expr);
    //  if (!self->state.proxy)
    //    self->state.proxy = self->spawn<monitored>(cq_proxy, sink);
    //  self->send(self->state.proxy, expr);
    //},
    //[=](expression const& expr, continuous_atom, disable_atom) {
    //  VAST_DEBUG(self, "got continuous query:", expr);
    //  if (!self->state.proxy)
    //    VAST_WARNING(self, "ignores disable request, no continuous queries");
    //  else
    //    self->send(self->state.proxy, expr, disable_atom::value);
    //},
    [=](expression const& expr, historical_atom) {
      VAST_DEBUG(self, "got historical query:", expr);
      auto q = self->state.queries.emplace(expr, partition_query_state{}).first;
      if (!q->second.task) {
        // Even if we still have evaluated this query in the past, we still
        // spin up a new task to ensure that we incorporate results from events
        // that have arrived in the meantime.
        VAST_DEBUG(self, "spawns new query task");
        q->second.task = self->spawn(task<steady_clock::time_point, expression>,
                                     steady_clock::now(), q->first);
        self->send(q->second.task, supervisor_atom::value, self);
        self->send(q->second.task, self);
        // Split up the query into predicates and handle each separately.
        bitmap cached_hits;
        for (auto& pred : visit(predicatizer{}, expr)) {
          VAST_DEBUG(self, "dispatches predicate", pred);
          auto p =
            self->state.predicates.emplace(pred, predicate_state()).first;
          VAST_ASSERT(p->first == pred);
          p->second.queries.insert(&q->first);
          auto i = self->state.indexers.begin();
          while (i != self->state.indexers.end()) {
            auto base = i->first;
            if (p->second.cache.contains(base)) {
              // If an indexer has already looked up this predicate in the
              // past, it must have sent the hits back to this partition, or is
              // in the process of doing so.
              VAST_DEBUG(self, "skips indexers for base", base);
              while (i->first == base && i != self->state.indexers.end())
                ++i;
              // If hits for this predicate exist already, we must send them
              // back to INDEX. Otherwise INDEX will produce false negatives.
              if (!p->second.hits.empty() && !all<0>(p->second.hits))
                cached_hits |= p->second.hits;
            } else {
              // Forward the predicate to the subset of indexers which we
              // haven't asked yet.
              VAST_DEBUG(self, "relays predicate for base", base);
              while (i->first == base && i != self->state.indexers.end()) {
                VAST_DEBUG(self, " - forwards predicate to indexer", i->second);
                p->second.cache.insert(i->first);
                if (!p->second.task) {
                  p->second.task =
                    self->spawn(task<steady_clock::time_point, predicate>,
                                steady_clock::now(), pred);
                  self->send(p->second.task, supervisor_atom::value, self);
                }
                self->send(q->second.task, p->second.task);
                self->send(p->second.task, i->second);
                self->send(i->second, pred, self, p->second.task);
                ++i;
              }
            }
          }
        }
        if (!cached_hits.empty() && !all<0>(cached_hits))
          self->send(sink, expr, std::move(cached_hits),
                     historical_atom::value);
        self->send(q->second.task, done_atom::value);
      }
      if (!q->second.hits.empty() && !all<0>(q->second.hits))
        self->send(sink, expr, q->second.hits, historical_atom::value);
    },
    [=](predicate const& pred, bitmap const& hits) {
      VAST_DEBUG(self, "got", rank(hits), "hits for predicate:", pred);
      self->state.predicates[pred].hits |= hits;
    },
    [=](done_atom, steady_clock::time_point start, predicate const& pred) {
      auto evaluator = make_bitmap_evaluator<bitmap>(
        [&](predicate const& p) -> bitmap const* {
          auto i = self->state.predicates.find(p);
          return i == self->state.predicates.end() ? nullptr : &i->second.hits;
        }
      );
      // Once we've completed all tasks of a certain predicate for all events,
      // we evaluate all queries in which the predicate participates.
      auto& ps = self->state.predicates[pred];
      VAST_DEBUG(self, "took", steady_clock::now() - start,
                 "to answer predicate for", ps.cache.size(), "indexers:", pred);
      for (auto& q : ps.queries) {
        VAST_ASSERT(q);
        VAST_DEBUG(self, "evaluates", *q);
        auto& qs = self->state.queries[*q];
        auto hits = visit(evaluator, *q);
        if (!hits.empty() && !all<0>(hits) && hits != qs.hits) {
          VAST_DEBUG(self, "relays", rank(hits), "hits");
          qs.hits = hits;
          self->send(sink, *q, std::move(hits), historical_atom::value);
        }
      }
      ps.task = {};
    },
    [=](done_atom, steady_clock::time_point start, expression const& expr) {
      VAST_DEBUG(self, "completed query", expr, "in",
                 steady_clock::now() - start);
      self->state.queries[expr].task = {};
      auto msg = self->current_mailbox_element()->move_content_to_message();
      self->send(sink, msg);
    },
    [=](flush_atom, actor const& task) {
      VAST_DEBUG(self, "peforms flush");
      for (auto& i : self->state.indexers)
        if (i.second) {
          self->send(task, i.second);
          self->send(i.second, flush_atom::value, task);
        }
      auto result = flush();
      if (!result) {
        VAST_ERROR(self, self->system().render(result.error()));
        self->quit(result.error());
      }
      self->send(task, done_atom::value);
    }
  };
}

} // namespace system
} // namespace vast
