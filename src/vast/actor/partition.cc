#include <caf/all.hpp>

#include "vast/event.h"
#include "vast/actor/atoms.h"
#include "vast/actor/indexer.h"
#include "vast/actor/partition.h"
#include "vast/actor/task.h"
#include "vast/expr/predicatizer.h"
#include "vast/concept/parseable/numeric/integral.h"
#include "vast/concept/parseable/to.h"
#include "vast/concept/printable/vast/error.h"
#include "vast/concept/printable/vast/expression.h"
#include "vast/concept/printable/vast/event.h"
#include "vast/concept/printable/vast/time.h"
#include "vast/concept/serializable/io.h"
#include "vast/concept/serializable/vast/schema.h"
#include "vast/util/assert.h"

namespace vast {

namespace {

template <typename Bitstream>
using hits_map = std::map<predicate, Bitstream>;

// Receives hits from INDEXERs and evaluates a set of expression over them.
template <typename Bitstream>
struct accumulator {
  struct state : basic_state {
    state(local_actor *self) : basic_state{self, "accumulator"} { }

    Bitstream evaluate(expression const& expr) const {
      struct evaluator : expr::bitstream_evaluator<evaluator, Bitstream> {
        evaluator(state const* s) : this_{s} { }
        Bitstream const* lookup(predicate const& pred) const {
          auto i = this_->hits.find(pred);
          return i == this_->hits.end() ? nullptr : &i->second;
        }
        state const* this_;
      };
      return visit(evaluator{this}, expr);
    }

    hits_map<Bitstream> hits;
  };

  static behavior make(stateful_actor<state>* self,
                       std::vector<expression> exprs, actor const& sink) {
    return {
      [=](expression& pred, Bitstream& hits) {
        auto p = get<predicate>(pred);
        VAST_ASSERT(p);
        self->state.hits.emplace(std::move(*p), std::move(hits));
      },
      [self, exprs=std::move(exprs), sink](done_atom) {
        for (auto& expr : exprs) {
          VAST_DEBUG_AT(self, "evalutes continuous query:", expr);
          self->send(sink, expr, self->state.evaluate(expr));
        }
        self->quit(exit::done);
        // TODO: relay the hits back to PARTITION if the query is also
        // historical. Caveat: we should not re-evaluate the historical query
        // with these hits to avoid that the sink receives duplicate hits.
      }
    };
  }
};

// Accumulates all hits from an event batch, evalutes a query, and sends the
// result of the evaluation back to PARTITION.
template <typename Bitstream>
struct cq_proxy {
  struct state : basic_state {
    state(local_actor* self) : basic_state{self, "cq-proxy"} { }

    util::flat_set<expression> exprs;
    util::flat_set<predicate> preds;
  };

  // Accumulates hits from indexers for a single event batch.
  static behavior make(stateful_actor<state>* self, actor const& sink) {
    return {
      [=](expression const& expr) {
        self->state.exprs.insert(expr);
        for (auto& p : visit(expr::predicatizer{}, expr))
          self->state.preds.insert(std::move(p));
      },
      [=](expression const& expr, disable_atom) {
        self->state.exprs.erase(expr);
        self->state.preds.clear();
        if (self->state.exprs.empty())
          self->quit(exit::done);
        else
          for (auto& ex : self->state.exprs)
            for (auto& p : visit(expr::predicatizer{}, ex))
              self->state.preds.insert(std::move(p));
      },
      [=](expression const&, Bitstream const& hits) {
        VAST_DEBUG_AT(self, "relays", hits.count(), "hits");
        self->send(sink, self->current_message()
                           + make_message(continuous_atom::value));
      },
      [=](std::vector<actor> const& indexers) {
        VAST_DEBUG_AT(self, "got", indexers.size(), "indexers");
        if (self->state.exprs.empty()) {
          VAST_WARN_AT(self, "got indexers without having queries");
          return;
        }
        // FIXME: do not stupidly send every predicate to every indexer,
        // rather, pick the minimal subset intelligently.
        auto acc = self->spawn(accumulator<Bitstream>::make,
                               self->state.exprs.as_vector(), self);
        auto t = self->spawn(task::make<>);
        self->send(t, supervisor_atom::value, acc);
        for (auto& indexer : indexers) {
          self->send(t, indexer, uint64_t{self->state.preds.size()});
          for (auto& p : self->state.preds)
            self->send(indexer, expression{p}, acc, t);
        }
      }
    };
  }
};

} // namespace <anonymous>

partition::state::state(local_actor* self) : basic_state{self, "partition"} { }

behavior partition::make(stateful_actor<state>* self, path dir, actor sink) {
  VAST_ASSERT(sink != invalid_actor);
  // If the directory exists already, we must have some state and are loading
  // all INDEXERs.
  // FIXME: it might be cheaper to load them on demand.
  if (exists(dir)) {
    // Load PARTITION meta data.
    auto t = load(dir / "schema", self->state.schema);
    if (!t) {
      VAST_ERROR_AT(self, "failed to load schema:", t.error());
      self->quit(exit::error);
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
            VAST_WARN_AT(self, "ignores directory with invalid format:",
                         interval);
            continue;
          }
          auto left = interval.substr(0, dash);
          auto base = to<event_id>(left);
          if (!base) {
            VAST_WARN_AT("ignores directory with invalid event ID:", left);
            continue;
          }
          // Load the INDEXERs for each type in the batch.
          for (auto& type_dir : directory{batch_dir}) {
            VAST_DEBUG_AT(self, "loads", interval / type_dir.basename());
            auto t = self->state.schema.find(type_dir.basename().str());
            VAST_ASSERT(t != nullptr);
            auto a = self->spawn<monitored>(
              event_indexer<bitstream_type>::make, type_dir, *t);
            self->state.indexers.emplace(*base, a);
          }
        }
      }
    }
  }
  // Write schema to disk.
  auto flush = [=] {
    if (self->state.schema.empty())
      return;
    VAST_DEBUG_AT(self, "flushes schema");
    auto t = save(dir / "schema", self->state.schema);
    if (!t) {
      VAST_ERROR_AT(self, "failed to flush:", t.error());
      self->quit(exit::error);
    }
  };
  // Basic DOWN handler.
  auto on_down = [=](down_msg const& msg) {
    if (msg.source == self->state.proxy) {
      self->state.proxy = invalid_actor;
      return;
    }
    auto pred = [&](auto& p) { return p.second.address() == msg.source; };
    auto i = std::find_if(self->state.indexers.begin(),
                          self->state.indexers.end(), pred);
    if (i != self->state.indexers.end())
      self->state.indexers.erase(i);
  };
  // Handler executing after indexing a batch of events.
  auto on_done = [=](done_atom, time::moment start, uint64_t events) {
    auto stop = time::snapshot();
    auto runtime = stop - start;
    auto unit = time::duration_cast<time::microseconds>(runtime).count();
    auto rate = events * 1e6 / unit;
    VAST_DEBUG_AT(self, "indexed", events, "events in", runtime,
               '(' << size_t(rate), "events/sec)");
    if (self->state.accountant) {
      static auto to_sec = [](auto t) -> int64_t{
        auto tp = t.time_since_epoch();
        return time::duration_cast<time::microseconds>(tp).count();
      };
      self->send(self->state.accountant, "partition", "indexing.start",
                 to_sec(start));
      self->send(self->state.accountant, "partition", "indexing.stop",
                 to_sec(stop));
      self->send(self->state.accountant, "partition", "indexing.events",
                 events);
      self->send(self->state.accountant, "partition", "indexing.rate", rate);
    }
    VAST_ASSERT(self->state.pending_events >= events);
    self->state.pending_events -= events;
  };
  self->trap_exit(true);
  return {
    [=](exit_msg const& msg) {
      // FIXME: don't abuse EXIT messages as shutdown mechanism.
      if (msg.reason == exit::kill) {
        if (self->state.proxy)
          self->send_exit(self->state.proxy, exit::kill);
        for (auto& i : self->state.indexers)
          self->link_to(i.second);
        for (auto& q : self->state.queries)
          self->link_to(q.second.task);
        self->quit(msg.reason);
        return;
      }
      if (self->current_mailbox_element()->mid.is_high_priority()) {
        VAST_DEBUG_AT(self, "delays EXIT from", msg.source);
        self->send(message_priority::normal, self, self->current_message());
        return;
      }
      // A partition doesn't have persistent query state, nor does the
      // continuous query proxy, so we can always terminate the them directly.
      if (self->state.proxy)
        self->send_exit(self->state.proxy, msg.reason);
      for (auto& q : self->state.queries)
        self->send_exit(q.second.task, msg.reason);
      // Terminate after all INDEXERs have exited safely.
      if (self->state.indexers.empty()) {
        self->quit(msg.reason);
      } else {
        VAST_DEBUG_AT(self, "brings down all indexers");
        for (auto& i : self->state.indexers)
          self->send_exit(i.second, msg.reason);
        // Terminate not before all INDEXERS have exited and we've recorded
        // the fact that they have finished.
        self->become(
          [reason=msg.reason, on_down, self](down_msg const& down) {
            on_down(down);
            if (self->state.pending_events == 0 && self->state.indexers.empty())
              self->quit(reason);
          },
          [reason=msg.reason, on_done, self](done_atom, time::moment start,
                                             uint64_t events) {
            on_done(done_atom::value, start, events);
            if (self->state.pending_events == 0 && self->state.indexers.empty())
              self->quit(reason);
          }
        );
      }
      flush();
    },
    on_down,
    on_done,
    [=](accountant::type const& accountant) {
      VAST_DEBUG_AT(self, "registers accountant#" << accountant->id());
      self->state.accountant = accountant;
    },
    [=](std::vector<event> const& events, schema const& sch,
        actor const& task) {
      VAST_ASSERT(!events.empty());
      VAST_DEBUG_AT(self, "got", events.size(),
                   "events [" << events.front().id() << ','
                              << (events.back().id() + 1) << ')');
      self->send(task, supervisor_atom::value, self);
      // Merge new schema into the existing one.
      auto success = self->state.schema.add(sch);
      VAST_ASSERT(success);
      // Create one INDEXER per type and relay events to them.
      auto base = events.front().id();
      auto interval = to_string(base) + "-" + to_string(base + events.size());
      std::vector<actor> indexers;
      indexers.reserve(sch.size());
      auto msg = self->current_message();
      msg = msg.take(1) + msg.take_right(1);
      for (auto& t : sch) {
        auto indexer_dir = dir / interval / t.name();
        auto indexer = self->spawn<monitored>(
          event_indexer<bitstream_type>::make, indexer_dir, t);
        self->send(task, indexer);
        self->send(indexer, msg);
        indexers.push_back(indexer);
        self->state.indexers.emplace(base, std::move(indexer));
      }
      // Relay INDEXERs to continuous query proxy.
      if (self->state.proxy != invalid_actor)
        self->send(self->state.proxy, std::move(indexers));
      // Update per-partition statistics.
      self->state.pending_events += events.size();
      VAST_DEBUG_AT(self, "indexes", self->state.pending_events,
                 "events in parallel");
    },
    [=](expression const& expr, continuous_atom) {
      VAST_DEBUG_AT(self, "got continuous query:", expr);
      if (!self->state.proxy)
        self->state.proxy
          = self->spawn<monitored>(cq_proxy<default_bitstream>::make, sink);
      self->send(self->state.proxy, expr);
    },
    [=](expression const& expr, continuous_atom, disable_atom) {
      VAST_DEBUG_AT(self, "got continuous query:", expr);
      if (!self->state.proxy)
        VAST_WARN_AT(self, "ignores disable request, no continuous queries");
      else
        self->send(self->state.proxy, expr, disable_atom::value);
    },
    [=](expression const& expr, historical_atom) {
      VAST_DEBUG_AT(self, "got historical query:", expr);
      auto q = self->state.queries.emplace(expr, query_state()).first;
      if (!q->second.task) {
        // Even if we still have evaluated this query in the past, we still
        // spin up a new task to ensure that we incorporate results from events
        // that have arrived in the meantime.
        VAST_DEBUG_AT(self, "spawns new query task");
        q->second.task = self->spawn(task::make<time::moment, expression>,
                                     time::snapshot(), q->first);
        self->send(q->second.task, supervisor_atom::value, self);
        self->send(q->second.task, self);
        // Split up the query into predicates and handle each separately.
        bitstream_type cached_hits;
        for (auto& pred : visit(expr::predicatizer{}, expr)) {
          VAST_DEBUG_AT(self, "dispatches predicate", pred);
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
              VAST_DEBUG_AT(self, "skips indexers for base", base);
              while (i->first == base && i != self->state.indexers.end())
                ++i;
              // If hits for this predicate exist already, we must send them
              // back to INDEX. Otherwise INDEX will produce false negatives.
              if (!p->second.hits.empty() && !p->second.hits.all_zeros())
                cached_hits |= p->second.hits;
            } else {
              // Forward the predicate to the subset of indexers which we
              // haven't asked yet.
              VAST_DEBUG_AT(self, "relays predicate for base", base);
              while (i->first == base && i != self->state.indexers.end()) {
                VAST_DEBUG_AT(self, " - forwards predicate to", i->second);
                p->second.cache.insert(i->first);
                if (!p->second.task) {
                  p->second.task =
                    self->spawn(task::make<time::moment, predicate>,
                                time::snapshot(), pred);
                  self->send(p->second.task, supervisor_atom::value, self);
                }
                self->send(q->second.task, p->second.task);
                self->send(p->second.task, i->second);
                self->send(i->second, expression{pred}, self, p->second.task);
                ++i;
              }
            }
          }
        }
        if (!cached_hits.empty() && !cached_hits.all_zeros())
          self->send(sink, expr, std::move(cached_hits),
                     historical_atom::value);
        self->send(q->second.task, done_atom::value);
      }
      if (!q->second.hits.empty() && !q->second.hits.all_zeros())
        self->send(sink, expr, q->second.hits, historical_atom::value);
    },
    [=](expression const& pred, bitstream_type const& hits) {
      VAST_DEBUG_AT(self, "got", hits.count(), "hits for predicate:", pred);
      self->state.predicates[*get<predicate>(pred)].hits |= hits;
    },
    [=](done_atom, time::moment start, predicate const& pred) {
      struct evaluator : expr::bitstream_evaluator<evaluator,
                                                   default_bitstream> {
        evaluator(state const& s) : state_{s} { }
        bitstream_type const* lookup(predicate const& pred) const {
          auto p = state_.predicates.find(pred);
          return p == state_.predicates.end() ? nullptr : &p->second.hits;
        }
        state const& state_;
      };
      // Once we've completed all tasks of a certain predicate for all events,
      // we evaluate all queries in which the predicate participates.
      auto& ps = self->state.predicates[pred];
      VAST_DEBUG_AT(self, "took", time::snapshot() - start,
                    "to complete predicate for", ps.cache.size(), "indexers:",
                    pred);
      for (auto& q : ps.queries) {
        VAST_ASSERT(q != nullptr);
        VAST_DEBUG_AT(self, "evaluates", *q);
        auto& qs = self->state.queries[*q];
        auto hits = visit(evaluator{self->state}, *q);
        if (!hits.empty() && !hits.all_zeros() && hits != qs.hits) {
          VAST_DEBUG_AT(self, "relays", hits.count(), "hits");
          qs.hits = hits;
          self->send(sink, *q, std::move(hits), historical_atom::value);
        }
      }
      ps.task = invalid_actor;
    },
    [=](done_atom, time::moment start, expression const& expr) {
      VAST_DEBUG_AT(self, "completed query", expr, "in",
                    time::snapshot() - start);
      self->state.queries[expr].task = invalid_actor;
      self->send(sink, self->current_message());
    },
    [=](flush_atom, actor const& task) {
      VAST_DEBUG_AT(self, "peforms flush");
      self->send(task, self);
      for (auto& i : self->state.indexers)
        if (i.second) {
          self->send(task, i.second);
          self->send(i.second, flush_atom::value, task);
        }
      flush();
      self->send(task, done_atom::value);
    },
    log_others(self)
  };
}

} // namespace vast
