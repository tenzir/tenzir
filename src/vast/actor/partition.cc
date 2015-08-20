#include <caf/all.hpp>

#include "vast/event.h"
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

// Accumulates all hits from an event batch, evalutes a query, and sends the
// result of the evaluation to PARTITION.
template <typename Bitstream>
struct continuous_query_proxy : default_actor {
  using predicate_map = std::map<predicate, Bitstream>;

  // Evalutes an expression according to a given set of predicates.
  struct evaluator : expr::bitstream_evaluator<evaluator, Bitstream> {
    evaluator(predicate_map const& pm) : map_{pm} {
    }

    Bitstream const* lookup(predicate const& pred) const {
      auto i = map_.find(pred);
      return i == map_.end() ? nullptr : &i->second;
    }

    predicate_map const& map_;
  };

  // Accumulates hits from indexers for a single event batch.
  struct accumulator : default_actor {
    accumulator(std::vector<expression> exprs, actor sink)
      : default_actor{"accumulator"},
        exprs_{std::move(exprs)},
        sink_{std::move(sink)} {
    }

    void on_exit() override {
      sink_ = invalid_actor;
    }

    behavior make_behavior() override {
      return {
        [=](expression& pred, Bitstream& hits) {
          auto p = get<predicate>(pred);
          VAST_ASSERT(p);
          map_.emplace(std::move(*p), std::move(hits));
        },
        [=](done_atom) {
          for (auto& expr : exprs_) {
            VAST_DEBUG(this, "evalutes continuous query:", expr);
            send(sink_, expr, visit(evaluator{map_}, expr));
          }
          quit(exit::done);
          // TODO: relay predicate_map back to PARTITION if the query is also
          // historical. Caveat: we should not re-evaluate the historical query
          // with these hits to avoid that the sink receives duplicate hits.
        }
      };
    }

    predicate_map map_;
    std::vector<expression> exprs_;
    actor sink_;
  };

  continuous_query_proxy(actor sink)
    : default_actor{"continuous-query-proxy"}, sink_{std::move(sink)} {
  }

  void on_exit() override {
    sink_ = invalid_actor;
  }

  behavior make_behavior() override {
    return {
      [=](expression const& expr) {
        exprs_.insert(expr);
        for (auto& p : visit(expr::predicatizer{}, expr))
          preds_.insert(std::move(p));
      },
      [=](expression const& expr, disable_atom) {
        exprs_.erase(expr);
        preds_.clear();
        if (exprs_.empty())
          quit(exit::done);
        else
          for (auto& ex : exprs_)
            for (auto& p : visit(expr::predicatizer{}, ex))
              preds_.insert(std::move(p));
      },
      [=](expression& expr, Bitstream& hits) {
        VAST_DEBUG(this, "relays", hits.count(), "hits");
        send(sink_, std::move(expr), std::move(hits),
             continuous_atom::value);
      },
      [=](std::vector<actor> const& indexers) {
        VAST_DEBUG(this, "got", indexers.size(), "indexers");
        if (exprs_.empty()) {
          VAST_WARN(this, "got indexers without having queries");
          return;
        }
        // FIXME: do not stupidly send every predicate to every indexer,
        // rather, pick the minimal subset intelligently.
        auto acc = spawn<accumulator>(exprs_.as_vector(), this);
        auto t = spawn<task>();
        send(t, supervisor_atom::value, acc);
        for (auto& i : indexers) {
          send(t, i, uint64_t{preds_.size()});
          for (auto& p : preds_)
            send(i, expression{p}, acc, t);
        }
      }
    };
  }

  actor sink_;
  util::flat_set<expression> exprs_;
  util::flat_set<predicate> preds_;
};

} // namespace <anonymous>

partition::evaluator::evaluator(partition const& p) : partition_{p} {
}

partition::bitstream_type const*
partition::evaluator::lookup(predicate const& pred) const {
  auto p = partition_.predicates_.find(pred);
  return p == partition_.predicates_.end() ? nullptr : &p->second.hits;
}

partition::partition(path dir, actor sink)
  : flow_controlled_actor{"partition"},
    dir_{std::move(dir)},
    sink_{std::move(sink)} {
  VAST_ASSERT(sink_ != invalid_actor);
  trap_exit(true);
}

void partition::on_exit() {
  sink_ = invalid_actor;
  proxy_ = invalid_actor;
  indexers_.clear();
  predicates_.clear();
  queries_.clear();
}

behavior partition::make_behavior() {
  if (exists(dir_)) {
    using vast::load;
    auto t = load(dir_ / "schema", schema_);
    if (!t) {
      VAST_ERROR(this, "failed to load schema:", t.error());
      quit(exit::error);
      return {};
    }
    VAST_ASSERT(!schema_.empty());
    for (auto& p : directory{dir_}) {
      auto interval = p.basename().str();
      if (p.is_directory()) {
        // Extract the base ID from the path. Directories have the form a-b.
        auto dash = interval.find('-');
        if (dash < 1 || dash == std::string::npos) {
          VAST_WARN(this, "ignores directory with invalid format:", interval);
          continue;
        }
        auto left = interval.substr(0, dash);
        auto base = to<event_id>(left);
        if (!base) {
          VAST_WARN("ignores directory with invalid event ID:", left);
          continue;
        }
        // Load the indexer for each event.
        for (auto& name : directory{p}) {
          VAST_DEBUG(this, "loads", interval / name.basename());
          auto t = schema_.find_type(name.basename().str());
          VAST_ASSERT(t != nullptr);
          auto a = spawn<event_indexer<bitstream_type>, monitored>(name, *t);
          indexers_.emplace(*base, a);
        }
      }
    }
  }
  // Basic DOWN handler.
  auto on_down = [=](down_msg const& msg) {
    if (msg.source == proxy_->address()) {
      proxy_ = invalid_actor;
      return;
    }
    if (remove_upstream_node(msg.source))
      return;
    auto i = std::find_if(indexers_.begin(), indexers_.end(), [&](auto& p) {
      return p.second.address() == msg.source;
    });
    if (i != indexers_.end())
      indexers_.erase(i);
  };
  return {
    forward_overload(),
    forward_underload(),
    register_upstream_node(),
    [=](exit_msg const& msg) {
      if (msg.reason == exit::kill) {
        if (proxy_)
          send_exit(proxy_, exit::kill);
        for (auto& i : indexers_)
          link_to(i.second);
        for (auto& q : queries_)
          link_to(q.second.task);
        quit(msg.reason);
        return;
      }
      if (downgrade_exit())
        return;
      // A partition doesn't have persistent query state, nor does the
      // continuous query proxy, so we can always terminate the them directly.
      for (auto& q : queries_)
        send_exit(q.second.task, msg.reason);
      if (proxy_)
        send_exit(proxy_, msg.reason);
      if (indexers_.empty()) {
        quit(msg.reason);
      } else {
        VAST_DEBUG(this, "brings down all indexers");
        for (auto& i : indexers_)
          send_exit(i.second, msg.reason);
        become([ reason = msg.reason, on_down, this ](down_msg const& down) {
          // Terminate as soon as all indexers have exited.
          on_down(down);
          if (indexers_.empty())
            quit(reason);
        });
      }
      flush();
    },
    on_down,
    [=](std::vector<event> const& events, actor const& task) {
      VAST_DEBUG(this, "got", events.size(),
                 "events [" << events.front().id() << ','
                            << (events.back().id() + 1) << ')');
      // Extract all unique types.
      util::flat_set<type> types;
      for (auto& e : events)
        types.insert(e.type());
      // Create one event indexer per type.
      auto base = events.front().id();
      auto interval = to_string(base) + "-" + to_string(base + events.size());
      std::vector<actor> indexers;
      for (auto& t : types)
        if (!t.find_attribute(type::attribute::skip)) {
          if (!schema_.add(t)) {
            VAST_ERROR(this, "failed to incorporate types from new schema");
            quit(exit::error);
            return;
          }
          auto indexer = spawn<event_indexer<bitstream_type>, monitored>(
            dir_ / interval / t.name(), t);
          indexers.push_back(indexer);
          indexers_.emplace(base, std::move(indexer));
        }
      if (indexers.empty()) {
        VAST_WARN(this, "didn't find any types to index");
        send_exit(task, exit::done);
        return;
      }
      for (auto& i : indexers) {
        send(task, i);
        send(i, current_message());
      }
      if (proxy_ != invalid_actor)
        send(proxy_, std::move(indexers));
      events_indexed_concurrently_ += events.size();
      if (++events_indexed_concurrently_ > 1 << 20) // TODO: calibrate
        overloaded(true);
      send(task, supervisor_atom::value, this);
      VAST_DEBUG(this, "indexes", events_indexed_concurrently_,
                 "events in parallel");
    },
    [=](done_atom, time::moment start, uint64_t events) {
      VAST_DEBUG(this, "indexed", events, "events in",
                 time::snapshot() - start);
      VAST_ASSERT(events_indexed_concurrently_ > events);
      events_indexed_concurrently_ -= events;
      if (events_indexed_concurrently_ < 1 << 20)
        overloaded(false);
    },
    [=](expression const& expr, continuous_atom) {
      VAST_DEBUG(this, "got continuous query:", expr);
      if (!proxy_)
        proxy_
          = spawn<continuous_query_proxy<default_bitstream>, monitored>(sink_);
      send(proxy_, expr);
    },
    [=](expression const& expr, continuous_atom, disable_atom) {
      VAST_DEBUG(this, "got continuous query:", expr);
      if (!proxy_)
        VAST_WARN(this, "ignores disable request, no continuous queries");
      else
        send(proxy_, expr, disable_atom::value);
    },
    [=](expression const& expr, historical_atom) {
      VAST_DEBUG(this, "got historical query:", expr);
      auto q = queries_.emplace(expr, query_state()).first;
      if (!q->second.task) {
        // Even if we still have evaluated this query in the past, we still
        // spin up a new task to ensure that we incorporate results from events
        // that have arrived in the meantime.
        VAST_DEBUG(this, "spawns new query task");
        q->second.task = spawn<task>(time::snapshot(), q->first);
        send(q->second.task, supervisor_atom::value, this);
        send(q->second.task, this);
        for (auto& pred : visit(expr::predicatizer{}, expr)) {
          VAST_DEBUG(this, "dispatches predicate", pred);
          auto p = predicates_.emplace(pred, predicate_state()).first;
          p->second.queries.insert(&q->first);
          auto i = indexers_.begin();
          while (i != indexers_.end()) {
            // We forward the predicate to the subset of indexers which we
            // haven't asked yet. If an indexer has already lookup up predicate
            // in the past, it must have sent the hits back to this partition,
            // or is in the process of doing so.
            auto base = i->first;
            if (p->second.cache.contains(i->first)) {
              VAST_DEBUG(this, "skips indexers for base", base);
              while (base == i->first && i != indexers_.end())
                ++i;
            } else {
              VAST_DEBUG(this, "relays predicate to indexers for base", base);
              while (base == i->first && i != indexers_.end()) {
                VAST_DEBUG(this, " - forwards predicate to", i->second);
                p->second.cache.insert(i->first);
                if (!p->second.task) {
                  p->second.task = spawn<task>(time::snapshot(), pred);
                  send(p->second.task, supervisor_atom::value, this);
                }
                send(q->second.task, p->second.task);
                send(p->second.task, i->second);
                send(i->second, expression{pred}, this, p->second.task);
                ++i;
              }
            }
          }
        }
        send(q->second.task, done_atom::value);
      }
      if (!q->second.hits.empty() && !q->second.hits.all_zeros())
        send(sink_, expr, q->second.hits, historical_atom::value);
    },
    [=](expression const& pred, bitstream_type const& hits) {
      VAST_DEBUG(this, "got", hits.count(), "hits for predicate:", pred);
      predicates_[*get<predicate>(pred)].hits |= hits;
    },
    [=](done_atom, time::moment start, predicate const& pred) {
      // Once we've completed all tasks of a certain predicate for all events,
      // we evaluate all queries in which the predicate participates.
      auto& ps = predicates_[pred];
      VAST_DEBUG(this, "took", time::snapshot() - start,
                 "to complete predicate for", ps.cache.size(), "indexers:",
                 pred);
      for (auto& q : ps.queries) {
        VAST_ASSERT(q != nullptr);
        VAST_DEBUG(this, "evaluates", *q);
        auto& qs = queries_[*q];
        auto hits = visit(evaluator{*this}, *q);
        if (!hits.empty() && !hits.all_zeros() && hits != qs.hits) {
          VAST_DEBUG(this, "relays", hits.count(), "hits");
          qs.hits = hits;
          send(sink_, *q, std::move(hits), historical_atom::value);
        }
      }
      ps.task = invalid_actor;
    },
    [=](done_atom, time::moment start, expression const& expr) {
      VAST_DEBUG(this, "completed query", expr, "in", time::snapshot() - start);
      queries_[expr].task = invalid_actor;
      send(sink_, current_message());
    },
    [=](flush_atom, actor const& task) {
      VAST_DEBUG(this, "peforms flush");
      send(task, this);
      for (auto& i : indexers_)
        if (i.second) {
          send(task, i.second);
          send(i.second, flush_atom::value, task);
        }
      flush();
      send(task, done_atom::value);
    },
    catch_unexpected()
  };
}

void partition::flush() {
  if (schema_.empty())
    return;
  VAST_DEBUG(this, "flushes schema");
  using vast::save;
  auto t = save(dir_ / "schema", schema_);
  if (!t) {
    VAST_ERROR(this, "failed to flush:", t.error());
    quit(exit::error);
  }
}

} // namespace vast
