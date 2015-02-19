#include "vast/actor/partition.h"

#include <caf/all.hpp>
#include "vast/event.h"
#include "vast/actor/indexer.h"
#include "vast/actor/task.h"
#include "vast/expr/predicatizer.h"
#include "vast/io/serialization.h"

using namespace caf;

namespace vast {

namespace {

// Accumulates all hits from a chunk, evalutes a query, and sends the result of
// the evaluation to PARTITION.
template <typename Bitstream>
struct continuous_query_proxy : default_actor
{
  using predicate_map = std::map<predicate, Bitstream>;

  // Evalutes an expression according to a given set of predicates.
  struct evaluator : expr::bitstream_evaluator<evaluator, Bitstream>
  {
    evaluator(predicate_map const& pm)
      : map_{pm}
    {
    }

    Bitstream const* lookup(predicate const& pred) const
    {
      auto i = map_.find(pred);
      return i == map_.end() ? nullptr : &i->second;
    }

    predicate_map const& map_;
  };

  // Accumulates hits from indexers for a single chunk.
  struct accumulator : default_actor
  {
    accumulator(std::vector<expression> exprs, actor sink)
      : exprs_{std::move(exprs)},
        sink_{std::move(sink)}
    {
      attach_functor([=](uint32_t)
        {
          sink_ = invalid_actor;
        });
    }

    message_handler make_handler() override
    {
      return
      {
        [=](expression& pred, Bitstream& hits)
        {
          auto p = get<predicate>(pred);
          assert(p);
          map_.emplace(std::move(*p), std::move(hits));
        },
        [=](done_atom)
        {
          for (auto& expr : exprs_)
          {
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

    std::string name() const override
    {
      return "accumulator";
    }

    predicate_map map_;
    std::vector<expression> exprs_;
    actor sink_;
  };

  continuous_query_proxy(actor sink)
    : sink_{std::move(sink)}
  {
    attach_functor([=](uint32_t)
      {
        sink_ = invalid_actor;
      });
  }

  message_handler make_handler() override
  {
    return
    {
      [=](expression const& expr)
      {
        exprs_.insert(expr);
        for (auto& p : visit(expr::predicatizer{}, expr))
          preds_.insert(std::move(p));
      },
      [=](expression const& expr, disable_atom)
      {
        exprs_.erase(expr);
        preds_.clear();
        for (auto& ex : exprs_)
          for (auto& p : visit(expr::predicatizer{}, ex))
            preds_.insert(std::move(p));
      },
      [=](expression& expr, Bitstream& hits)
      {
        VAST_DEBUG(this, "relays", hits.count(), "hits");
        send(sink_, std::move(expr), std::move(hits), continuous_atom::value);
      },
      [=](std::vector<actor> const& indexers)
      {
        // FIXME: do not stupidly send every predicate to every indexer,
        // rather, pick the minimal subset intelligently.
        assert(! exprs_.empty());
        auto acc = spawn<accumulator>(exprs_.as_vector(), this);
        auto t = spawn<task>();
        send(t, supervisor_atom::value, acc);
        for (auto& i : indexers)
        {
          send(t, i, uint64_t{preds_.size()});
          for (auto& p : preds_)
            send(i, expression{p}, acc, t);
        }
      }
    };
  }

  std::string name() const override
  {
    return "proxy";
  }

  actor sink_;
  util::flat_set<expression> exprs_;
  util::flat_set<predicate> preds_;
};

} // namespace <anonymous>

partition::evaluator::evaluator(partition const& p)
  : partition_{p}
{
}

partition::bitstream_type const*
partition::evaluator::lookup(predicate const& pred) const
{
  auto p = partition_.predicates_.find(pred);
  return p == partition_.predicates_.end() ? nullptr : &p->second.hits;
}


partition::partition(path dir, actor sink)
  : dir_{std::move(dir)},
    sink_{std::move(sink)}
{
  assert(sink_ != invalid_actor);
  attach_functor([=](uint32_t)
    {
      sink_ = invalid_actor;
      proxy_ = invalid_actor;
      indexers_.clear();
      predicates_.clear();
      queries_.clear();
    });
  // TODO: calibrate.
  overload_when([=] { return chunks_indexed_concurrently_ > 10; });
}

void partition::at(down_msg const& msg)
{
  if (dechunkifiers_.erase(msg.source) == 1)
    return;
  auto i = std::find_if(
      indexers_.begin(),
      indexers_.end(),
      [&](auto& p) { return p.second.address() == msg.source; });
  if (i != indexers_.end())
    indexers_.erase(i);
}

void partition::at(exit_msg const& msg)
{
  if (msg.reason == exit::kill)
  {
    for (auto& i : indexers_)
      link_to(i.second);
    for (auto& q : queries_)
      link_to(q.second.task);
    quit(msg.reason);
    return;
  }
  // We terminate queries unconditionally.
  for (auto& q : queries_)
    send_exit(q.second.task, msg.reason);
  // Two-staged shutdown of dechunkifiers and indexers: first, wait for all
  // dechunkifiers to exit since they relay events to the indexers. Thereafter
  // we can guaranatee that they have received all events and can shut them
  // down safely.
  // FIXME: since we provide a "naked" DOWN handler, we override the
  // flow-control DOWN handler. In this case it is safe because we have no
  // downstream nodes, but we should really figure out a better way to make
  // become() work with composite behaviors.
  auto phase2 = [=](down_msg const& down)
  {
    at(down);
    if (indexers_.empty())
      quit(msg.reason);
  };
  auto phase1 = [=](down_msg const& down)
  {
    at(down);
    if (! dechunkifiers_.empty())
      return;
    if (indexers_.empty())
    {
      quit(msg.reason);
      return;
    }
    become(phase2);
    for (auto& i : indexers_)
      send_exit(i.second, msg.reason);
  };
  if (! dechunkifiers_.empty())
  {
    become(phase1);
    return;
  }
  if (! indexers_.empty())
  {
    become(phase2);
    for (auto& i : indexers_)
      send_exit(i.second, msg.reason);
    return;
  }
  quit(msg.reason);
}

message_handler partition::make_handler()
{
  if (exists(dir_))
  {
    auto t = io::unarchive(dir_ / "schema", schema_);
    if (! t)
    {
      VAST_ERROR(this, "failed to load schema:", t.error());
      quit(exit::error);
      return {};
    }
    for (auto& p : directory{dir_})
    {
      auto basename = p.basename().str();
      if (p.is_directory())
      {
        // Extract the base ID from the path. Directories have the form a-b.
        auto dash = basename.find('-');
        if (dash < 1 || dash == std::string::npos)
        {
          VAST_WARN(this, "ignores directory with invalid format:", basename);
          continue;
        }
        auto left = basename.substr(0, dash);
        auto base = to<event_id>(left);
        if (! base)
        {
          VAST_WARN("ignores directory with invalid event ID:", left);
          continue;
        }
        // Load the indexer for each event.
        for (auto& inner : directory{p})
        {
          VAST_DEBUG(this, "loads", basename / inner.basename());
          auto a = spawn<event_indexer<bitstream_type>, monitored>(inner);
          indexers_.emplace(*base, a);
        }
      }
    }
  }

  return
  {
    [=](chunk const& chk, actor const& task)
    {
      VAST_DEBUG(this, "got chunk with", chk.events(), "events:",
                 '[' << chk.base() << ',' << (chk.base() + chk.events())
                 << ')');
      // Create event indexers according to chunk schema.
      auto base = chk.base();
      auto interval = to_string(base) + "-" + to_string(base + chk.events());
      std::vector<actor> indexers;
      for (auto& t : chk.meta().schema)
        if (! t.find_attribute(type::attribute::skip))
        {
          if (! schema_.add(t))
          {
            VAST_ERROR(this, "failed to incorporate types from new schema");
            quit(exit::error);
            return;
          }
          auto indexer = spawn<event_indexer<bitstream_type>, monitored>(
              dir_ / interval / t.name(), t);
          indexers.push_back(indexer);
          indexers_.emplace(base, std::move(indexer));
        }
      if (indexers.empty())
      {
        send_exit(task, exit::done);
        return;
      }
      // Spin up a dechunkifier for this chunk and tell it to send all events
      // to the freshly created indexers.
      auto dechunkifier = spawn<monitored>(
        [chk, indexers=std::move(indexers), task, proxy=proxy_]
        (event_based_actor* self)
        {
          assert(! indexers.empty());
          auto events = chk.uncompress();
          assert(! events.empty());
          auto msg = make_message(std::move(events), task);
          for (auto& i : indexers)
          {
            self->send(task, i);
            self->send(i, msg);
          }
          if (proxy != invalid_actor)
            self->send(proxy, std::move(indexers));
        });
      dechunkifiers_.insert(dechunkifier->address());
      ++chunks_indexed_concurrently_;
      check_overload();
      send(task, supervisor_atom::value, this);
      VAST_DEBUG(this, "indexes", chunks_indexed_concurrently_, "in parallel");
    },
    [=](done_atom, time::moment start, uint64_t events)
    {
      VAST_DEBUG(this, "indexed", events, "events in", time::snapshot() - start);
      --chunks_indexed_concurrently_;
      check_underload();
    },
    [=](expression const& expr, continuous_atom)
    {
      VAST_DEBUG(this, "got continuous query:", expr);
      if (! proxy_)
        proxy_ =
          spawn<continuous_query_proxy<default_bitstream>, linked>(sink_);
      send(proxy_, expr);
    },
    [=](expression const& expr, continuous_atom, disable_atom)
    {
      VAST_DEBUG(this, "got continuous query:", expr);
      if (! proxy_)
        VAST_WARN(this, "ignores disable request, no continuous queries");
      else
        send(proxy_, expr, disable_atom::value);
    },
    [=](expression const& expr, historical_atom)
    {
      VAST_DEBUG(this, "got historical query:", expr);
      auto q = queries_.emplace(expr, query_state()).first;
      if (! q->second.task)
      {
        // Even if we still have evaluated this query in the past, we still
        // spin up a new task to ensure that we incorporate results from chunks
        // that have arrived in the meantime.
        VAST_DEBUG(this, "spawns new query task");
        q->second.task = spawn<task>(time::snapshot(), q->first);
        send(q->second.task, supervisor_atom::value, this);
        send(q->second.task, this);
        for (auto& pred : visit(expr::predicatizer{}, expr))
        {
          auto p = predicates_.emplace(pred, predicate_state()).first;
          p->second.queries.insert(&q->first);
          for (auto& i : indexers_)
            // We forward this predicate only to the subset of indexers which we
            // haven't asked yet, i.e., those who's coverage is higher than
            // this predicate.
            if (! p->second.coverage.contains(i.first))
            {
              VAST_DEBUG(this, "forwards predicate to", i.second << ':', pred);
              p->second.coverage.insert(i.first);
              if (! p->second.task)
              {
                p->second.task = spawn<task>(time::snapshot(), p->first);
                send(p->second.task, supervisor_atom::value, this);
              }
              send(q->second.task, p->second.task);
              send(p->second.task, i.second);
              send(i.second, expression{pred}, this, p->second.task);
            }
        }
        send(q->second.task, done_atom::value);
      }
      if (! q->second.hits.empty() && ! q->second.hits.all_zeros())
        send(sink_, expr, q->second.hits, historical_atom::value);
    },
    [=](expression const& pred, bitstream_type const& hits)
    {
      VAST_DEBUG(this, "got", hits.count(), "hits for predicate:", pred);
      predicates_[*get<predicate>(pred)].hits |= hits;
    },
    [=](done_atom, time::moment start, predicate const& pred)
    {
      // Once we've completed all tasks of a certain predicate for all chunks,
      // we evaluate all queries in which the predicate participates.
      auto& ps = predicates_[pred];
      VAST_DEBUG(this, "took", time::snapshot() - start,
                 "to complete predicate for", ps.coverage.size(),
                 "indexers:", pred);
      for (auto& q : ps.queries)
      {
        assert(q != nullptr);
        VAST_DEBUG(this, "evaluates", *q);
        auto& qs = queries_[*q];
        auto hits = visit(evaluator{*this}, *q);
        if (! hits.empty() && ! hits.all_zeros() && hits != qs.hits)
        {
          VAST_DEBUG(this, "relays", hits.count(), "hits");
          qs.hits = hits;
          send(sink_, *q, std::move(hits), historical_atom::value);
        }
      }
      ps.task = invalid_actor;
    },
    [=](done_atom, time::moment start, expression const& expr)
    {
      VAST_DEBUG(this, "completed query", expr, "in",
                 time::snapshot() - start);
      queries_[expr].task = invalid_actor;
      send(sink_, current_message());
    },
    [=](flush_atom, actor const& task)
    {
      VAST_DEBUG(this, "got flush request");
      if (dechunkifiers_.empty())
      {
        flush(task);
      }
      else
      {
        VAST_DEBUG(this, "waits for dechunkifiers to exit");
        become(
          keep_behavior,
          [=](down_msg const& msg)
          {
            at(msg);
            if (dechunkifiers_.empty())
            {
              flush(task);
              unbecome();
            }
          });
      }
    }
  };
}

std::string partition::name() const
{
  return "partition";
}

void partition::flush(actor const& task)
{
  VAST_DEBUG(this, "peforms flush");
  send(task, this);
  for (auto& i : indexers_)
    if (i.second)
    {
      send(task, i.second);
      send(i.second, flush_atom::value, task);
    }
  if (! schema_.empty())
  {
    auto t = io::archive(dir_ / "schema", schema_);
    if (! t)
    {
      VAST_ERROR(this, "failed to flush:", t.error());
      quit(exit::error);
    }
  }
  send(task, done_atom::value);
}

} // namespace vast
