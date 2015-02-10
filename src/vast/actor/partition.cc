#include "vast/actor/partition.h"

#include <caf/all.hpp>
#include "vast/event.h"
#include "vast/actor/indexer.h"
#include "vast/actor/task.h"
#include "vast/expr/predicatizer.h"
#include "vast/io/serialization.h"

using namespace caf;

namespace vast {

struct partition::evaluator
{
  using bitstream_type = partition::bitstream_type;

  evaluator(partition const& p)
    : partition_{p}
  {
  }

  default_bitstream operator()(none) const
  {
    assert(! "should never happen");
    return {};
  }

  bitstream_type operator()(conjunction const& con) const
  {
    auto hits = visit(*this, con[0]);
    if (hits.empty() || hits.all_zeros())
      return {};
    for (size_t i = 1; i < con.size(); ++i)
    {
      hits &= visit(*this, con[i]);
      if (hits.empty() || hits.all_zeros()) // short-circuit
        return {};
    }
    return hits;
  }

  bitstream_type operator()(disjunction const& dis) const
  {
    bitstream_type hits;
    for (auto& op : dis)
    {
      hits |= visit(*this, op);
      if (! hits.empty() && hits.all_ones())  // short-circuit
        break;
    }
    return hits;
  }

  bitstream_type operator()(negation const& n) const
  {
    auto hits = visit(*this, n.expression());
    hits.flip();
    return hits;
  }

  bitstream_type operator()(predicate const& pred) const
  {
    auto& preds = partition_.predicates_;
    auto p = preds.find(pred);
    return p == preds.end() ? bitstream_type{} : p->second.hits;
  }

  partition const& partition_;
};

partition::partition(path const& dir)
  : dir_{dir}
{
  high_priority_exit(false);
  attach_functor([=](uint32_t)
    {
      dechunkifiers_.clear();
      indexers_.clear();
      predicates_.clear();
      queries_.clear();
    });
  overload_when([this] // TODO: calibrate values.
    {
      return dechunkifiers_.size() > 10 || chunk_tasks_.size() > 10;
    });
}

void partition::at(down_msg const& msg)
{
  auto d = dechunkifiers_.find(last_sender());
  if (d != dechunkifiers_.end())
  {
    dechunkifiers_.erase(d);
    check_overload();
    return;
  }
  // Indexers exit less regularly, so we check them after the dechunkifiers.
  for (auto i = indexers_.begin(); i != indexers_.end(); ++i)
    if (i->second.address() == msg.source)
    {
      indexers_.erase(i);
      check_underload();
      return;
    }
  // The same sink may exist with multiple predicates in the cache.
  for (auto& i : queries_)
    i.second.sinks.erase(actor_cast<actor>(msg.source));
}

void partition::at(exit_msg const& msg)
{
  if (msg.reason == exit::kill)
  {
    quit(msg.reason);
    return;
  }
  trap_exit(false);
  auto t = spawn<task, linked>(msg.reason);
  send(this, flush_atom::value, t);
  for (auto& i : indexers_)
    link_to(i.second);
  for (auto& q : queries_)
    link_to(q.second.task);
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
    [=](chunk const& c, actor const& task)
    {
      VAST_DEBUG(this, "got chunk with", c.events(), "events:",
                 '[' << c.base() << ',' << (c.base() + c.events()) << ')');
      send(task, this);
      send(task, supervisor_atom::value, this);
      chunk_tasks_.emplace(task->address(), c.events());
      auto base = c.base();
      auto i = to_string(base) + "-" + to_string(base + c.events());
      std::vector<actor> indexers;
      for (auto& t : c.meta().schema)
        if (! t.find_attribute(type::attribute::skip))
        {
          if (! schema_.add(t))
          {
            VAST_ERROR(this, "failed to incorporate types from new chunk schema");
            quit(exit::error);
            return;
          }
          auto indexer = spawn<event_indexer<bitstream_type>, monitored>(
              dir_ / i / t.name(), t);
          indexers.push_back(indexer);
          indexers_.emplace(base, std::move(indexer));
        }
      if (!indexers.empty())
      {
        auto dechunkifier = spawn<monitored>(
          [c, task](event_based_actor* self, std::vector<actor> indexers)
          {
            chunk::reader reader{c};
            std::vector<event> events(c.events());
            for (size_t i = 0; i < c.events(); ++i)
            {
              auto e = reader.read();
              if (! e)
              {
                self->quit(exit::error);
                return;
              }
              events[i] = std::move(*e);
            }
            auto msg = make_message(std::move(events), task);
            for (auto& i : indexers)
            {
              self->send(task, i);
              self->send(i, msg);
            }
          }, std::move(indexers));
        dechunkifiers_.insert(dechunkifier->address());
        check_overload();
      }
      send(task, done_atom::value);
      VAST_DEBUG(this, "runs", indexers_.size(), "indexers and",
                 dechunkifiers_.size(), "dechunkifiers for",
                 chunk_tasks_.size(), "chunks");
    },
    [=](expression const& expr, actor const& sink)
    {
      VAST_DEBUG(this, "got query for", sink << ':', expr);
      monitor(sink);
      auto q = queries_.emplace(expr, query_state{}).first;
      q->second.sinks.insert(sink);
      if (! q->second.task)
      {
        // Even if we still have evaluated this query in the past, we still
        // spin up a new task. This ensures that we incorporate results from
        // chunks that have arrived in the meantime.
        VAST_DEBUG(this, "spawns new query task");
        q->second.task = spawn<task>();
        query_tasks_.emplace(q->second.task->address(), &q->first);
        send(q->second.task, supervisor_atom::value, this);
        send(q->second.task, this);
        for (auto& pred : visit(expr::predicatizer{}, expr))
        {
          auto p = predicates_.emplace(pred, predicate_state{}).first;
          p->second.queries.insert(expr);
          for (auto& i : indexers_)
            if (! p->second.coverage.contains(i.first))
            {
              VAST_DEBUG(this, "forwards predicate to", i.second << ':', pred);
              p->second.coverage.insert(i.first);
              if (! p->second.task)
              {
                p->second.task = spawn<task>();
                send(p->second.task, supervisor_atom::value, this);
                predicate_tasks_.emplace(p->second.task.address(), &p->first);
              }
              send(q->second.task, p->second.task);
              send(p->second.task, i.second);
              send(i.second, expression{pred}, this, p->second.task);
            }
        }
        send(q->second.task, done_atom::value);
      }
      if (! q->second.hits.empty() && ! q->second.hits.all_zeros())
        send(sink, expr, q->second.hits);
    },
    [=](done_atom, time::duration runtime)
    {
      auto ct = chunk_tasks_.find(last_sender());
      if (ct != chunk_tasks_.end())
      {
        VAST_DEBUG(this, "indexed", ct->second, "events in", runtime);
        chunk_tasks_.erase(ct);
        return;
      }
      // Once we're done with an entire query, propagate this fact to all
      // sinks. This needs to happen in the same channel as the results flow,
      // checking the asynchronously running task would not work.
      auto qt = query_tasks_.find(last_sender());
      if (qt != query_tasks_.end())
      {
        VAST_DEBUG(this, "completed query", *qt->second, "in", runtime);
        auto& qs = queries_[*qt->second];
        auto msg = make_message(done_atom::value, runtime, *qt->second);
        for (auto& s : qs.sinks)
          send(s, msg);
        qs.task = invalid_actor;
        query_tasks_.erase(qt);
        return;
      }
      // Once we've completed all tasks pertaining to a certain predicate, we
      // we evaluate all queries in which this predicate occurs.
      auto pt = predicate_tasks_.find(last_sender());
      assert(pt != predicate_tasks_.end());
      auto& ps = predicates_[*pt->second];
      VAST_DEBUG(this, "took", runtime, "to complete predicate for",
                 ps.coverage.size(), "indexers:", *pt->second);
      for (auto& q : ps.queries)
      {
        VAST_DEBUG(this, "evaluates", q);
        auto& qs = queries_[q];
        auto result = visit(evaluator{*this}, q);
        if (! result.empty() && ! result.all_zeros() && result != qs.hits)
        {
          VAST_DEBUG(this, "relays", result.count(), "hits");
          qs.hits = std::move(result);
          auto msg = make_message(q, qs.hits);
          for (auto& sink : qs.sinks)
            send(sink, msg);
        }
      }
      ps.task = invalid_actor;
      predicate_tasks_.erase(pt);
    },
    [=](expression const& pred, bitstream_type const& hits)
    {
      VAST_DEBUG(this, "got", hits.count(), "hits for predicate:", pred);
      predicates_[*get<predicate>(pred)].hits |= hits;
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
