#include "vast/actor/index.h"

#include <caf/all.hpp>
#include "vast/bitmap_index.h"
#include "vast/chunk.h"
#include "vast/print.h"
#include "vast/query_options.h"
#include "vast/actor/partition.h"
#include "vast/actor/task.h"
#include "vast/expr/restrictor.h"
#include "vast/io/serialization.h"

using namespace caf;

namespace vast {

void index::partition_state::serialize(serializer& sink) const
{
  sink << events << from << to << last_modified;
}

void index::partition_state::deserialize(deserializer& source)
{
  source >> events >> from >> to >> last_modified;
}

index::index(path const& dir, size_t max_events,
             size_t max_parts, size_t active_parts)
  : dir_{dir / "index"},
    max_events_per_partition_{max_events},
    max_partitions_{max_parts},
    active_partitions_{active_parts}
{
  assert(max_events_per_partition_ > 0);
  assert(active_partitions_ > 0);
  assert(active_partitions_ < max_partitions_);
  high_priority_exit(false);
  attach_functor([=](uint32_t)
    {
      accountant_ = invalid_actor;
      queries_.clear();
      partitions_.clear();
    });
}

void index::at(down_msg const& msg)
{
  for (auto q = queries_.begin(); q != queries_.end(); ++q)
    if (q->second.subscribers.erase(actor_cast<actor>(msg.source)) == 1)
    {
      if (q->second.subscribers.empty())
      {
        VAST_VERBOSE(this, "removes query subscriber", msg.source);
        if (q->second.cont)
        {
          VAST_VERBOSE(this, "disables continuous query:", q->first);
          q->second.cont = {};
          for (auto& a : active_)
            send(partitions_[a].actor, q->first,
                 continuous_atom::value, disable_atom::value);
        }
        if (! q->second.cont && ! q->second.hist)
        {
          VAST_VERBOSE(this, "removes query:", q->first);
          queries_.erase(q);
        }
      }
      return;
    }
  // Because the set of existing partitions may by far exceed the set of
  // in-memory partitions, we only look at the latter ones.
  for (auto i = active_.begin(); i != active_.end(); ++i)
  {
    auto& p = partitions_[*i];
    if (p.actor.address() == msg.source)
    {
      p.actor = invalid_actor;
      active_.erase(i);
      VAST_DEBUG(this, "shrinks active partitions to",
                 active_.size() << '/' << active_partitions_);
      return;
    }
  }
  for (auto i = passive_.begin(); i != passive_.end(); ++i)
  {
    auto& p = partitions_[*i];
    if (p.actor.address() == msg.source)
    {
      p.actor = invalid_actor;
      passive_.erase(i);
      VAST_DEBUG(this, "shrinks passive partitions to", passive_.size() <<
                 '/' << max_partitions_ - active_partitions_);
      return;
    }
  }
}

void index::at(exit_msg const& msg)
{
  if (msg.reason == exit::kill)
  {
    quit(exit::kill);
    return;
  }
  flush();
  trap_exit(false); // Once the task completes we go down with it.
  auto t = spawn<task, linked>();
  send(t, msg.reason);
  for (auto& q : queries_)
    if (q.second.cont)
      link_to(q.second.cont->task);
    else if (q.second.hist)
      link_to(q.second.hist->task);
  for (auto& p : partitions_)
    if (p.second.actor)
      send(t, p.second.actor);
  for (auto& p : partitions_)
    if (p.second.actor)
      send_exit(p.second.actor, msg.reason);
}

message_handler index::make_handler()
{
  VAST_VERBOSE(this, "caps partition at", max_events_per_partition_, "events");
  VAST_VERBOSE(this, "uses", active_partitions_ << "/" << max_partitions_,
               "active partitions");

  if (exists(dir_ / "meta"))
  {
    auto t = io::unarchive(dir_ / "meta", partitions_);
    if (! t)
    {
      VAST_ERROR(this, "failed to load meta data:", t.error());
      quit(exit::error);
      return {};
    }
  }

  // Load the k last modified partitions that have not exceeded their capacity.
  std::vector<std::pair<uuid, partition_state>> parts;
  for (auto& p : partitions_)
    if (p.second.events < max_events_per_partition_)
      parts.push_back(p);

  std::sort(parts.begin(),
            parts.end(),
            [](auto& x, auto& y)
            {
              return y.second.last_modified < x.second.last_modified;
            });

  active_.resize(active_partitions_);
  for (size_t i = 0; i < active_partitions_; ++i)
  {
    auto id = i < parts.size() ? parts[i].first : uuid::random();
    auto& p = partitions_[id];
    VAST_VERBOSE(this, "activates partition", id);
    p.actor = spawn<partition, monitored>(dir_ / to_string(id), this);
    send(p.actor, flow_control::announce{this});
    active_[i] = std::move(id);
  }

  return
  {
    [=](accountant_atom, actor const& accountant)
    {
      VAST_DEBUG(this, "registers accountant", accountant);
      accountant_ = accountant;
    },
    [=](flush_atom, actor const& task)
    {
      VAST_VERBOSE(this, "flushes", active_.size(), "active partitions");
      send(task, this);
      for (auto& id : active_)
      {
        auto& p = partitions_[id].actor;
        send(p, flush_atom::value, task);
      }
      flush();
      send(task, done_atom::value);
    },
    [=](chunk const& chk)
    {
      auto& part = active_[next_++];
      next_ %= active_.size();
      auto i = partitions_.find(part);
      assert(i != partitions_.end());
      assert(i->second.actor);
      // Replace partition with a new one on overflow. If the max is too small
      // that even the first chunk doesn't fit, then we just accept this and
      // have a one-chunk partition.
      if (i->second.events > 0
          && i->second.events + chk.events() > max_events_per_partition_)
      {
        VAST_VERBOSE(this, "replaces partition (" << part << ')');
        send_exit(i->second.actor, exit::stop);
        i->second.actor = invalid_actor;
        // Create a new partition.
        part = uuid::random();
        i = partitions_.emplace(part, partition_state()).first;
        i->second.actor =
          spawn<partition, monitored>(dir_ / to_string(part), this);
        send(i->second.actor, flow_control::announce{this});
        // Register continuous queries.
        for (auto& q : queries_)
          if (q.second.cont)
            send(i->second.actor, q.first, continuous_atom::value);
      }
      // Update partition meta data.
      auto& p = i->second;
      p.events += chk.events();
      p.last_modified = time::now();
      if (p.from == time::duration{} || chk.meta().first < p.from)
        p.from = chk.meta().first;
      if (p.to == time::duration{} || chk.meta().last > p.to)
        p.to = chk.meta().last;
      // Relay chunk.
      VAST_DEBUG(this, "forwards chunk [" << chk.base() << ',' << chk.base() +
                 chk.events() << ')', "to", p.actor, '(' << part << ')');
      auto t = spawn<task>(chk.events());
      send(t, supervisor_atom::value, this);
      send(p.actor, chk, t);
    },
    [=](expression const& expr, query_options opts, actor const& subscriber)
    {
      VAST_VERBOSE(this, "got query:", expr);
      if (opts == no_query_options)
      {
        VAST_WARN(this, "ignores query with no options:", expr);
        return;
      }
      monitor(subscriber);
      auto& qs = queries_[expr];
      qs.subscribers.insert(subscriber);
      if (has_historical_option(opts))
      {
        if (! qs.hist)
        {
          VAST_DEBUG(this, "instantiates historical query");
          qs.hist = historical_query_state();
        }
        if (! qs.hist->task)
        {
          VAST_VERBOSE(this, "enables historical query");
          qs.hist->task = spawn<task>();
          // Test whether this query matches any partition and relay it where
          // possible.
          for (auto& p : partitions_)
            if (visit(expr::time_restrictor{p.second.from, p.second.to}, expr))
              if (auto a = dispatch(p.first, expr))
              {
                qs.hist->parts.emplace(a->address(), p.first);
                send(qs.hist->task, *a);
                send(*a, expr, historical_atom::value);
              }
          if (qs.hist->parts.empty())
          {
            VAST_DEBUG(this, "did not find a qualifying partition for query");
            send_exit(qs.hist->task, exit::done);
            qs.hist->task = invalid_actor;
          }
        }
        send(subscriber, qs.hist->task);
        if (! qs.hist->hits.empty() && ! qs.hist->hits.all_zeros())
          send(subscriber, qs.hist->hits);
        if (! qs.hist->task && ! has_continuous_option(opts))
          queries_.erase(expr);
      }
      if (has_continuous_option(opts))
      {
        if (! qs.cont)
        {
          VAST_DEBUG(this, "instantiates continuous query");
          qs.cont = continuous_query_state();
        }
        if (! qs.cont->task)
        {
          VAST_VERBOSE(this, "enables continuous query");
          qs.cont->task = spawn<task>();
          send(qs.cont->task, this);
          // Relay the continuous query to all active partitions, as these may
          // still receive chunks.
          for (auto& a : active_)
            send(partitions_[a].actor, expr, continuous_atom::value);
        }
        send(subscriber, qs.cont->task);
        if (! qs.cont->hits.empty() && ! qs.cont->hits.all_zeros())
          send(subscriber, qs.cont->hits);
      }
    },
    [=](expression const& expr, continuous_atom, disable_atom)
    {
      VAST_VERBOSE(this, "got request to disable continuous query:", expr);
      auto q = queries_.find(expr);
      if (q == queries_.end())
      {
        VAST_WARN(this, "has not such query:", expr);
      }
      else if (! q->second.cont)
      {
        VAST_WARN(this, "has already disabled query:", expr);
      }
      else
      {
        VAST_VERBOSE(this, "disables continuous query:", expr);
        send(q->second.cont->task, done_atom::value);
        q->second.cont->task = invalid_actor;
      }
    },
    [=](done_atom, time::duration runtime, uint64_t events)
    {
      VAST_VERBOSE(this, "indexed", events, "events in", runtime);
      if (accountant_)
        send(accountant_, time::now(), description() + "-events", events);
    },
    [=](done_atom, time::duration runtime, expression const& expr)
    {
      VAST_DEBUG(this, "got signal that", last_sender(),
                 "finished for historical query: ", expr);
      auto q = queries_.find(expr);
      assert(q != queries_.end());
      assert(q->second.hist);
      auto p = q->second.hist->parts.find(last_sender());
      assert(p != q->second.hist->parts.end());
      consolidate(p->second, expr);
      send(q->second.hist->task, done_atom::value, p->first);
      q->second.hist->parts.erase(p);
      if (q->second.hist->parts.empty())
      {
        VAST_VERBOSE(this, "completed query", expr, "in", runtime);
        for (auto& s : q->second.subscribers)
          send(s, last_dequeued());
        // TODO: consider caching it for a while and also record its coverage
        // so that future queries don't need to start over again.
        queries_.erase(q);
      }
    },
    [=](expression const& expr, bitstream_type const& hits, historical_atom)
    {
      VAST_DEBUG(this, "received", hits.count(), "historical hits from",
                 last_sender(), "for query:", expr);
      auto& qs = queries_[expr];
      assert(qs.hist);
      auto before = qs.hist->hits.count();
      qs.hist->hits |= hits;
      auto after = qs.hist->hits.count();
      if (after > 0 && after > before)
        for (auto& s : qs.subscribers)
          // TODO: just send 'hits' to avoid sending redundant information
          // already sent in the past.
          send(s, qs.hist->hits);
    },
    [=](expression const& expr, bitstream_type const& hits, continuous_atom)
    {
      VAST_DEBUG(this, "received", hits.count(), "continuous hits from",
                 last_sender(), "for query:", expr);
      auto& qs = queries_[expr];
      assert(qs.cont);
      qs.cont->hits |= hits;
      for (auto& s : qs.subscribers)
        send(s, qs.cont->hits); // TODO: see note above.
    }
  };
}

std::string index::name() const
{
  return "index";
}

optional<actor> index::dispatch(uuid const& part, expression const& expr)
{
  auto& ps = partitions_[part];
  if (ps.events == 0)
    return {};

  auto i = std::find_if(
      schedule_.begin(),
      schedule_.end(),
      [&](schedule_state const& s) { return s.part == part; });

  // If the partition is already scheduled, we add the expression to the set of
  // to-be-queried expressions.
  if (i != schedule_.end())
  {
    VAST_DEBUG(this, "adds expression to", part << ":", expr);
    i->queries.insert(expr);
    // If the partition is in memory we can send it the query directly.
    if (ps.actor != invalid_actor)
      return ps.actor;
    else
      return {};
  }

  // If the partition is not in memory we enqueue it in the schedule.
  VAST_DEBUG(this, "enqueues partition", part, "with", expr);
  schedule_.push_back(index::schedule_state{part, {expr}});

  // If the partition is active (and has events), we can just relay the expression.
  if (std::find(active_.begin(), active_.end(), part) != active_.end())
    return ps.actor;

  // If we have not fully maxed out our available passive partitions, we can
  // spawn the partition directly.
  if (passive_.size() < max_partitions_ - active_partitions_)
  {
    passive_.push_back(part);
    VAST_DEBUG(this, "spawns passive partition", part);
    auto& a = partitions_[part].actor;
    assert(! a);
    a = spawn<partition, monitored>(dir_ / to_string(part), this);
    send(a, flow_control::announce{this});
    return a;
  }

  return {};
}

void index::consolidate(uuid const& part, expression const& expr)
{
  VAST_DEBUG(this, "consolidates", expr, "for partition", part);

  auto i = std::find_if(
      schedule_.begin(),
      schedule_.end(),
      [&](schedule_state const& s) { return s.part == part; });

  // Remove the completed expression of the partition.
  assert(i != schedule_.end());
  assert(! i->queries.empty());
  auto x = i->queries.find(expr);
  assert(x != i->queries.end());
  i->queries.erase(x);

  // We keep the partition in the schedule as long it has outstanding queries.
  if (! i->queries.empty())
  {
    VAST_DEBUG(this, "got completed query", expr, "for partition",
               part << ',', i->queries.size(), "remaining");
    return;
  }

  VAST_DEBUG(this, "removes partition from schedule:", part);
  schedule_.erase(i);
  if (schedule_.empty())
    VAST_DEBUG(this, "finished with entire schedule");

  // We never unload active partitions.
  if (std::find(active_.begin(), active_.end(), part) != active_.end())
    return;

  // If we're not dealing with an active partition, it must exist in the
  // passive list, unless we dispatched an expression to an active partition
  // and that got replaced with a new one. Then the replaced partition is
  // neither in the active nor passive set and already being taken care of, so
  // we can safely ignore this consolidation request.
  auto j = std::find(passive_.begin(), passive_.end(), part);
  if (j == passive_.end())
    return;
  else
    passive_.erase(j);

  VAST_DEBUG(this, "evicts partition from memory:", part);
  auto& p = partitions_[part];
  assert(p.actor);
  send_exit(p.actor, exit::stop);
  p.actor = invalid_actor;

  // Now that we've unloaded one, load the next. Because partitions can
  // complete in any order, we have to walk through the schedule from the
  // beginning again to find the next partition to load.
  for (auto& entry : schedule_)
  {
    auto& next_part = partitions_[entry.part].actor;
    if (! next_part)
    {
      VAST_DEBUG(this, "schedules next partition", entry.part);
      passive_.push_back(entry.part);
      next_part =
        spawn<partition, monitored>(dir_ / to_string(entry.part), this);
      send(next_part, flow_control::announce{this});
      for (auto& next_expr : entry.queries)
      {
        auto q = queries_.find(next_expr);
        assert(q != queries_.end());
        assert(q->second.hist);
        q->second.hist->parts.emplace(next_part->address(), entry.part);
        send(q->second.hist->task, next_part);
        send(next_part, next_expr, historical_atom::value);
      }
      break;
    }
  }
}

void index::flush()
{
  for (auto& p : partitions_)
    if (p.second.events > 0)
    {
      auto t = io::archive(dir_ / "meta", partitions_);
      if (! t)
      {
        VAST_ERROR(this, "failed to save meta data:", t.error());
        quit(exit::error);
      }
    }
}

} // namespace vast
