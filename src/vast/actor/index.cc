#include "vast/actor/index.h"

#include <caf/all.hpp>
#include "vast/bitmap_index.h"
#include "vast/chunk.h"
#include "vast/print.h"
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

  attach_functor(
      [=](uint32_t)
      {
        auto t = flush();
        if (! t)
          VAST_ERROR(this, "failed to save meta data:", t.error());
        queries_.clear();
        partitions_.clear();
      });
}

void index::at(down_msg const& msg)
{
  VAST_DEBUG(this, "got DOWN from", msg.source);
  for (auto& q : queries_)
    if (q.second.subscribers.erase(actor_cast<actor>(msg.source)) == 1)
      return;
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
  VAST_DEBUG(this, "got EXIT from", msg.source);
  if (msg.reason == exit::kill)
  {
    for (auto& p : partitions_)
      if (p.second.actor)
        send_exit(p.second.actor, exit::kill);
    quit(msg.reason);
    for (auto& p : partitions_)
      if (p.second.actor)
        send_exit(p.second.actor, exit::kill);
    for (auto& q : queries_)
      send_exit(q.second.task, exit::kill);
    quit(exit::kill);
  }
  else
  {
    trap_exit(false);
    auto t = spawn<task, linked>(msg.reason);
    send(this, atom("flush"), t);
    for (auto& p : partitions_)
      if (p.second.actor)
        link_to(p.second.actor);
    for (auto& q : queries_)
      link_to(q.second.task);
  }
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
    VAST_DEBUG(this, "activates partition", id);
    p.actor = spawn<partition, monitored>(dir_ / to_string(id));
    send(p.actor, flow_control::announce{this});
    active_[i] = std::move(id);
  }

  return
  {
    on(atom("flush"), arg_match) >> [=](actor const& task)
    {
      VAST_DEBUG(this, "flushes active partitions");
      send(task, this);
      for (auto& id : active_)
      {
        auto& p = partitions_[id].actor;
        send(task, p);
        send(p, atom("flush"), task);
      }
      auto t = flush();
      if (! t)
      {
        VAST_ERROR(this, "failed to save meta data:", t.error());
        quit(exit::error);
      }
      send(task, atom("done"));
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
      // TODO: We could manually split up chunks that are too large to fit the
      // partition limit.
      if (i->second.events > 0
          && i->second.events + chk.events() > max_events_per_partition_)
      {
        VAST_VERBOSE(this, "replaces partition (" << part << ')');
        send_exit(i->second.actor, exit::stop);
        i->second.actor = invalid_actor;

        part = uuid::random();
        i = partitions_.emplace(part, partition_state{}).first;
        i->second.actor = spawn<partition, monitored>(dir_ / to_string(part));
        send(i->second.actor, flow_control::announce{this});
        // TODO: once we have live queries in place, we must inform the new
        // partition about them at this point.
      }

      // Update partition meta data.
      auto& p = i->second;
      p.events += chk.events();
      p.last_modified = now();
      if (p.from == time_range{} || chk.meta().first < p.from)
        p.from = chk.meta().first;
      if (p.to == time_range{} || chk.meta().last > p.to)
        p.to = chk.meta().last;

      VAST_DEBUG(this, "forwards chunk to", p.actor, '(' << part << ')');
      forward_to(p.actor);
    },
    [=](expression const& expr, actor const& sink)
    {
      VAST_DEBUG(this, "got query for", sink << ':', expr);
      monitor(sink);
      auto q = queries_.find(expr);
      if (q != queries_.end())
      {
        VAST_DEBUG(this, "found query with", q->second.hits.count(), "hits");
        assert(q->second.task != invalid_actor);
        q->second.subscribers.insert(sink);
        send(sink, q->second.task);
        if (! q->second.hits.empty() && ! q->second.hits.all_zeros())
          send(sink, q->second.hits);
        return;
      }
      query_state qs;
      VAST_DEBUG(this, "spawns new query task");
      qs.subscribers.insert(sink);
      qs.task = spawn<task>();
      send(sink, qs.task);
      for (auto& p : partitions_)
        if (visit(expr::time_restrictor{p.second.from, p.second.to}, expr))
          if (auto a = dispatch(p.first, expr))
          {
            qs.parts.emplace(*a, p.first);
            send(qs.task, *a);
            send(*a, expr, this);
          }
      if (qs.parts.empty())
      {
        VAST_DEBUG(this, "did not find a qualifying partition for query");
        send_exit(qs.task, exit::done);
      }
      else
      {
        VAST_DEBUG(this, "queried with", qs.parts.size(), "partitions");
        queries_.emplace(expr, qs);
      }
    },
    on(atom("done"), arg_match) >> [=](expression const& expr)
    {
      VAST_DEBUG(this, "got signal that", last_sender(), "finished for", expr);
      auto q = queries_.find(expr);
      assert(q != queries_.end());
      auto p = q->second.parts.find(actor_cast<actor>(last_sender()));
      assert(p != q->second.parts.end());
      consolidate(p->second, expr);
      send(q->second.task, atom("done"), p->first);
      q->second.parts.erase(p);
      if (q->second.parts.empty())
      {
        VAST_DEBUG(this, "completed query", expr);
        for (auto& s : q->second.subscribers)
          send(s, last_dequeued());
        queries_.erase(q);
      }
    },
    [=](expression const& expr, bitstream_type const& hits)
    {
      VAST_DEBUG(this, "received", hits.count(), "hits from",
                 last_sender(), "for query", expr);

      auto& qs = queries_[expr];
      auto before = qs.hits.count();
      qs.hits |= hits;
      auto after = qs.hits.count();
      if (after > 0 && after > before)
        for (auto& s : qs.subscribers)
          send(s, qs.hits);
    },
    on(atom("delete")) >> [=]
    {
      for (auto& id : active_)
        send_exit(partitions_[id].actor, exit::kill);

      become(
          keep_behavior,
          [=](down_msg const& msg)
          {
            if (msg.reason != exit::kill)
              VAST_WARN(this, "got DOWN from", msg.source,
                        "with unexpected exit code", msg.reason);

            for (auto i = active_.begin(); i != active_.end(); ++i)
              if (partitions_[*i].actor == msg.source)
              {
                active_.erase(i);
                break;
              }

            if (active_.empty())
            {
              if (! rm(dir_))
              {
                VAST_ERROR(this, "failed to delete index directory:", dir_);
                send_exit(this, exit::error);
                return;
              }

              VAST_INFO(this, "deleted index:", dir_);
              unbecome();
            }
          }
      );
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
    a = spawn<partition, monitored>(dir_ / to_string(part));
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

  VAST_DEBUG(this, "unloads partition from memory:", part);
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
      next_part = spawn<partition, monitored>(dir_ / to_string(entry.part));
      send(next_part, flow_control::announce{this});
      for (auto& next_expr : entry.queries)
      {
        auto q = queries_.find(next_expr);
        assert(q != queries_.end());
        q->second.parts.emplace(next_part, entry.part);
        send(q->second.task, next_part);
        send(next_part, next_expr, this);
      }
      break;
    }
  }
}

trial<void> index::flush()
{
  for (auto& p : partitions_)
    if (p.second.events > 0)
      return io::archive(dir_ / "meta", partitions_);
  return nothing;
}

} // namespace vast
