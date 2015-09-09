#include <caf/all.hpp>

#include "vast/bitmap_index.h"
#include "vast/event.h"
#include "vast/query_options.h"
#include "vast/actor/index.h"
#include "vast/actor/partition.h"
#include "vast/actor/task.h"
#include "vast/expr/restrictor.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/expression.h"
#include "vast/concept/printable/vast/error.h"
#include "vast/concept/printable/vast/uuid.h"
#include "vast/concept/serializable/io.h"
#include "vast/concept/serializable/state.h"
#include "vast/concept/serializable/std/array.h"
#include "vast/concept/serializable/std/chrono.h"
#include "vast/concept/serializable/std/unordered_map.h"
#include "vast/concept/state/uuid.h"
#include "vast/concept/state/time.h"
#include "vast/util/assert.h"

namespace vast {

template <typename Serializer>
void serialize(Serializer& sink, index::partition_state const& ps) {
  sink << ps.events << ps.from << ps.to << ps.last_modified;
}

template <typename Deserializer>
void deserialize(Deserializer& source, index::partition_state& ps) {
  source >> ps.events >> ps.from >> ps.to >> ps.last_modified;
}

index::index(path const& dir, size_t max_events, size_t passive_parts,
             size_t active_parts)
  : flow_controlled_actor{"index"},
    dir_{dir},
    max_events_per_partition_{max_events} {
  trap_exit(true);
  VAST_ASSERT(max_events_per_partition_ > 0);
  VAST_ASSERT(active_parts > 0);
  VAST_ASSERT(passive_parts > 0);
  active_.resize(active_parts);
  passive_.capacity(passive_parts);
  passive_.on_evict([=](uuid id, actor& p) {
    VAST_DEBUG(this, "evicts partition", id);
    send_exit(p, exit::stop);
  });
}

void index::on_exit() {
  queries_.clear();
}

behavior index::make_behavior() {
  VAST_VERBOSE(this, "caps partitions at", max_events_per_partition_, "events");
  VAST_VERBOSE(this, "uses at most", passive_.capacity(), "passive partitions");
  VAST_VERBOSE(this, "uses", active_.size(), "active partitions");
  // Load meta data about each partition.
  if (exists(dir_ / "meta")) {
    using vast::load;
    auto t = load(dir_ / "meta", partitions_);
    if (!t) {
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
  std::sort(parts.begin(), parts.end(), [](auto& x, auto& y) {
    return y.second.last_modified < x.second.last_modified;
  });
  for (size_t i = 0; i < active_.size(); ++i) {
    auto id = i < parts.size() ? parts[i].first : uuid::random();
    VAST_VERBOSE(this, "spawns", (i < parts.size() ? "existing" : "new"),
                 "active partition", id);
    auto p = spawn<partition, monitored>(dir_ / to_string(id), this);
    send(p, upstream_atom::value, this);
    active_[i] = {id, p};
    partitions_[id].last_modified = time::now();
  }
  return {
    forward_overload(),
    forward_underload(),
    register_upstream_node(),
    [=](exit_msg const& msg) {
      if (msg.reason == exit::kill) {
        quit(exit::kill);
        return;
      }
      if (downgrade_exit())
        return;
      flush();
      trap_exit(false); // Once the task completes we go down with it.
      auto t = spawn<task, linked>();
      send(t, msg.reason);
      for (auto& q : queries_)
        if (q.second.cont)
          link_to(q.second.cont->task);
        else if (q.second.hist)
          link_to(q.second.hist->task);
      for (auto& a : active_)
        send(t, a.second);
      for (auto p : passive_)
        send(t, p.second);
      for (auto& a : active_)
        send_exit(a.second, msg.reason);
      for (auto p : passive_)
        send_exit(p.second, msg.reason);
    },
    [=](down_msg const& msg) {
      if (remove_upstream_node(msg.source))
        return;
      for (auto q = queries_.begin(); q != queries_.end(); ++q)
        if (q->second.subscribers.erase(actor_cast<actor>(msg.source)) == 1) {
          if (q->second.subscribers.empty()) {
            VAST_VERBOSE(this, "removes query subscriber", msg.source);
            if (q->second.cont) {
              VAST_VERBOSE(this, "disables continuous query:", q->first);
              q->second.cont = {};
              for (auto& a : active_)
                send(a.second, q->first, continuous_atom::value,
                     disable_atom::value);
            }
            if (!q->second.cont && !q->second.hist) {
              VAST_VERBOSE(this, "removes query:", q->first);
              queries_.erase(q);
            }
          }
          return;
        }
      for (auto i = active_.begin(); i != active_.end(); ++i) {
        if (i->second.address() == msg.source) {
          VAST_DEBUG(this, "removes active partitions", i->first);
          active_.erase(i);
          return;
        }
      }
      for (auto i = passive_.begin(); i != passive_.end(); ++i) {
        if (i->second.address() == msg.source) {
          passive_.erase(i->first);
          VAST_DEBUG(this, "shrinks passive partitions to",
                     passive_.size() << '/' << passive_.capacity());
          return;
        }
      }
    },
    [=](accountant::actor_type const& acc) {
      VAST_DEBUG(this, "registers accountant#", acc->id());
      accountant_ = acc;
      for (auto& pair : active_)
        send(pair.second, acc);
    },
    [=](flush_atom) {
      VAST_VERBOSE(this, "flushes", active_.size(), "active partitions");
      auto t = spawn<task>();
      send(t, this);
      for (auto& a : active_)
        send(a.second, flush_atom::value, t);
      flush();
      send(t, done_atom::value);
      return t;
    },
    [=](std::vector<event> const& events) {
      auto& a = active_[next_active_++ % active_.size()];
      auto i = partitions_.find(a.first);
      VAST_ASSERT(i != partitions_.end());
      VAST_ASSERT(a.second != invalid_actor);
      // Replace partition with a new one on overflow. If the max is too small
      // that even the first batch doesn't fit, then we just accept this and
      // have a partition with a single batch.
      if (i->second.events > 0
          && i->second.events + events.size() > max_events_per_partition_) {
        VAST_VERBOSE(this, "replaces partition (" << a.first << ')');
        send_exit(a.second, exit::stop);
        // Create a new partition.
        a.first = uuid::random();
        a.second = spawn<partition, monitored>(dir_ / to_string(a.first), this);
        if (accountant_)
          send(a.second, accountant_);
        send(a.second, upstream_atom::value, this);
        i = partitions_.emplace(a.first, partition_state()).first;
        // Register continuous queries.
        for (auto& q : queries_)
          if (q.second.cont)
            send(a.second, q.first, continuous_atom::value);
      }
      // Update partition meta data.
      auto& p = i->second;
      p.events += events.size();
      p.last_modified = time::now();
      if (p.from == time::duration{} || events.front().timestamp() < p.from)
        p.from = events.front().timestamp();
      if (p.to == time::duration{} || events.back().timestamp() > p.to)
        p.to = events.back().timestamp();
      // Relay events.
      VAST_DEBUG(this, "forwards", events.size(), "events [" <<
                 events.front().id() << ',' << (events.back().id() + 1) << ')',
                 "to", a.second, '(' << a.first << ')');
      auto t = spawn<task>(time::snapshot(), uint64_t{events.size()});
      send(a.second, current_message() + make_message(std::move(t)));
    },
    [=](expression const& expr, query_options opts, actor const& subscriber) {
      VAST_VERBOSE(this, "got query:", expr);
      if (opts == no_query_options) {
        VAST_WARN(this, "ignores query with no options:", expr);
        return;
      }
      monitor(subscriber);
      auto& qs = queries_[expr];
      qs.subscribers.insert(subscriber);
      if (has_historical_option(opts)) {
        if (!qs.hist) {
          VAST_DEBUG(this, "instantiates historical query");
          qs.hist = historical_query_state();
        }
        if (!qs.hist->task) {
          VAST_VERBOSE(this, "enables historical query");
          qs.hist->task
            = spawn<task>(time::snapshot(), expr, historical_atom::value);
          send(qs.hist->task, supervisor_atom::value, this);
          // Test whether this query matches any partition and relay it where
          // possible.
          for (auto& p : partitions_)
            if (visit(expr::time_restrictor{p.second.from, p.second.to}, expr))
              if (auto a = dispatch(p.first, expr)) {
                qs.hist->parts.emplace(a->address(), p.first);
                send(qs.hist->task, *a);
                send(*a, expr, historical_atom::value);
              }
          if (qs.hist->parts.empty()) {
            VAST_DEBUG(this, "did not find a qualifying partition for query");
            send_exit(qs.hist->task, exit::done);
            qs.hist->task = invalid_actor;
          }
        }
        send(subscriber, qs.hist->task);
        if (!qs.hist->hits.empty() && !qs.hist->hits.all_zeros()) {
          VAST_VERBOSE(this, "relays", qs.hist->hits.count(), "cached hits");
          send(subscriber, qs.hist->hits);
        }
      }
      if (has_continuous_option(opts)) {
        if (!qs.cont) {
          VAST_DEBUG(this, "instantiates continuous query");
          qs.cont = continuous_query_state();
        }
        if (!qs.cont->task) {
          VAST_VERBOSE(this, "enables continuous query");
          qs.cont->task = spawn<task>(time::snapshot());
          send(qs.cont->task, this);
          // Relay the continuous query to all active partitions, as these may
          // still receive events.
          for (auto& a : active_)
            send(a.second, expr, continuous_atom::value);
        }
        send(subscriber, qs.cont->task);
        if (!qs.cont->hits.empty() && !qs.cont->hits.all_zeros())
          send(subscriber, qs.cont->hits);
      }
    },
    [=](expression const& expr, continuous_atom, disable_atom) {
      VAST_VERBOSE(this, "got request to disable continuous query:", expr);
      auto q = queries_.find(expr);
      if (q == queries_.end()) {
        VAST_WARN(this, "has not such query:", expr);
      } else if (!q->second.cont) {
        VAST_WARN(this, "has already disabled query:", expr);
      } else {
        VAST_VERBOSE(this, "disables continuous query:", expr);
        send(q->second.cont->task, done_atom::value);
        q->second.cont->task = invalid_actor;
      }
    },
    [=](done_atom, time::moment start, expression const& expr) {
      auto runtime = time::snapshot() - start;
      VAST_DEBUG(this, "got signal that", current_sender(), "took", runtime,
                 "to complete query: ", expr);
      auto q = queries_.find(expr);
      VAST_ASSERT(q != queries_.end());
      VAST_ASSERT(q->second.hist);
      auto p = q->second.hist->parts.find(current_sender());
      VAST_ASSERT(p != q->second.hist->parts.end());
      consolidate(p->second, expr);
      send(q->second.hist->task, done_atom::value, p->first);
      q->second.hist->parts.erase(p);
    },
    [=](done_atom, time::moment start, expression const& expr,
        historical_atom) {
      auto runtime = time::snapshot() - start;
      VAST_VERBOSE(this, "completed lookup", expr, "in", runtime);
      auto q = queries_.find(expr);
      VAST_ASSERT(q != queries_.end());
      VAST_ASSERT(q->second.hist);
      VAST_ASSERT(q->second.hist->parts.empty());
      // Notify subscribers about completion.
      for (auto& s : q->second.subscribers)
        send(s, done_atom::value, runtime, expr);
      // Remove query state.
      // TODO: consider caching it for a while and also record its coverage
      // so that future queries don't need to start over again.
      q->second.hist->task = invalid_actor;
      queries_.erase(q);
    },
    [=](expression const& expr, bitstream_type& hits, historical_atom) {
      VAST_DEBUG(this, "received", hits.count(), "historical hits from",
                 current_sender(), "for query:", expr);
      auto& qs = queries_[expr];
      VAST_ASSERT(qs.hist);
      auto delta = hits - qs.hist->hits;
      if (delta.count() > 0) {
        qs.hist->hits |= delta;
        auto msg = make_message(std::move(delta));
        for (auto& s : qs.subscribers)
          send(s, msg);
      }
    },
    [=](expression const& expr, bitstream_type& hits, continuous_atom) {
      VAST_DEBUG(this, "received", hits.count(), "continuous hits from",
                 current_sender(), "for query:", expr);
      auto& qs = queries_[expr];
      VAST_ASSERT(qs.cont);
      qs.cont->hits |= hits;
      auto msg = make_message(std::move(hits));
      for (auto& s : qs.subscribers)
        send(s, msg);
    },
    catch_unexpected()};
}

optional<actor> index::dispatch(uuid const& part, expression const& expr) {
  if (partitions_[part].events == 0)
    return {};
  // If the partition is already scheduled, we add the expression to the set of
  // to-be-queried expressions.
  auto schedule_pred = [&](auto& s) { return s.part == part; };
  auto i = std::find_if(schedule_.begin(), schedule_.end(), schedule_pred);
  if (i == schedule_.end()) {
    VAST_DEBUG(this, "enqueues partition", part, "with", expr);
    schedule_.push_back(index::schedule_state{part, {expr}});
  } else {
    VAST_DEBUG(this, "adds expression to", part << ":", expr);
    i->queries.insert(expr);
  }
  // If the partition is in memory, we send it the expression directly.
  for (auto& a : active_)
    if (a.first == part)
      return a.second;
  if (auto p = passive_.lookup(part))
    return *p;
  // If we have not fully maxed out our available passive partitions, we can
  // spawn the partition directly.
  if (passive_.size() < passive_.capacity()) {
    VAST_DEBUG(this, "spawns passive partition", part);
    auto p = spawn<partition, monitored>(dir_ / to_string(part), this);
    if (accountant_)
      send(p, accountant_);
    send(p, upstream_atom::value, this);
    passive_.insert(part, p);
    return p;
  }
  return {};
}

void index::consolidate(uuid const& part, expression const& expr) {
  VAST_DEBUG(this, "consolidates", part, "for", expr);
  auto schedule_pred = [&](auto& s) { return s.part == part; };
  auto i = std::find_if(schedule_.begin(), schedule_.end(), schedule_pred);
  // Remove the completed query expression from the schedule.
  VAST_ASSERT(i != schedule_.end());
  VAST_ASSERT(!i->queries.empty());
  auto x = i->queries.find(expr);
  VAST_ASSERT(x != i->queries.end());
  i->queries.erase(x);
  // We keep the partition in the schedule as long it has outstanding queries.
  if (!i->queries.empty()) {
    VAST_DEBUG(this, "got completed query", expr, "for partition", part << ',',
               i->queries.size(), "remaining");
    return;
  }
  VAST_DEBUG(this, "removes partition from schedule:", part);
  schedule_.erase(i);
  if (schedule_.empty())
    VAST_DEBUG(this, "finished with entire schedule");
  // We never unload active partitions.
  auto part_pred = [&](auto& a) { return a.first == part; };
  if (std::find_if(active_.begin(), active_.end(), part_pred) != active_.end())
    return;
  // If we're not dealing with an active partition, it must exist in the
  // passive list, unless we dispatched an expression to an active partition
  // and that got replaced with a new one. In the latter case the replaced
  // partition is neither in the active nor passive set and has already being
  // taken care of, so we can safely ignore this consolidation request.
  if (passive_.lookup(part) == nullptr)
    return;
  // For each consolidated passive partition, we load another new one. Because
  // partitions can complete in any order, we have to walk through the schedule
  // from the beginning again to find the next passive partition to load.
  for (auto& entry : schedule_) {
    auto entry_pred = [&](auto& a) { return a.first == entry.part; };
    auto a = std::find_if(active_.begin(), active_.end(), entry_pred);
    if (a == active_.end() && !passive_.contains(entry.part)) {
      VAST_DEBUG(this, "schedules next passive partition", entry.part);
      auto p = spawn<partition, monitored>(dir_ / to_string(entry.part), this);
      if (accountant_)
        send(p, accountant_);
      send(p, upstream_atom::value, this);
      passive_.insert(entry.part, p); // automatically evicts 'part'.
      for (auto& next_expr : entry.queries) {
        auto q = queries_.find(next_expr);
        VAST_ASSERT(q != queries_.end());
        VAST_ASSERT(q->second.hist);
        q->second.hist->parts.emplace(p->address(), entry.part);
        send(q->second.hist->task, p);
        send(p, next_expr, historical_atom::value);
      }
      break;
    }
  }
}

void index::flush() {
  for (auto& p : partitions_)
    if (p.second.events > 0) {
      using vast::save;
      auto t = save(dir_ / "meta", partitions_);
      if (!t) {
        VAST_ERROR(this, "failed to save meta data:", t.error());
        quit(exit::error);
      }
    }
}

} // namespace vast
