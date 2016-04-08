#include <caf/all.hpp>

#include "vast/bitmap_index.hpp"
#include "vast/event.hpp"
#include "vast/json.hpp"
#include "vast/query_options.hpp"
#include "vast/actor/index.hpp"
#include "vast/actor/partition.hpp"
#include "vast/actor/task.hpp"
#include "vast/expr/restrictor.hpp"
#include "vast/concept/convertible/vast/type.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/concept/serializable/io.hpp"
#include "vast/concept/serializable/state.hpp"
#include "vast/concept/serializable/std/array.hpp"
#include "vast/concept/serializable/std/chrono.hpp"
#include "vast/concept/serializable/std/unordered_map.hpp"
#include "vast/concept/serializable/vast/schema.hpp"
#include "vast/concept/state/uuid.hpp"
#include "vast/concept/state/time.hpp"
#include "vast/util/assert.hpp"

namespace vast {

template <typename Serializer>
void serialize(Serializer& sink, index::partition_state const& ps) {
  sink << ps.last_modified << ps.schema << ps.events << ps.from << ps.to;
}

template <typename Deserializer>
void deserialize(Deserializer& source, index::partition_state& ps) {
  source >> ps.last_modified >> ps.schema >> ps.events >> ps.from >> ps.to;
}

namespace {

maybe<actor> dispatch(stateful_actor<index::state>* self,
                         uuid const& part, expression const& expr) {
  if (self->state.partitions[part].events == 0)
    return {};
  // If the partition is already scheduled, we add the expression to the set of
  // to-be-queried expressions.
  auto i = std::find_if(self->state.schedule.begin(),
                        self->state.schedule.end(),
                        [&](auto& s) { return s.part == part; });
  if (i == self->state.schedule.end()) {
    VAST_DEBUG_AT(self, "enqueues partition", part, "with", expr);
    self->state.schedule.push_back(index::schedule_state{part, {expr}});
  } else {
    VAST_DEBUG_AT(self, "adds expression to", part << ":", expr);
    i->queries.insert(expr);
  }
  // If the partition is in memory, we send it the expression directly.
  for (auto& a : self->state.active)
    if (a.first == part)
      return a.second;
  if (auto p = self->state.passive.lookup(part))
    return *p;
  // If we have not fully maxed out our available passive partitions, we can
  // self->spawn the partition directly.
  if (self->state.passive.size() < self->state.passive.capacity()) {
    VAST_DEBUG_AT(self, "spawns passive partition", part);
    auto p = self->spawn<monitored>(partition::make,
                                    self->state.dir / to_string(part), self);
    if (self->state.accountant)
      self->send(p, self->state.accountant);
    self->state.passive.insert(part, p);
    return p;
  }
  return {};
}

void consolidate(stateful_actor<index::state>*self,
                 uuid const& part, expression const& expr) {
  VAST_DEBUG_AT(self, "consolidates", part, "for", expr);
  auto i = std::find_if(self->state.schedule.begin(),
                        self->state.schedule.end(),
                        [&](auto& s) { return s.part == part; });
  // Remove the completed query expression from the schedule.
  VAST_ASSERT(i != self->state.schedule.end());
  VAST_ASSERT(!i->queries.empty());
  auto x = i->queries.find(expr);
  VAST_ASSERT(x != i->queries.end());
  i->queries.erase(x);
  // We keep the partition in the schedule as long it has outstanding queries.
  if (!i->queries.empty()) {
    VAST_DEBUG_AT(self, "got completed query", expr, "for partition",
                  part << ',', i->queries.size(), "remaining");
    return;
  }
  VAST_DEBUG_AT(self, "removes partition from schedule:", part);
  self->state.schedule.erase(i);
  if (self->state.schedule.empty())
    VAST_DEBUG_AT(self, "finished with entire schedule");
  // We never unload active partitions.
  auto j = std::find_if(self->state.active.begin(),
                        self->state.active.end(),
                        [&](auto& a) { return a.first == part; });
  if (j != self->state.active.end())
    return;
  // If we're not dealing with an active partition, it must exist in the
  // passive list, unless we dispatched an expression to an active partition
  // and that got replaced with a new one. In the latter case the replaced
  // partition is neither in the active nor passive set and has already being
  // taken care of, so we can safely ignore this consolidation request.
  if (self->state.passive.lookup(part) == nullptr)
    return;
  // For each consolidated passive partition, we load another new one. Because
  // partitions can complete in any order, we have to walk through the schedule
  // from the beginning again to find the next passive partition to load.
  for (auto& entry : self->state.schedule) {
    auto a = std::find_if(self->state.active.begin(),
                          self->state.active.end(),
                          [&](auto& a) { return a.first == entry.part; });
    if (a == self->state.active.end()
        && !self->state.passive.contains(entry.part)) {
      VAST_DEBUG_AT(self, "schedules next passive partition", entry.part);
      auto p = self->spawn<monitored>(
        partition::make, self->state.dir / to_string(entry.part), self);
      if (self->state.accountant)
        self->send(p, self->state.accountant);
      self->state.passive.insert(entry.part, p); // automatically evicts 'part'.
      for (auto& next_expr : entry.queries) {
        auto q = self->state.queries.find(next_expr);
        VAST_ASSERT(q != self->state.queries.end());
        VAST_ASSERT(q->second.hist);
        q->second.hist->parts.emplace(p->address(), entry.part);
        self->send(q->second.hist->task, p);
        self->send(p, next_expr, historical_atom::value);
      }
      break;
    }
  }
}

void flush(stateful_actor<index::state>* self) {
  for (auto& p : self->state.partitions)
    if (p.second.events > 0) {
      auto t = save(self->state.dir / "meta", self->state.partitions);
      if (!t) {
        VAST_ERROR_AT(self, "failed to save meta data:", t.error());
        self->quit(exit::error);
      }
    }
}

} // namespace <anonymous>

index::state::state(local_actor* self) : basic_state{self, "index"} { }

behavior index::make(stateful_actor<state>*self, path const& dir,
                     size_t max_events, size_t passive_parts,
                     size_t active_parts) {
  self->state.dir = dir;
  self->trap_exit(true);
  VAST_ASSERT(max_events > 0);
  VAST_ASSERT(active_parts > 0);
  VAST_ASSERT(passive_parts > 0);
  self->state.active.resize(active_parts);
  self->state.passive.capacity(passive_parts);
  self->state.passive.on_evict([=](uuid id, actor& p) {
    VAST_DEBUG_AT(self, "evicts partition", id);
    self->send_exit(p, exit::stop);
  });
  VAST_VERBOSE_AT(self, "caps partitions at", max_events, "events");
  VAST_VERBOSE_AT(self, "uses at most", passive_parts, "passive partitions");
  VAST_VERBOSE_AT(self, "uses", active_parts, "active partitions");
  // Load partition meta data.
  if (exists(self->state.dir / "meta")) {
    auto t = load(self->state.dir / "meta", self->state.partitions);
    if (!t) {
      VAST_ERROR_AT(self, "failed to load meta data:", t.error());
      self->quit(exit::error);
      return {};
    }
  }
  // Load the k last modified partitions that have not exceeded their capacity.
  std::vector<std::pair<uuid, partition_state>> parts;
  for (auto& p : self->state.partitions)
    if (p.second.events < max_events)
      parts.push_back(p);
  std::sort(parts.begin(), parts.end(), [](auto& x, auto& y) {
    return y.second.last_modified < x.second.last_modified;
  });
  for (size_t i = 0; i < self->state.active.size(); ++i) {
    auto id = i < parts.size() ? parts[i].first : uuid::random();
    VAST_VERBOSE_AT(self, "spawns", (i < parts.size() ? "existing" : "new"),
                    "active partition", id);
    auto p = self->spawn<monitored>(partition::make,
                                    self->state.dir / to_string(id), self);
    self->state.active[i] = {id, p};
    self->state.partitions[id].last_modified = time::now();
  }
  return {
    [=](exit_msg const& msg) {
      if (msg.reason == exit::kill) {
        self->quit(exit::kill);
        return;
      }
      if (self->current_mailbox_element()->mid.is_high_priority()) {
        VAST_DEBUG_AT(self, "delays EXIT from", msg.source);
        self->send(message_priority::normal, self, self->current_message());
        return;
      }
      flush(self);
      self->trap_exit(false); // Once the task completes we go down with it.
      auto t = self->spawn<linked>(task::make<>);
      self->send(t, msg.reason);
      for (auto& q : self->state.queries)
        if (q.second.cont)
          self->link_to(q.second.cont->task);
        else if (q.second.hist)
          self->link_to(q.second.hist->task);
      for (auto& a : self->state.active)
        self->send(t, a.second);
      for (auto p : self->state.passive)
        self->send(t, p.second);
      for (auto& a : self->state.active)
        self->send_exit(a.second, msg.reason);
      for (auto p : self->state.passive)
        self->send_exit(p.second, msg.reason);
    },
    [=](down_msg const& msg) {
      for (auto q = self->state.queries.begin();
           q != self->state.queries.end(); ++q)
        if (q->second.subscribers.erase(actor_cast<actor>(msg.source)) == 1) {
          if (q->second.subscribers.empty()) {
            VAST_VERBOSE_AT(self, "removes query subscriber", msg.source);
            if (q->second.cont) {
              VAST_VERBOSE_AT(self, "disables continuous query:", q->first);
              q->second.cont = nil;
              for (auto& a : self->state.active)
                self->send(a.second, q->first, continuous_atom::value,
                     disable_atom::value);
            }
            if (!q->second.cont && !q->second.hist) {
              VAST_VERBOSE_AT(self, "removes query:", q->first);
              self->state.queries.erase(q);
            }
          }
          return;
        }
      for (auto i = self->state.active.begin();
           i != self->state.active.end(); ++i) {
        if (i->second.address() == msg.source) {
          VAST_DEBUG_AT(self, "removes active partitions", i->first);
          self->state.active.erase(i);
          return;
        }
      }
      for (auto i = self->state.passive.begin();
           i != self->state.passive.end(); ++i) {
        if (i->second.address() == msg.source) {
          self->state.passive.erase(i->first);
          VAST_DEBUG_AT(self, "shrinks passive partitions to",
                        self->state.passive.size()
                          << '/' << self->state.passive.capacity());
          return;
        }
      }
    },
    [=](accountant::type const& acc) {
      VAST_DEBUG_AT(self, "registers accountant#", acc->id());
      self->state.accountant = acc;
      for (auto& pair : self->state.active)
        self->send(pair.second, acc);
    },
    [=](flush_atom) {
      VAST_VERBOSE_AT(self, "flushes", self->state.active.size(),
                      "active partitions");
      auto t = self->spawn(task::make<>);
      self->send(t, self);
      for (auto& a : self->state.active)
        self->send(a.second, flush_atom::value, t);
      flush(self);
      self->send(t, done_atom::value);
      return t;
    },
    [=](schema_atom) {
      std::map<std::string, json::array> history;
      // Sort partition meta data in chronological order.
      std::vector<partition_state const*> parts;
      parts.reserve(self->state.partitions.size());
      for (auto& pair : self->state.partitions)
        parts.push_back(&pair.second);
      std::sort(parts.begin(), parts.end(), [](auto& x, auto& y) {
        return x->last_modified < y->last_modified;
      });
      // Go through each type and accumulate meta data. Upon finding a
      // type clash, we make a snapshot of the current meta data, and start
      // over. The result is a list of types, each of which expressed as a
      // sequence of versions to track the evolution history.
      struct type_state {
        vast::type type;
        time::point last_modified = time::duration::zero();
        time::point from = time::duration::zero();
        time::point to = time::duration::zero();
      };
      std::unordered_map<std::string, type_state> type_states;
      auto snapshot = [&](type_state const& ts) {
        json::object o{
          {"oldest", ts.from.time_since_epoch().count()},
          {"youngest", ts.to.time_since_epoch().count()},
          {"last_modified", ts.last_modified.time_since_epoch().count()},
          {"type", to_json(ts.type)}
        };
        history[ts.type.name()].push_back(std::move(o));
      };
      for (auto& part : parts) {
        for (auto& t : part->schema) {
          auto& s = type_states[t.name()];
          if (s.type == t) { // Accumulate
            s.last_modified = part->last_modified;
            if (part->from < s.from)
              s.from = part->from;
            if (part->to > s.to)
              s.to = part->to;
          } else {
            if (!is<none>(s.type)) // Type clash
              snapshot(s);
            s.type = t;
            s.last_modified = part->last_modified;
            s.from = part->from;
            s.to = part->to;
          }
        }
      }
      for (auto& pair : type_states)
        snapshot(pair.second);
      // Return result as JSON object.
      json::object result;
      for (auto& pair : history)
        result[pair.first] = std::move(pair.second);
      return json{std::move(result)};
    },
    [=](std::vector<event> const& events) {
      if (events.empty()) {
        VAST_WARN(self, "got batch of empty events");
        return;
      }
      // Figure out which active partition to use.
      auto idx = self->state.next_active++ % self->state.active.size();
      auto& a = self->state.active[idx];
      VAST_ASSERT(a.second != invalid_actor);
      auto part = &self->state.partitions[a.first];
      // Replace partition with a new one on overflow. If the max is too small
      // that even the first batch doesn't fit, then we just accept this and
      // have a partition with a single batch.
      if (part->events > 0 && part->events + events.size() > max_events) {
        VAST_VERBOSE_AT(self, "replaces partition (" << a.first << ')');
        self->send_exit(a.second, exit::stop);
        // Create a new partition.
        a.first = uuid::random();
        auto part_dir = self->state.dir / to_string(a.first);
        a.second = self->spawn<monitored>(partition::make, part_dir, self);
        if (self->state.accountant)
          self->send(a.second, self->state.accountant);
        auto i = self->state.partitions.emplace(a.first, partition_state());
        part = &i.first->second;
        // Register continuous queries.
        for (auto& q : self->state.queries)
          if (q.second.cont)
            self->send(a.second, q.first, continuous_atom::value);
      }
      // Extract schema.
      util::flat_set<type> types;
      auto youngest = events.front().timestamp();
      auto oldest = events.front().timestamp();
      for (auto& e : events) {
        if (!e.type().find_attribute(type::attribute::skip))
          types.insert(e.type());
        if (e.timestamp() < youngest)
          youngest = e.timestamp();
        if (e.timestamp() > oldest)
          oldest = e.timestamp();
      }
      if (types.empty()) {
        VAST_WARN_AT(self, "received non-indexable events");
        return;
      }
      schema sch;
      for (auto& t : types)
        if (! sch.add(t)) {
          VAST_ERROR_AT(self, "failed to derive valid schema from event data");
          self->quit(exit::error);
          return;
        }
      // Update partition meta data.
      part->last_modified = time::now();
      if (! part->schema.add(sch)) {
        // TODO: Instead of failing, seal the active partition, replace it with
        // a new one, and send the events there. This will ensure that a
        // parition uniquely represents an event.
        VAST_ERROR_AT(self, "failed to merge schemata");
        self->quit(exit::error);
        return;
      }
      part->events += events.size();
      if (part->from == time::duration{} || youngest < part->from)
        part->from = youngest;
      if (part->to == time::duration{} || oldest > part->to)
        part->to = oldest;
      // Relay events.
      VAST_DEBUG_AT(self, "forwards", events.size(), "events [" <<
                    events.front().id() << ',' << (events.back().id() + 1)
                      << ')', "to", a.second, '(' << a.first << ')');
      auto t = self->spawn(task::make<time::moment, uint64_t>,
                           time::snapshot(), events.size());
      self->send(a.second, self->current_message()
                             + make_message(std::move(sch))
                             + make_message(std::move(t)));
    },
    [=](expression const& expr, query_options opts, actor const& subscriber) {
      VAST_VERBOSE_AT(self, "got query:", expr);
      if (opts == no_query_options) {
        VAST_WARN_AT(self, "ignores query with no options:", expr);
        return;
      }
      self->monitor(subscriber);
      auto& qs = self->state.queries[expr];
      qs.subscribers.insert(subscriber);
      if (has_historical_option(opts)) {
        if (!qs.hist) {
          VAST_DEBUG_AT(self, "instantiates historical query");
          qs.hist = historical_query_state();
        }
        if (!qs.hist->task) {
          VAST_VERBOSE_AT(self, "enables historical query");
          qs.hist->task
            = self->spawn(task::make<time::moment, expression, historical_atom>,
                    time::snapshot(), expr, historical_atom::value);
          self->send(qs.hist->task, supervisor_atom::value, self);
          // Test whether this query matches any partition and relay it where
          // possible.
          for (auto& p : self->state.partitions)
            if (visit(expr::time_restrictor{p.second.from, p.second.to}, expr))
              if (auto a = dispatch(self, p.first, expr)) {
                qs.hist->parts.emplace(a->address(), p.first);
                self->send(qs.hist->task, *a);
                self->send(*a, expr, historical_atom::value);
              }
          if (qs.hist->parts.empty()) {
            VAST_DEBUG_AT(self, "did not find a partition for query");
            self->send_exit(qs.hist->task, exit::done);
            qs.hist->task = invalid_actor;
          }
        }
        self->send(subscriber, qs.hist->task);
        if (!qs.hist->hits.empty() && !qs.hist->hits.all_zeros()) {
          VAST_VERBOSE_AT(self, "relays", qs.hist->hits.count(), "cached hits");
          self->send(subscriber, qs.hist->hits);
        }
      }
      if (has_continuous_option(opts)) {
        if (!qs.cont) {
          VAST_DEBUG_AT(self, "instantiates continuous query");
          qs.cont = continuous_query_state();
        }
        if (!qs.cont->task) {
          VAST_VERBOSE_AT(self, "enables continuous query");
          qs.cont->task =
            self->spawn(task::make<time::moment>, time::snapshot());
          self->send(qs.cont->task, self);
          // Relay the continuous query to all active partitions, as these may
          // still receive events.
          for (auto& a : self->state.active)
            self->send(a.second, expr, continuous_atom::value);
        }
        self->send(subscriber, qs.cont->task);
        if (!qs.cont->hits.empty() && !qs.cont->hits.all_zeros())
          self->send(subscriber, qs.cont->hits);
      }
    },
    [=](expression const& expr, continuous_atom, disable_atom) {
      VAST_VERBOSE_AT(self, "got request to disable continuous query:", expr);
      auto q = self->state.queries.find(expr);
      if (q == self->state.queries.end()) {
        VAST_WARN_AT(self, "has not such query:", expr);
      } else if (!q->second.cont) {
        VAST_WARN_AT(self, "has already disabled query:", expr);
      } else {
        VAST_VERBOSE_AT(self, "disables continuous query:", expr);
        self->send(q->second.cont->task, done_atom::value);
        q->second.cont->task = invalid_actor;
      }
    },
    [=](done_atom, time::moment start, expression const& expr) {
      auto runtime = time::snapshot() - start;
      VAST_DEBUG_AT(self, "got signal that", self->current_sender(), "took",
                    runtime, "to complete query: ", expr);
      auto q = self->state.queries.find(expr);
      VAST_ASSERT(q != self->state.queries.end());
      VAST_ASSERT(q->second.hist);
      auto p = q->second.hist->parts.find(self->current_sender());
      VAST_ASSERT(p != q->second.hist->parts.end());
      consolidate(self, p->second, expr);
      self->send(q->second.hist->task, done_atom::value, p->first);
      q->second.hist->parts.erase(p);
    },
    [=](done_atom, time::moment start, expression const& expr,
        historical_atom) {
      auto now = time::snapshot();
      auto runtime = now - start;
      VAST_VERBOSE_AT(self, "completed lookup", expr, "in", runtime);
      auto q = self->state.queries.find(expr);
      VAST_ASSERT(q != self->state.queries.end());
      VAST_ASSERT(q->second.hist);
      VAST_ASSERT(q->second.hist->parts.empty());
      // Notify subscribers about completion.
      for (auto& s : q->second.subscribers)
        self->send(s, done_atom::value, now, runtime, expr);
      // Remove query state.
      // TODO: consider caching it for a while and also record its coverage
      // so that future queries don't need to start over again.
      q->second.hist->task = invalid_actor;
      self->state.queries.erase(q);
    },
    [=](expression const& expr, bitstream_type& hits, historical_atom) {
      VAST_DEBUG_AT(self, "received", hits.count(), "historical hits from",
                 self->current_sender(), "for query:", expr);
      auto& qs = self->state.queries[expr];
      VAST_ASSERT(qs.hist);
      auto delta = hits - qs.hist->hits;
      if (delta.count() > 0) {
        qs.hist->hits |= delta;
        auto msg = make_message(std::move(delta));
        for (auto& s : qs.subscribers)
          self->send(s, msg);
      }
    },
    [=](expression const& expr, bitstream_type& hits, continuous_atom) {
      VAST_DEBUG_AT(self, "received", hits.count(), "continuous hits from",
                 self->current_sender(), "for query:", expr);
      auto& qs = self->state.queries[expr];
      VAST_ASSERT(qs.cont);
      qs.cont->hits |= hits;
      auto msg = make_message(std::move(hits));
      for (auto& s : qs.subscribers)
        self->send(s, msg);
    },
    log_others(self)
  };
}

} // namespace vast
