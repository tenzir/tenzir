#include <caf/all.hpp>

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/detail/assert.hpp"
#include "vast/event.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/json.hpp"
#include "vast/load.hpp"
#include "vast/logger.hpp"
#include "vast/query_options.hpp"
#include "vast/save.hpp"

#include "vast/system/index.hpp"
#include "vast/system/partition.hpp"
#include "vast/system/task.hpp"

using namespace std::chrono;
using namespace caf;

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(steady_clock::time_point)

namespace vast {
namespace system {

namespace {

actor dispatch(stateful_actor<index_state>* self, uuid const& part,
               expression const& expr) {
  if (self->state.partitions[part].events == 0)
    return {};
  // If the partition is already scheduled, we add the expression to the set of
  // to-be-queried expressions.
  auto i = std::find_if(self->state.schedule.begin(),
                        self->state.schedule.end(),
                        [&](auto& s) { return s.part == part; });
  if (i == self->state.schedule.end()) {
    VAST_DEBUG(self, "enqueues partition", part, "with", expr);
    self->state.schedule.push_back(schedule_state{part, {expr}});
  } else {
    VAST_DEBUG(self, "adds expression to", part << ":", expr);
    i->queries.insert(expr);
  }
  // If the partition is in memory, we send it the expression directly.
  if (part == self->state.active_id)
    return self->state.active;
  if (auto p = self->state.passive.lookup(part))
    return *p;
  // If we have not fully maxed out our available passive partitions, we can
  // spawn the partition directly.
  if (self->state.passive.size() < self->state.passive.capacity()) {
    VAST_DEBUG(self, "spawns passive partition", part);
    auto p = self->spawn<monitored>(partition,
                                    self->state.dir / to_string(part), self);
    self->state.passive.insert(part, p);
    return p;
  }
  return {};
}

void consolidate(stateful_actor<index_state>* self, uuid const& part,
                 expression const& expr) {
  VAST_DEBUG(self, "consolidates", part, "for", expr);
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
    VAST_DEBUG(self, "got completed query", expr, "for partition",
                  part << ',', i->queries.size(), "remaining");
    return;
  }
  VAST_DEBUG(self, "removes partition from schedule:", part);
  self->state.schedule.erase(i);
  if (self->state.schedule.empty())
    VAST_DEBUG(self, "finished with entire schedule");
  // We never unload active partitions.
  if (part == self->state.active_id)
    return;
  // If we're not dealing with the active partition, it must exist in the
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
    if (entry.part != self->state.active_id
        && !self->state.passive.contains(entry.part)) {
      VAST_DEBUG(self, "schedules next passive partition", entry.part);
      auto p = self->spawn<monitored>(partition,
                                      self->state.dir / to_string(entry.part),
                                      self);
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

template <class Actor>
void flush(Actor* self) {
  if (!self->state.partitions.empty() && !exists(self->state.dir)) {
    auto res = mkdir(self->state.dir);
    if (!res) {
      VAST_ERROR(self, "failed to create partition directory:",
                 self->system().render(res.error()));
      self->quit(res.error());
    }
  }
  for (auto& p : self->state.partitions)
    if (p.second.events > 0) {
      auto res = save(self->state.dir / "meta", self->state.partitions);
      if (!res) {
        VAST_ERROR(self, "failed to save meta data:",
                   self->system().render(res.error()));
        self->quit(res.error());
      }
    }
}

} // namespace <anonymous>

behavior index(stateful_actor<index_state>* self, path const& dir,
               size_t max_events, size_t passive) {
  self->state.dir = dir;
  VAST_ASSERT(max_events > 0);
  VAST_ASSERT(passive > 0);
  // Setup cache for passive partitions
  self->state.passive.capacity(passive);
  self->state.passive.on_evict([=](uuid id, actor& p) {
    VAST_DEBUG(self, "evicts partition", id);
    self->send(p, shutdown_atom::value);
  });
  VAST_DEBUG(self, "caps partitions at", max_events, "events");
  VAST_DEBUG(self, "uses at most", passive, "passive partitions");
  // Load partition meta data.
  if (exists(self->state.dir / "meta")) {
    auto t = load(self->state.dir / "meta", self->state.partitions);
    if (!t) {
      VAST_ERROR(self, "failed to load meta data:",
                 self->system().render(t.error()));
      self->quit(t.error());
      return {};
    }
  }
  // Load the last active partition that has not exceeded its capacity.
  auto pred = [=](auto& p) { return p.second.events < max_events; };
  auto fillable = std::find_if(self->state.partitions.begin(),
                               self->state.partitions.end(), pred);
  if (fillable != self->state.partitions.end()) {
    VAST_DEBUG(self, "re-opens active partition with", fillable->second.events,
               "events");
    auto part_dir = self->state.dir / to_string(fillable->first);
    auto p = self->spawn<monitored>(partition, part_dir, self);
    self->state.active = p;
    self->state.active_id = fillable->first;
  }
  // Register the accountant, if available.
  auto acc = self->system().registry().get(accountant_atom::value);
  if (acc) {
    VAST_DEBUG(self, "registers accountant", acc);
    self->state.accountant = actor_cast<accountant_type>(acc);
  }
  self->set_down_handler(
    [=](down_msg const& msg) {
      if (self->state.active == msg.source)
        self->state.active = {};
      // Check whether a query went down.
      for (auto q = self->state.queries.begin();
           q != self->state.queries.end(); ++q)
        if (q->second.subscribers.erase(actor_cast<actor>(msg.source)) == 1) {
          if (q->second.subscribers.empty()) {
            VAST_DEBUG(self, "removes query subscriber", msg.source);
            if (q->second.cont) {
              VAST_DEBUG(self, "disables continuous query:", q->first);
              q->second.cont = {};
              if (self->state.active)
                self->send(self->state.active, q->first,
                           continuous_atom::value, disable_atom::value);
            }
            if (!q->second.cont && !q->second.hist) {
              VAST_DEBUG(self, "removes query:", q->first);
              self->state.queries.erase(q);
            }
          }
          return;
        }
      // Check wehther a partition went down.
      auto& passive = self->state.passive;
      for (auto i = passive.begin(); i != passive.end(); ++i) {
        if (i->second.address() == msg.source) {
          passive.erase(i->first);
          VAST_DEBUG(self, "shrinks passive partitions to",
                     passive.size() << '/' << passive.capacity());
          return;
        }
      }
    }
  );
  self->set_exit_handler(
    [=](const exit_msg& msg) {
      auto n = std::make_shared<size_t>(0);
      // Queries
      for (auto& q : self->state.queries)
        if (q.second.cont) {
          self->monitor(q.second.cont->task);
          self->send_exit(q.second.cont->task, msg.reason);
          ++*n;
        }
        else if (q.second.hist) {
          self->monitor(q.second.hist->task);
          self->send_exit(q.second.hist->task, msg.reason);
          ++*n;
        }
      // Partitions
      if (self->state.active) {
        self->send(self->state.active, shutdown_atom::value);
        ++*n;
      }
      for (auto p : self->state.passive)
        self->send(p.second, shutdown_atom::value);
      *n += self->state.passive.size();
      // Our own state.
      flush(self);
      if (*n == 0)
        self->quit(msg.reason);
      else
        self->set_down_handler(
          [=](const down_msg& down) {
            if (--*n == 0)
              self->quit(down.reason);
          }
        );
    }
  );
  return {
    [=](std::vector<event> const& events) {
      if (events.empty()) {
        VAST_WARNING(self, "got batch of empty events");
        return;
      }
      auto make_partition = [&] {
        self->state.active_id = uuid::random();
        VAST_DEBUG(self, "spawns new active partition", self->state.active_id);
        auto part_dir = self->state.dir / to_string(self->state.active_id);
        self->state.active = self->spawn<monitored>(partition, part_dir, self);
        auto* active = &self->state.partitions[self->state.active_id];
        // Register continuous queries.
        for (auto& q : self->state.queries)
          if (q.second.cont)
            self->send(self->state.active, q.first, continuous_atom::value);
        return active;
      };
      if (!self->state.active)
        make_partition();
      // Figure out which active partition to use.
      auto* active = &self->state.partitions[self->state.active_id];
      // Replace partition with a new one on overflow and move the currently
      // ative one into the cache. If the max is too small that even the first
      // batch doesn't fit, then we just accept this and have a partition with
      // a single batch.
      if (active->events > 0 && active->events + events.size() > max_events) {
        VAST_DEBUG(self, "replaces active partition ", self->state.active_id);
        self->state.passive.insert(self->state.active_id, self->state.active);
        active = make_partition();
      }
      // Now we're ready to forward the events to the active partition. But
      // before doing so, extract event meta data to speed up partition finding
      // when querying.
      vast::detail::flat_set<type> types;
      auto earliest = events.front().timestamp();
      auto latest = events.front().timestamp();
      auto find_skip_attr = [](const type& t) {
        auto i = std::find(t.attributes().begin(), t.attributes().end(),
                           attribute{"skip"});
        return i != t.attributes().end();
      };
      for (auto& e : events) {
        if (!find_skip_attr(e.type()))
          types.insert(e.type());
        if (e.timestamp() < earliest)
          earliest = e.timestamp();
        if (e.timestamp() > latest)
          latest = e.timestamp();
      }
      if (types.empty()) {
        VAST_WARNING(self, "received non-indexable events");
        return;
      }
      schema sch;
      for (auto& t : types)
        if (!sch.add(t)) {
          VAST_ERROR(self, "failed to derive valid schema from event data");
          self->quit(make_error(ec::type_clash, "schema incompatibility"));
          return;
        }
      // Update partition meta data.
      active->last_modified = timestamp::clock::now();
      auto merged = schema::merge(active->schema, sch);
      if (!merged) {
        // TODO: Instead of failing, seal the active partition, replace it with
        // a new one, and send the events there. This will ensure that a
        // partition uniquely represents an event.
        VAST_ERROR(self, "failed to merge new with existing schema");
        self->quit(make_error(ec::type_clash, "failed to merge schemata"));
        return;
      }
      active->schema = std::move(*merged);
      active->events += events.size();
      // Update meta index.
      if (earliest < active->from)
        active->from = earliest;
      if (latest > active->to)
        active->to = latest;
      // Relay events to active partition.
      VAST_DEBUG(self, "forwards", events.size(), "events ["
                 << events.front().id() << ',' << (events.back().id() + 1)
                 << ')', "to", self->state.active_id);
      auto msg = self->current_mailbox_element()->move_content_to_message();
      self->send(self->state.active, msg + make_message(std::move(sch)));
    },
    [=](expression const& expr, query_options opts, actor const& subscriber)
    -> result<actor> {
      VAST_DEBUG(self, "got query:", expr);
      if (opts == no_query_options) {
        VAST_WARNING(self, "ignores query with no options");
        return make_error(ec::syntax_error, "no query options given");
      }
      self->monitor(subscriber);
      auto& qs = self->state.queries[expr];
      qs.subscribers.insert(subscriber);
      if (has_historical_option(opts)) {
        if (!qs.hist) {
          VAST_DEBUG(self, "instantiates historical query");
          qs.hist = historical_query_state();
        }
        if (!qs.hist->task) {
          VAST_DEBUG(self, "enables historical query");
          qs.hist->task = self->spawn(
            task<steady_clock::time_point, expression, historical_atom>,
            steady_clock::now(), expr, historical_atom::value);
          self->send(qs.hist->task, supervisor_atom::value, self);
          // Test whether this query matches any partition and relay it where
          // possible.
          // TODO: this is technically the task of the meta index.
          for (auto& p : self->state.partitions)
            if (visit(time_restrictor{p.second.from, p.second.to}, expr))
              if (auto a = dispatch(self, p.first, expr)) {
                qs.hist->parts.emplace(a->address(), p.first);
                self->send(qs.hist->task, a);
                self->send(a, expr, historical_atom::value);
              }
          if (qs.hist->parts.empty()) {
            VAST_DEBUG(self, "did not find a partition for query");
            self->send_exit(qs.hist->task, exit_reason::user_shutdown);
            qs.hist->task = {};
          }
        }
        if (!qs.hist->hits.empty() && !all<0>(qs.hist->hits)) {
          VAST_DEBUG(self, "relays", rank(qs.hist->hits), "cached hits");
          self->send(subscriber, qs.hist->hits);
        }
        return qs.hist->task;
      }
      return make_error(ec::unspecified, "continuous queries not implemented");
      // TODO
      //if (has_continuous_option(opts)) {
      //  if (!qs.cont) {
      //    VAST_DEBUG(self, "instantiates continuous query");
      //    qs.cont = continuous_query_state();
      //  }
      //  if (!qs.cont->task) {
      //    VAST_DEBUG(self, "enables continuous query");
      //    qs.cont->task =
      //      self->spawn(task<steady_clock::time_point>, steady_clock::now());
      //    self->send(qs.cont->task, self);
      //    // Relay the continuous query to all active partitions, as these may
      //    // still receive events.
      //    if (self->state.active)
      //      self->send(self->state.active, expr, continuous_atom::value);
      //  }
      //  self->send(subscriber, qs.cont->task);
      //  if (!qs.cont->hits.empty() && !all<0>(qs.cont->hits))
      //    self->send(subscriber, qs.cont->hits);
      //}
    },
    [=](expression const& expr, continuous_atom, disable_atom) {
      VAST_DEBUG(self, "got request to disable continuous query:", expr);
      auto q = self->state.queries.find(expr);
      if (q == self->state.queries.end()) {
        VAST_WARNING(self, "has not such query:", expr);
      } else if (!q->second.cont) {
        VAST_WARNING(self, "has already disabled query:", expr);
      } else {
        VAST_DEBUG(self, "disables continuous query:", expr);
        self->send(q->second.cont->task, done_atom::value);
        q->second.cont->task = {};
      }
    },
    [=](done_atom, steady_clock::time_point start, expression const& expr) {
      auto runtime = steady_clock::now() - start;
      VAST_DEBUG(self, "got signal that partition", self->current_sender(),
                 "took", runtime, "to complete query", expr);
      auto q = self->state.queries.find(expr);
      VAST_ASSERT(q != self->state.queries.end());
      VAST_ASSERT(q->second.hist);
      auto sender_addr = actor_cast<actor_addr>(self->current_sender());
      auto p = q->second.hist->parts.find(sender_addr);
      VAST_ASSERT(p != q->second.hist->parts.end());
      consolidate(self, p->second, expr);
      self->send(q->second.hist->task, done_atom::value, p->first);
      q->second.hist->parts.erase(p);
    },
    [=](done_atom, steady_clock::time_point start, expression const& expr,
        historical_atom) {
      auto now = steady_clock::now();
      auto runtime = now - start;
      VAST_DEBUG(self, "completed lookup", expr, "in", runtime);
      auto q = self->state.queries.find(expr);
      VAST_ASSERT(q != self->state.queries.end());
      VAST_ASSERT(q->second.hist);
      VAST_ASSERT(q->second.hist->parts.empty());
      // Notify subscribers about completion.
      for (auto& s : q->second.subscribers)
        self->send(s, done_atom::value, timespan{runtime}, expr);
      // Remove query state.
      // TODO: consider caching it for a while and also record its coverage
      // so that future queries don't need to start over again.
      q->second.hist->task = {};
      self->state.queries.erase(q);
    },
    [=](expression const& expr, bitmap& hits, historical_atom) {
      VAST_DEBUG(self, "received", rank(hits), "historical hits from",
                 self->current_sender(), "for query:", expr);
      auto& qs = self->state.queries[expr];
      VAST_ASSERT(qs.hist);
      auto delta = hits - qs.hist->hits;
      if (rank(delta) > 0) {
        qs.hist->hits |= delta;
        auto msg = make_message(std::move(delta));
        for (auto& s : qs.subscribers)
          self->send(s, msg);
      }
    },
    [=](expression const& expr, bitmap& hits, continuous_atom) {
      VAST_DEBUG(self, "received", rank(hits), "continuous hits from",
                 self->current_sender(), "for query:", expr);
      auto& qs = self->state.queries[expr];
      VAST_ASSERT(qs.cont);
      qs.cont->hits |= hits;
      auto msg = make_message(std::move(hits));
      for (auto& s : qs.subscribers)
        self->send(s, msg);
    },
    [=](flush_atom) {
      auto t = self->spawn(task<>);
      self->send(t, self);
      if (self->state.active) {
        VAST_DEBUG(self, "flushes active partition", self->state.active_id);
        self->send(self->state.active, flush_atom::value, t);
      } else {
        VAST_DEBUG(self, "ignores request to flush, no active partition");
      }
      flush(self);
      self->send(t, done_atom::value);
      return t;
    },
    //[=](schema_atom) {
    //  std::map<std::string, json::array> history;
    //  // Sort partition meta data in chronological order.
    //  std::vector<index_partition_state const*> parts;
    //  parts.reserve(self->state.partitions.size());
    //  for (auto& pair : self->state.partitions)
    //    parts.push_back(&pair.second);
    //  std::sort(parts.begin(), parts.end(), [](auto& x, auto& y) {
    //    return x->last_modified < y->last_modified;
    //  });
    //  // Go through each type and accumulate meta data. Upon finding a
    //  // type clash, we make a snapshot of the current meta data, and start
    //  // over. The result is a list of types, each of which expressed as a
    //  // sequence of versions to track the evolution history.
    //  struct type_state {
    //    vast::type type;
    //    timestamp last_modified = time::duration::zero();
    //    timestamp from = time::duration::zero();
    //    timestamp to = time::duration::zero();
    //  };
    //  std::unordered_map<std::string, type_state> type_states;
    //  auto snapshot = [&](type_state const& ts) {
    //    json::object o{
    //      {"latest", ts.from.time_since_epoch().count()},
    //      {"earliest", ts.to.time_since_epoch().count()},
    //      {"last_modified", ts.last_modified.time_since_epoch().count()},
    //      {"type", to_json(ts.type)}
    //    };
    //    history[ts.type.name()].push_back(std::move(o));
    //  };
    //  for (auto& part : parts) {
    //    for (auto& t : part->schema) {
    //      auto& s = type_states[t.name()];
    //      if (s.type == t) { // Accumulate
    //        s.last_modified = part->last_modified;
    //        if (part->from < s.from)
    //          s.from = part->from;
    //        if (part->to > s.to)
    //          s.to = part->to;
    //      } else {
    //        if (!is<none>(s.type)) // Type clash
    //          snapshot(s);
    //        s.type = t;
    //        s.last_modified = part->last_modified;
    //        s.from = part->from;
    //        s.to = part->to;
    //      }
    //    }
    //  }
    //  for (auto& pair : type_states)
    //    snapshot(pair.second);
    //  // Return result as JSON object.
    //  json::object result;
    //  for (auto& pair : history)
    //    result[pair.first] = std::move(pair.second);
    //  return json{std::move(result)};
    //},
  };
}

} // namespace system
} // namespace vast
