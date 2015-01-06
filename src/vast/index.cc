#include "vast/index.h"

#include <caf/all.hpp>
#include "vast/bitmap_index.h"
#include "vast/chunk.h"
#include "vast/partition.h"
#include "vast/print.h"
#include "vast/task_tree.h"
#include "vast/io/serialization.h"

using namespace caf;

namespace vast {

namespace {

std::vector<uuid> intersect(std::vector<uuid> const& x,
                            std::vector<uuid> const& y)
{
  std::vector<uuid> r;
  std::set_intersection(
      x.begin(), x.end(),
      y.begin(), y.end(),
      std::back_inserter(r));

  return r;
}

std::vector<uuid> unify(std::vector<uuid> const& x,
                        std::vector<uuid> const& y)
{
  std::vector<uuid> r;
  std::set_union(
      x.begin(), x.end(),
      y.begin(), y.end(),
      std::back_inserter(r));

  return r;
}

} // namespace <anonymous>

// Retrieves all predicates in an expression.
struct index::predicatizer
{
  std::vector<predicate> operator()(none)
  {
    return {};
  }

  std::vector<predicate> operator()(conjunction const& con)
  {
    std::vector<predicate> preds;
    for (auto& op : con)
    {
      auto ps = visit(*this, op);
      std::move(ps.begin(), ps.end(), std::back_inserter(preds));
    }

    return preds;
  }

  std::vector<predicate> operator()(disjunction const& dis)
  {
    std::vector<predicate> preds;
    for (auto& op : dis)
    {
      auto ps = visit(*this, op);
      std::move(ps.begin(), ps.end(), std::back_inserter(preds));
    }

    return preds;
  }

  std::vector<predicate> operator()(negation const& n)
  {
    return visit(*this, n[0]);
  }

  std::vector<predicate> operator()(predicate const& pred)
  {
    return {pred};
  }
};

// Builds the set of restrictions bottom-up.
struct index::builder
{
  builder(std::unordered_map<uuid, partition_state> const& partitions,
          std::map<expression, std::vector<uuid>>& restrictions)
    : partitions_{partitions},
      restrictions_{restrictions}
  {
  }

  void operator()(none)
  {
    assert(! "should never happen");
  }

  void operator()(conjunction const& con)
  {
    for (auto& op : con)
      visit(*this, op);

    auto& r = restrictions_[con];
    r = restrictions_[con[0]];
    for (size_t i = 1; i < con.size(); ++i)
      r = intersect(r, restrictions_[con[i]]);

    VAST_DEBUG("restricted", con, "to",
               r.size() << '/' << partitions_.size(), "partitions");
  }

  void operator()(disjunction const& dis)
  {
    for (auto& op : dis)
      visit(*this, op);

    auto& r = restrictions_[dis];
    for (auto& op : dis)
      r = unify(r, restrictions_[op]);

    VAST_DEBUG("restricted", dis, "to",
               r.size() << '/' << partitions_.size(), "partitions");
  }

  void operator()(negation const& n)
  {
    visit(*this, n[0]);
  }

  void operator()(predicate const& pred)
  {
    std::vector<uuid> parts;
    if (is<time_extractor>(pred.lhs))
    {
      auto d = get<data>(pred.rhs);
      assert(d && is<time_point>(*d));

      for (auto& p : partitions_)
        if (data::evaluate(p.second.first_event, pred.op, *d)
            || data::evaluate(p.second.last_event, pred.op, *d))
          parts.push_back(p.first);
    }
    else
    {
      parts.reserve(partitions_.size());
      for (auto& p : partitions_)
        parts.push_back(p.first);
    }

    VAST_DEBUG("restricted", pred, "to",
               parts.size() << '/' << partitions_.size(), "partitions");

    std::sort(parts.begin(), parts.end());
    restrictions_[pred] = std::move(parts);
  }

  std::unordered_map<uuid, partition_state> const& partitions_;
  std::map<expression, std::vector<uuid>>& restrictions_;
};

// Propagates the accumulated restrictions top-down.
struct index::pusher
{
  pusher(std::map<expression, std::vector<uuid>>& restrictions)
    : restrictions_{restrictions}
  {
  }

  void operator()(none)
  {
    assert(! "should never happen");
  }

  void operator()(conjunction const& con)
  {
    auto& r = restrictions_[con];
    for (auto& op : con)
      restrictions_[op] = intersect(r, restrictions_[op]);

    for (auto& op : con)
    {
      VAST_DEBUG("pushing", r.size(), "restrictions:", con, " --> ", op);
      visit(*this, op);
    }
  }

  void operator()(disjunction const& dis)
  {
    auto& r = restrictions_[dis];
    for (auto& op : dis)
      restrictions_[op] = intersect(r, restrictions_[op]);

    for (auto& op : dis)
    {
      VAST_DEBUG("pushing", r.size(), "restrictions:", dis, " --> ", op);
      visit(*this, op);
    }
  }

  void operator()(negation const& n)
  {
    visit(*this, n[0]);
  }

  void operator()(predicate const&)
  {
    // Done with this path.
  }

  std::map<expression, std::vector<uuid>>& restrictions_;
};

// Dispatches the predicates of an expression to the corresponding partitions.
struct index::dispatcher
{
  dispatcher(index& idx)
    : index_{idx}
  {
  }

  void operator()(none) { }

  void operator()(conjunction const& con)
  {
    if (is<none>(root_))
      root_ = con;

    for (auto& op : con)
      visit(*this, op);
  }

  void operator()(disjunction const& dis)
  {
    if (is<none>(root_))
      root_ = dis;

    for (auto& op : dis)
      visit(*this, op);
  }

  void operator()(negation const& n)
  {
    if (is<none>(root_))
      root_ = n;

    visit(*this, n[0]);
  }

  void operator()(predicate const& pred)
  {
    if (is<none>(root_))
      root_ = pred;

    for (auto& part : index_.queries_[root_].predicates[pred].restrictions)
    {
      auto& status = index_.partitions_[part].status;
      if (status.find(pred) == status.end())
      {
        index_.dispatch(part, pred);
        status[pred];
      }
    }
  }

  index& index_;
  expression root_;
};

// Evaluates an expression by taking existing hits from the predicate cache.
struct index::evaluator
{
public:
  evaluator(index& idx)
    : index_{idx}
  {
  }

  bitstream operator()(none)
  {
    assert(! "should never happen");
    return {};
  }

  bitstream operator()(conjunction const& con)
  {
    if (is<none>(root_))
      root_ = con;

    auto hits = visit(*this, con[0]);
    if (! hits)
      return {};

    for (size_t i = 1; i < con.size(); ++i)
    {
      if (hits &= visit(*this, con[i]))
        continue;
      else
        return {};  // short-circuit evaluation
    }

    auto& state = index_.queries_[root_].predicates[con];
    state.hits = std::move(hits);
    return state.hits;
  }

  bitstream operator()(disjunction const& dis)
  {
    if (is<none>(root_))
      root_ = dis;

    bitstream hits;
    for (auto& op : dis)
      hits |= visit(*this, op);

    auto& state = index_.queries_[root_].predicates[dis];
    state.hits = std::move(hits);
    return state.hits;
  }

  bitstream operator()(negation const& n)
  {
    if (is<none>(root_))
      root_ = n;

    auto hits = visit(*this, n[0]);
    hits.flip();

    auto& state = index_.queries_[root_].predicates[n];
    state.hits = std::move(hits);
    return state.hits;
  }

  bitstream operator()(predicate const& pred)
  {
    if (is<none>(root_))
      root_ = pred;

    auto& state = index_.queries_[root_].predicates[pred];
    bitstream hits;
    for (auto& part : state.restrictions)
    {
      auto& status = index_.partitions_[part].status;
      auto i = status.find(pred);
      if (i != status.end())
        hits |= i->second.hits;
    }

    state.hits |= hits;
    return state.hits;
  }

  index& index_;
  expression root_;
};

struct index::propagator
{
public:
  propagator(index& idx, expression const& pred)
    : index_{idx},
      pred_{pred}
  {
  }

  bool operator()(none)
  {
    assert(! "should never happen");
  }

  bool operator()(conjunction const& con)
  {
    if (is<none>(root_))
      root_ = con;

    for (auto& op : con)
      if (visit(*this, op))
      {
        auto& preds = index_.queries_[root_].predicates;
        auto& now = preds[con].hits;
        auto prev = now;

        now = preds[con[0]].hits;
        for (size_t i = 0; i < con.size(); ++i)
          if (now &= preds[con[i]].hits)
            continue;
          else
            return false; // short-circuit evaluation

        return now && (! prev || now != prev);
      }

    return false;
  }

  bool operator()(disjunction const& dis)
  {
    if (is<none>(root_))
      root_ = dis;

    for (auto& op : dis)
      if (visit(*this, op))
      {
        auto& preds = index_.queries_[root_].predicates;
        auto& now = preds[dis].hits;
        auto prev = now;
        now |= preds[op].hits;
        return now && (! prev || now != prev);
      }

    return false;
  }

  bool operator()(negation const& n)
  {
    if (is<none>(root_))
      root_ = n;

    if (! visit(*this, n[0]))
      return false;

    auto& now = index_.queries_[root_].predicates[n].hits;
    auto prev = now;
    now.flip();

    return now && (! prev || now != prev);
  }

  bool operator()(predicate const& pred)
  {
    if (is<none>(root_))
      root_ = pred;

    return !! index_.queries_[root_].predicates[pred].hits;
  }

  index& index_;
  expression const& pred_;
  expression root_;
};

void index::partition_state::serialize(serializer& sink) const
{
  sink << events << first_event << last_event << last_modified;
}

void index::partition_state::deserialize(deserializer& source)
{
  source >> events >> first_event >> last_event >> last_modified;
}

index::index(path const& dir, size_t batch_size, size_t max_events,
             size_t max_parts, size_t active_parts)
  : dir_{dir / "index"},
    batch_size_{batch_size},
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
        auto empty = true;
        for (auto& p : partitions_)
          if (p.second.events > 0)
          {
            empty = false;
            break;
          }

        if (! empty)
        {
          auto t = io::archive(dir_ / "meta.data", partitions_);
          if (! t)
            VAST_ERROR(this, "failed to save meta data:", t.error());
        }

        queries_.clear();
        partitions_.clear();
      });
}

void index::dispatch(uuid const& part, predicate const& pred)
{
  auto i = std::find_if(
      schedule_.begin(),
      schedule_.end(),
      [&](schedule_state const& s) { return s.part == part; });

  // If the partition is queued, we add the predicate to the set of
  // to-be-queried predicates.
  if (i != schedule_.end())
  {
    VAST_DEBUG(this, "adds predicate to", part << ":", pred);
    i->predicates.insert(pred);

    // If the partition is in memory we can send it the predicate directly.
    auto& a = partitions_[part].actor;
    if (a)
      send(a, expression{pred}, this);

    return;
  }

  // If the partition is not in memory we enqueue it in the schedule.
  VAST_DEBUG(this, "enqueues partition", part, "with", pred);
  schedule_.push_back(index::schedule_state{part, {pred}});

  if (std::find(active_.begin(), active_.end(), part) != active_.end())
  {
    // If we have an active partition, we only need to relay the predicate.
    auto& a = partitions_[part].actor;
    assert(a);
    send(a, expression{pred}, this);
  }
  else if (passive_.size() < max_partitions_ - active_partitions_)
  {
    // If we have not fully maxed out our available passive partitions, we can
    // spawn the partition directly.
    passive_.push_back(part);
    VAST_DEBUG(this, "spawns passive partition", part);
    auto& a = partitions_[part].actor;
    assert(! a);
    a = spawn<partition, monitored>(dir_, part, batch_size_);
    send(a, expression{pred}, this);
  }
}

void index::consolidate(uuid const& part, predicate const& pred)
{
  VAST_DEBUG(this, "consolidates", pred, "for partition", part);

  auto i = std::find_if(
      schedule_.begin(),
      schedule_.end(),
      [&](schedule_state const& s) { return s.part == part; });

  assert(i != schedule_.end());

  // Remove the completed predicate of the partition.
  assert(! i->predicates.empty());
  auto x = i->predicates.find(pred);
  assert(x != i->predicates.end());
  i->predicates.erase(x);

  // We keep the partition in the schedule as long as there exist outstanding
  // predicates.
  if (! i->predicates.empty())
  {
    VAST_DEBUG(this, "got completed predicate", pred, "for partition",
               part << ',', i->predicates.size(), "remaining");
    return;
  }

  VAST_DEBUG(this, "evicts completed partition", part);
  schedule_.erase(i);

  if (schedule_.empty())
    VAST_DEBUG(this, "finished with entire schedule");

  // We don't unload active partitions.
  if (std::find(active_.begin(), active_.end(), part) != active_.end())
    return;

  // It could well happen that we dispatched a predicate to an active partition
  // which then gets replaced with a new partition. In this case the partition
  // replaced partition is neither in the active nor passive set, allowing us
  // to ignore this consolidation request.
  auto j = std::find(passive_.begin(), passive_.end(), part);
  if (j == passive_.end())
    return;

  passive_.erase(j);
  auto& p = partitions_[part];
  assert(p.actor);
  send_exit(p.actor, exit::stop);
  p.actor = invalid_actor;

  // Because partitions can complete in any order, we have to walk through the
  // schedule from the beginning again to find the next partition.
  for (auto& entry : schedule_)
  {
    auto& next_actor = partitions_[entry.part].actor;
    if (! next_actor)
    {
      VAST_DEBUG(this, "schedules next partition", entry.part);
      passive_.push_back(entry.part);
      next_actor = spawn<partition, monitored>(dir_, entry.part, batch_size_);
      for (auto& next_pred : entry.predicates)
        send(next_actor, expression{next_pred}, this);
      break;
    }
  }
}

double index::progress(expression const& expr) const
{
  auto parts = 0.0;
  auto preds = 0.0;
  auto i = queries_.find(expr);
  assert(i != queries_.end());
  auto j = i->second.predicates.find(expr);
  auto& restrictions = j->second.restrictions;

  if (restrictions.empty())
    return 1.0;

  for (auto& part : restrictions)
  {
    auto part_pred = 0.0;
    auto ps = visit(predicatizer{}, expr);
    assert(! ps.empty());
    for (auto& pred : ps)
    {
      auto& status = partitions_.find(part)->second.status.find(pred)->second;
      auto& expected = status.expected;
      if (expected)
        part_pred += *expected == 0 ? 1 : double(status.got) / *expected;
    }

    part_pred /= ps.size();
    preds += part_pred;
    if (part_pred > 0)
      ++parts;
  }

  preds /= parts > 0 ? parts : 1.0;
  parts /= restrictions.size();

  return parts * preds;
}

void index::at_down(down_msg const& msg)
{
  VAST_DEBUG(this, "got DOWN from", last_sender());

  auto found = false;
  for (auto i = active_.begin(); i != active_.end(); ++i)
  {
    auto& p = partitions_[*i];
    if (p.actor == last_sender())
    {
      p.actor = invalid_actor;
      active_.erase(i);
      VAST_DEBUG(this, "shrinks active partitions to",
                 active_.size() << '/' << active_partitions_);
      found = true;
      break;
    }
  }

  if (! found)
    for (auto i = passive_.begin(); i != passive_.end(); ++i)
    {
      auto& p = partitions_[*i];
      if (p.actor == last_sender())
      {
        p.actor = invalid_actor;
        passive_.erase(i);
        VAST_DEBUG(this, "shrinks passive partitions to", passive_.size() << 
                   '/' << max_partitions_ - active_partitions_);
        break;
      }
    }

  // If a partition did not exit with error/kill, we wait until all of
  // them terminate.
  if (msg.reason == exit::stop || msg.reason == exit::done)
  {
    if (active_.empty())
      quit(msg.reason);
  }
  else
  {
    quit(msg.reason);
  }
}

void index::at_exit(exit_msg const& msg)
{
  if (active_.empty())
    quit(msg.reason);
  else
    for (auto& p : partitions_)
      if (p.second.actor)
        send_exit(p.second.actor, msg.reason);
}

message_handler index::make_handler()
{
  trap_exit(true);

  VAST_VERBOSE(this, "caps partitions at", max_events_per_partition_, "events");
  VAST_VERBOSE(this, "uses", active_partitions_ << "/" << max_partitions_,
               "active partitions");

  if (exists(dir_ / "meta.data"))
  {
    auto t = io::unarchive(dir_ / "meta.data", partitions_);
    if (! t)
    {
      VAST_ERROR(this, "failed to load meta data:", t.error());
      quit(exit::error);
      return {};
    }
  }

  // Use the N last modified partitions which still have not exceeded their
  // capacity.
  std::vector<std::pair<uuid, partition_state>> parts;
  for (auto& p : partitions_)
    if (p.second.events < max_events_per_partition_)
      parts.push_back(p);

  std::sort(parts.begin(),
            parts.end(),
            [](auto x, auto y)
            {
              return y.second.last_modified < x.second.last_modified;
            });

  active_.resize(active_partitions_);
  for (size_t i = 0; i < active_partitions_; ++i)
  {
    auto id = i < parts.size() ? parts[i].first : uuid::random();
    auto& p = partitions_[id];
    VAST_DEBUG(this, "activates partition", id);
    p.actor = spawn<partition, priority_aware+monitored>(dir_, id, batch_size_);
    send(p.actor, flow_control::announce{this});
    active_[i] = std::move(id);
  }

  return
  {
    on(atom("flush")) >> [=]
    {
      auto tree = spawn<task_tree>(this);
      for (auto& id : active_)
      {
        send(tree, this, partitions_[id].actor);
        send(partitions_[id].actor, atom("flush"), tree);
      }

      return tree;
    },
    [=](chunk const& chk)
    {
      auto& id = active_[next_];
      next_ = ++next_ % active_.size();

      auto i = partitions_.find(id);
      assert(i != partitions_.end());
      assert(i->second.actor);

      // Replace partition with new one on overflow.
      if (i->second.events + chk.events() > max_events_per_partition_)
      {
        VAST_DEBUG(this, "replaces", i->second.actor, '(' << id << ')');
        send_exit(i->second.actor, exit::stop);
        i->second.actor = invalid_actor;

        id = uuid::random();
        i = partitions_.emplace(id, partition_state{}).first;
        i->second.actor =
          spawn<partition, priority_aware+monitored>(dir_, id, batch_size_);
        send(i->second.actor, flow_control::announce{this});
      }

      // Update partition meta data.
      auto& p = i->second;
      p.events += chk.events();
      p.last_modified = now();
      if (p.first_event == time_range{} || chk.meta().first < p.first_event)
        p.first_event = chk.meta().first;
      if (p.last_event == time_range{} || chk.meta().last > p.last_event)
        p.last_event = chk.meta().last;

      forward_to(p.actor);
      VAST_DEBUG(this, "forwards chunk to", p.actor, '(' << id << ')');
    },
    on(atom("query"), arg_match) >> [=](expression const& ast, actor sink)
    {
      if (! queries_.count(ast))
      {
        auto preds = visit(predicatizer{}, ast);
        auto root = std::make_shared<expression>(ast);
        for (auto& pred : preds)
          predicates_.emplace(std::move(pred), root);

        std::map<expression, std::vector<uuid>> restrictions;
        visit(builder{partitions_, restrictions}, ast);
        visit(pusher{restrictions}, ast);

        for (auto& p : restrictions)
          queries_[ast].predicates[p.first].restrictions = std::move(p.second);

        visit(dispatcher{*this}, ast);

        VAST_DEBUG(this, "evaluates", ast);
        visit(evaluator{*this}, ast);
      }

      queries_[ast].subscribers.insert(sink);

      auto& hits = queries_[ast].predicates[ast].hits;
      if (hits && ! hits.all_zero())
        send(sink, hits);

      send(sink, atom("progress"), progress(ast), hits ? hits.count() : 0);
    },
    [=](expression const& pred, uuid const& part, uint64_t n)
    {
      VAST_DEBUG(this, "expects partition", part, "to deliver", n,
                 "hits for predicate", pred);

      auto& status = partitions_[part].status[pred];
      status.expected = n;

      // It could happen that we receive all hits before we get the actual
      // expected number.
      if (status.got == n)
        consolidate(part, *get<predicate>(pred));
    },
    [=](expression const& pred, uuid const& part, bitstream const& hits)
    {
      VAST_DEBUG(this, "received", (hits ? hits.count() : 0), "hits from",
                 part, "for predicate", pred);

      assert(partitions_[part].status.count(pred));
      auto& status = partitions_[part].status[pred];
      status.hits |= hits;
      ++status.got;

      // Once we have received all hits from a partition, we remove it from
      // the schedule.
      if (status.expected && status.got == *status.expected)
        consolidate(part, *get<predicate>(pred));

      // Re-evaluate all affected queries.
      auto range = predicates_.equal_range(pred);
      for (auto i = range.first; i != range.second; ++i)
      {
        auto& root = *i->second;
        VAST_DEBUG(this, "evaluates", root);
        auto& qs = queries_[root];
        qs.predicates[pred].hits |= hits;
        auto changed = visit(propagator{*this, i->first}, root);
        auto& query_hits = qs.predicates[root].hits;
        if (changed)
        {
          assert(query_hits);
          if (! query_hits.all_zero())
            for (auto& sink : qs.subscribers)
              send(sink, query_hits);
        }

        auto count = query_hits ? query_hits.count() : 0;
        for (auto& sink : qs.subscribers)
          send(sink, atom("progress"), progress(root), count);
      }
    },
    on(atom("delete")) >> [=]
    {
      become(
          keep_behavior,
          [=](down_msg const& d)
          {
            if (d.reason != exit::kill)
              VAST_WARN(this, "got DOWN from", last_sender(),
                        "with unexpected exit code", d.reason);

            for (auto i = active_.begin(); i != active_.end(); ++i)
              if (partitions_[*i].actor == last_sender())
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

      for (auto& id : active_)
        send_exit(partitions_[id].actor, exit::kill);
    }
  };
}

std::string index::name() const
{
  return "index";
}

} // namespace vast
