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

using restriction_map = std::map<expression, std::vector<uuid>>;

} // namespace <anonymous>

// Registers each sub-tree with the GQG.
struct index::dissector
{
  dissector(index& idx, expression const& root)
    : index_{idx},
      root_{root}
  {
  }

  void operator()(none)
  {
    assert(! "should never happen");
  }

  void operator()(conjunction const& con)
  {
    if (! is<none>(parent_))
      index_.gqg_[con].insert(parent_);
    else if (con == root_)
      index_.gqg_[con];

    parent_ = con;
    for (auto& op : con)
      visit(*this, op);
  }

  void operator()(disjunction const& dis)
  {
    if (! is<none>(parent_))
      index_.gqg_[dis].insert(parent_);
    else if (dis == root_)
      index_.gqg_[dis];

    parent_ = dis;
    for (auto& op : dis)
      visit(*this, op);
  }

  void operator()(negation const& n)
  {
    if (! is<none>(parent_))
      index_.gqg_[n].insert(parent_);
    else if (n == root_)
      index_.gqg_[n];

    parent_ = n;
    visit(*this, n[0]);
  }

  void operator()(predicate const& pred)
  {
    assert(! is<none>(parent_) || pred == root_);

    if (is<none>(parent_))
      index_.gqg_[pred];
    else
      index_.gqg_[pred].insert(parent_);
  }

  index& index_;
  expression const& root_;
  expression parent_;
};

// Builds the set of restrictions bottom-up.
struct index::builder
{
  builder(index const& idx, restriction_map& restrictions)
    : index_{idx},
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
  }

  void operator()(disjunction const& dis)
  {
    for (auto& op : dis)
      visit(*this, op);

    auto& r = restrictions_[dis];
    for (auto& op : dis)
      r = unify(r, restrictions_[op]);
  }

  void operator()(negation const& n)
  {
    visit(*this, n[0]);
  }

  void operator()(predicate const& pred)
  {
    auto e = get<time_extractor>(pred.lhs);
    if (! e)
    {
      if (all_.empty())
      {
        all_.reserve(index_.part_state_.size());
        for (auto& p : index_.part_state_)
          all_.push_back(p.first);

        all_.shrink_to_fit();
        std::sort(all_.begin(), all_.end());
      }

      restrictions_[pred] = all_;
    }
    else
    {
      auto d = get<data>(pred.rhs);
      assert(d && is<time_point>(*d));

      restriction_map::mapped_type partitions;
      VAST_LOG_DEBUG("checking restrictors for " << expression{pred});
      for (auto& p : index_.part_state_)
      {
        if (data::evaluate(p.second.first, pred.op, *d))
        {
          VAST_LOG_DEBUG("  - " << p.second.first << " for " << p.first);
          partitions.push_back(p.first);
        }
        else if (data::evaluate(p.second.last, pred.op, *d))
        {
          VAST_LOG_DEBUG("  - " << p.second.last << " for " << p.first);
          partitions.push_back(p.first);
        }
      }

      std::sort(partitions.begin(), partitions.end());
      restrictions_[pred] = std::move(partitions);
    }
  }

  index const& index_;
  restriction_map& restrictions_;
  restriction_map::mapped_type all_;
};

// Propagates the accumulated restrictions top-down.
struct index::pusher
{
  pusher(restriction_map& restrictions)
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
      VAST_LOG_DEBUG("pushing restriction of " << expression{con} <<
                     " to " << expression{op});
      for (auto& p : r)
        VAST_LOG_DEBUG("  -  " << p);

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
      VAST_LOG_DEBUG("pushing restriction of " << expression{dis} <<
                     " to " << expression{op});
      for (auto& p : r)
        VAST_LOG_DEBUG("  -  " << p);

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

  restriction_map& restrictions_;
};

struct index::evaluator
{
public:
  evaluator(index& idx, restriction_map& restrictions)
    : index_{idx},
      restrictions_{restrictions}
  {
  }

  void operator()(none)
  {
    assert(! "should never happen");
  }

  void operator()(conjunction const& con)
  {
    bitstream hits;
    for (auto& op : con)
    {
      visit(*this, op);
      if (! result_.hits)
      {
        // Short circuit evaluation.
        result_.hits = {};
        return;
      }

      hits &= result_.hits;
    }

    result_.hits = std::move(hits);
  }

  void operator()(disjunction const& dis)
  {
    bitstream hits;
    for (auto& op : dis)
    {
      visit(*this, op);
      if (result_.hits)
        hits |= result_.hits;
    }

    result_.hits = std::move(hits);
  }

  void operator()(negation const& n)
  {
    visit(*this, n[0]);
    result_.hits.flip();
  }

  void operator()(predicate const& pred)
  {
    result_.hits = {};
    double got = 0.0;
    double need = 0.0;
    double misses = 0.0;

    auto& parts = index_.predicate_cache_[pred].parts;
    VAST_LOG_DEBUG("evaluating " << expression{pred});
    for (auto& r : restrictions_[pred])
    {
      auto i = parts.find(r);
      if (i == parts.end())
      {
        ++misses;

        // FIXME: for now, all partition actors reside in memory. This needs to
        // change because it's not possible to keep them all hot.
        assert(index_.part_actors_.count(r));

        VAST_LOG_DEBUG("cache miss for partition " << r << ", asking " <<
                       index_.part_actors_[r]);

        actor self = &index_;
        index_.send(index_.part_actors_[r], expression{pred}, self);

        parts[r];
      }
      else if (! i->second.expected)
      {
        ++misses;
      }
      else
      {
        if (i->second.hits)
          result_.hits |= i->second.hits;

        got += i->second.got;
        need += *i->second.expected;
      }

      assert(parts.count(r));
      VAST_LOG_DEBUG("  - " << r <<
                     ": " << parts[r].got << '/' <<
                     (parts[r].expected ? to_string(*parts[r].expected) : "-"));
    }

    if (! parts.empty())
    {
      auto completion = double(parts.size() - misses) / double(parts.size());
      auto progress = need == 0 ? 1.0 : got / need;
      VAST_LOG_DEBUG("  -> completion:  " << int(completion * 100) <<
                     "%, progress: " << int(progress * 100) << "%");

      result_.predicate_progress[pred] = completion * progress;
    }
  }

  index& index_;
  restriction_map& restrictions_;
  index::evaluation result_;
};

index::index(path const& dir, size_t batch_size)
  : dir_{dir / "index"},
    batch_size_{batch_size}
{
}

index::evaluation index::evaluate(expression const& ast)
{
  assert(queries_.count(ast));

  restriction_map r;
  visit(builder{*this, r}, ast);

  for (auto& p : r)
  {
    VAST_LOG_ACTOR_DEBUG("built restriction of " << ast << " for " << p.first);
    for (auto& u : p.second)
      VAST_LOG_DEBUG("  -  " << u);
  }

  visit(pusher{r}, ast);

  evaluator e{*this, r};
  visit(e, ast);

  auto& er = e.result_;
  if (er.total_progress != 1.0 && ! er.predicate_progress.empty())
  {
    double sum = 0.0;
    for (auto& p : er.predicate_progress)
      sum += p.second;
    er.total_progress = sum / er.predicate_progress.size();
  }

  VAST_LOG_ACTOR_DEBUG("evaluated " << ast <<
                       " (" << int(er.total_progress * 100) << "%)");
  for (auto& p : er.predicate_progress)
    VAST_LOG_DEBUG("  -  " << int(p.second * 100) << "% of " << p.first);

  return std::move(er);
}

trial<void> index::make_partition(path const& dir)
{
  auto name = dir.basename();

  uuid id;
  auto i = part_map_.find(name.str());
  if (i != part_map_.end())
  {
    id = i->second;
  }
  else
  {
    if (exists(dir))
    {
      partition::meta_data meta;

      if (! exists(dir / partition::part_meta_file))
        return error{"couldn't find meta data of partition ", name};
      else if (! io::unarchive(dir / partition::part_meta_file, meta))
        return error{"failed to read meta data of partition ", name};

      update_partition(meta.id, meta.first_event, meta.last_event);

      id = meta.id;
    }
    else
    {
      id = uuid::random();
    }

    part_map_.emplace(name.str(), id);
  }

  auto& a = part_actors_[id];
  if (! a)
    a = spawn<partition, monitored>(dir, batch_size_, id);

  return nothing;
}

void index::update_partition(uuid const& id, time_point first, time_point last)
{
  auto& p = part_state_[id];

  if (p.first == time_range{} || first < p.first)
    p.first = first;

  if (p.last == time_range{} || last > p.last)
    p.last = last;
}

std::vector<expression> index::walk(
    expression const& start,
    std::function<bool(expression const&, util::flat_set<expression> const&)> f)
{
  auto i = gqg_.find(start);
  if (i == gqg_.end() || ! f(i->first, i->second))
    return {};

  std::vector<expression> path;
  for (auto& parent : i->second)
  {
    auto ppath = walk(parent, f);
    std::move(ppath.begin(), ppath.end(), std::back_inserter(path));
  }

  path.push_back(start);
  std::sort(path.begin(), path.end());
  path.erase(std::unique(path.begin(), path.end()), path.end());

  return path;
}

message_handler index::act()
{
  trap_exit(true);

  traverse(
      dir_,
      [&](path const& p) -> bool
      {
        VAST_LOG_ACTOR_VERBOSE("found partition " << p.basename());

        auto r = make_partition(p);
        if (! r)
          VAST_LOG_ACTOR_ERROR(r.error());

        return true;
      });

  if (! part_map_.empty())
  {
    active_part_ = *part_actors_.find(part_map_.rbegin()->second);
    VAST_LOG_ACTOR_INFO("appends to existing partition " <<
                        part_map_.rbegin()->first);
  }

  attach_functor(
      [=](uint32_t)
      {
        queries_.clear();
        part_actors_.clear();
        active_part_.second = invalid_actor;
      });

  return
  {
    [=](exit_msg const& e)
    {
      if (part_actors_.empty())
        quit(e.reason);
      else
        for (auto& p : part_actors_)
          send_exit(p.second, e.reason);
    },
    [=](down_msg const& d)
    {
      VAST_LOG_ACTOR_DEBUG("got DOWN from " << last_sender());

      if (active_part_.second == last_sender())
        active_part_.second = invalid_actor;

      for (auto i = part_actors_.begin(); i != part_actors_.end(); ++i)
        if (i->second == last_sender())
        {
          part_actors_.erase(i);
          break;
        }

      if (d.reason == exit::stop || d.reason == exit::done)
      {
        if (part_actors_.empty())
          quit(d.reason);
      }
      else
      {
        if (d.reason != exit::error)
          VAST_LOG_ACTOR_WARN("terminates with unknown exit code from " <<
                              last_sender() << ": " << d.reason);

        quit(exit::error);
      }
    },
    on(atom("flush")) >> [=]
    {
      auto tree = spawn<task_tree>(this);
      for (auto& p : part_actors_)
      {
        send(tree, this, p.second);
        send(p.second, atom("flush"), tree);
      }

      return tree;
    },
    on(atom("partition"), arg_match) >> [=](std::string const& dir)
    {
      auto r = make_partition(dir_ / path{dir});
      if (! r)
      {
        VAST_LOG_ACTOR_ERROR(r.error());
        send_exit(this, exit::error);
        return;
      }

      assert(part_map_.count(dir));
      assert(part_actors_.count(part_map_[dir]));

      active_part_ = *part_actors_.find(part_map_[dir]);
      VAST_LOG_ACTOR_INFO("now appends to partition " << dir);

      assert(active_part_.second);
    },
    [=](chunk const& chk)
    {
      if (part_map_.empty())
      {
        auto r = make_partition(dir_ / to_string(uuid::random()).data());
        if (! r)
        {
          VAST_LOG_ACTOR_ERROR(r.error());
          send_exit(this, exit::error);
          return;
        }

        auto first = part_map_.begin();
        active_part_ = *part_actors_.find(first->second);
        VAST_LOG_ACTOR_INFO("created new partition " << first->first);
      }

      update_partition(active_part_.first, chk.meta().first, chk.meta().last);

      VAST_LOG_ACTOR_DEBUG("forwards chunk");
      forward_to(active_part_.second);
    },
    on(atom("backlog")) >> [=]
    {
      forward_to(active_part_.second);
    },
    on(atom("query"), arg_match) >> [=](expression const& ast, actor sink)
    {
      if (part_map_.empty())
      {
        VAST_LOG_ACTOR_WARN("has no partitions to answer queries");
        send_exit(sink, exit::error);
        return;
      }

      if (! queries_.count(ast))
        visit(dissector{*this, ast}, ast);

      assert(! queries_[ast].subscribers.contains(sink));
      queries_[ast].subscribers.insert(sink);

      send(this, atom("first-eval"), ast, sink);
    },
    on(atom("first-eval"), arg_match) >> [=](expression const& ast, actor sink)
    {
      auto e = evaluate(ast);

      if (e.total_progress == 0.0)
        return;

      auto& qs = queries_[ast];
      if (e.hits && ! e.hits.all_zero()
          && (! qs.hits || e.hits != qs.hits || e.total_progress == 1.0))
      {
        qs.hits = e.hits;
        send(sink, e.hits);
      }

      auto count = qs.hits ? qs.hits.count() : 0;
      send(sink, atom("progress"), e.total_progress, count);
    },
    [=](expression const& pred, uuid const& part, uint64_t n)
    {
      VAST_LOG_ACTOR_DEBUG("expects partition " << part <<
                           " to deliver " << n << " hits for predicate " <<
                           pred);

      predicate_cache_[pred].parts[part].expected = n;
    },
    [=](expression const& pred, uuid const& part, bitstream const& hits)
    {
      VAST_LOG_ACTOR_DEBUG(
          "received " << (hits ? hits.count() : 0) <<
          " hits from " << part << " for predicate " << pred);

      assert(predicate_cache_.count(pred));

      auto& entry = predicate_cache_[pred].parts[part];

      if (hits)
      {
        ++entry.got;
        entry.hits |= hits;
      }

      assert(! entry.expected || entry.got <= *entry.expected);

      std::vector<expression> roots;
      walk(
          pred,
          [&](expression const& ast, util::flat_set<expression> const&) -> bool
          {
            if (queries_.count(ast))
              roots.push_back(ast);

            return true;
          });

      for (auto& r : roots)
      {
        auto e = evaluate(r);

        auto& qs = queries_[r];
        if (e.hits && ! e.hits.all_zero() && (! qs.hits || e.hits != qs.hits))
        {
          qs.hits = e.hits;
          for (auto& sink : qs.subscribers)
            send(sink, e.hits);
        }

        if (e.total_progress == 0.0)
          return;

        for (auto& sink : qs.subscribers)
          send(sink, atom("progress"),
               e.total_progress, qs.hits ? qs.hits.count() : 0);
      }
    },
    on(atom("delete")) >> [=]
    {
      if (part_map_.empty())
      {
        VAST_LOG_ACTOR_WARN("ignores request to delete empty index");
        return;
      }

      become(
          keep_behavior,
          [=](down_msg const& d)
          {
            if (d.reason != exit::kill)
              VAST_LOG_ACTOR_WARN("got DOWN from " << last_sender() <<
                                  " with unexpected exit code " << d.reason);

            for (auto i = part_actors_.begin(); i != part_actors_.end(); ++i)
              if (i->second == last_sender())
              {
                part_actors_.erase(i);
                break;
              }

            if (part_actors_.empty())
            {
              if (! rm(dir_))
              {
                VAST_LOG_ACTOR_ERROR("failed to delete index directory: " <<
                                     dir_);
                send_exit(this, exit::error);
                return;
              }

              VAST_LOG_ACTOR_INFO("deleted index: " << dir_);
              unbecome();
            }
          }
      );

      for (auto& p : part_actors_)
        send_exit(p.second, exit::kill);
    }
  };
}

std::string index::describe() const
{
  return "index";
}

} // namespace vast
