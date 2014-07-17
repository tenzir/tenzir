#include "vast/index.h"

#include <caf/all.hpp>
#include "vast/bitmap_index.h"
#include "vast/segment.h"
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

using restriction_map = std::map<expr::ast, std::vector<uuid>>;

} // namespace <anonymous>

// Registers each sub-tree with the GQG.
class index::dissector : public expr::default_const_visitor
{
public:
  dissector(index& idx, expr::ast const& root)
    : index_{idx},
      root_{root}
  {
  }

  virtual void visit(expr::conjunction const& conj)
  {
    expr::ast ast{conj};

    if (parent_)
      index_.gqg_[ast].insert(parent_);
    else if (ast == root_)
      index_.gqg_[ast];

    parent_ = ast;
    for (auto& op : conj.operands)
      op->accept(*this);
  }

  virtual void visit(expr::disjunction const& disj)
  {
    expr::ast ast{disj};

    if (parent_)
      index_.gqg_[ast].insert(parent_);
    else if (ast == root_)
      index_.gqg_[ast];

    parent_ = ast;
    for (auto& op : disj.operands)
      op->accept(*this);
  }

  virtual void visit(expr::predicate const& pred)
  {
    assert(parent_);
    index_.gqg_[expr::ast{pred}].insert(parent_);
  }

private:
  expr::ast parent_;
  index& index_;
  expr::ast const& root_;
};

// Builds the set of restrictions.
class index::builder : public expr::default_const_visitor
{
public:
  builder(index const& idx, restriction_map& restrictions)
    : index_{idx},
      restrictions_{restrictions}
  {
  }

  virtual void visit(expr::conjunction const& conj)
  {
    for (auto& operand : conj.operands)
      operand->accept(*this);

    auto& r = restrictions_[conj];
    r = restrictions_[*conj.operands[0]];
    for (size_t i = 1; i < conj.operands.size(); ++i)
      r = intersect(r, restrictions_[*conj.operands[i]]);
  }

  virtual void visit(expr::disjunction const& disj)
  {
    for (auto& operand : disj.operands)
      operand->accept(*this);

    auto& r = restrictions_[disj];
    for (auto& operand : disj.operands)
      r = unify(r, restrictions_[*operand]);
  }

  virtual void visit(expr::predicate const& pred)
  {
    timestamp_found_ = false;
    pred.lhs().accept(*this);
    if (! timestamp_found_)
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
      v_ = nullptr;
      pred.rhs().accept(*this);
      assert(v_);
      auto ts = v_->get<time_point>();

      restriction_map::mapped_type partitions;
      VAST_LOG_DEBUG("checking restrictors for " << expr::ast{pred});
      for (auto& p : index_.part_state_)
      {
        if (pred.pred(p.second.first, ts))
        {
          VAST_LOG_DEBUG("  - " << p.second.first << " for " << p.first);
          partitions.push_back(p.first);
        }
        else if (pred.pred(p.second.last, ts))
        {
          VAST_LOG_DEBUG("  - " << p.second.last << " for " << p.first);
          partitions.push_back(p.first);
        }
      }

      std::sort(partitions.begin(), partitions.end());
      restrictions_[pred] = std::move(partitions);
    }
  }

  virtual void visit(expr::timestamp_extractor const&)
  {
    timestamp_found_ = true;
  }

  virtual void visit(expr::constant const& c)
  {
    v_ = &c.val;
  }

private:
  index const& index_;
  restriction_map& restrictions_;
  restriction_map::mapped_type all_;
  bool timestamp_found_;
  value const* v_;
};

// Propagates the accumulated restrictions.
class index::pusher : public expr::default_const_visitor
{
public:
  pusher(restriction_map& restrictions)
    : restrictions_{restrictions}
  {
  }

  virtual void visit(expr::conjunction const& conj)
  {
    auto& r = restrictions_[conj];
    for (auto& operand : conj.operands)
      restrictions_[*operand] = intersect(r, restrictions_[*operand]);

    for (auto& operand : conj.operands)
    {
      VAST_LOG_DEBUG("pushing restriction of " << expr::ast{conj} <<
                     " to " << expr::ast{*operand});
      for (auto& p : r)
        VAST_LOG_DEBUG("  -  " << p);

      operand->accept(*this);
    }
  }

  virtual void visit(expr::disjunction const& disj)
  {
    auto& r = restrictions_[disj];
    for (auto& operand : disj.operands)
      restrictions_[*operand] = intersect(r, restrictions_[*operand]);

    for (auto& operand : disj.operands)
    {
      VAST_LOG_DEBUG("pushing restriction of " << expr::ast{disj} <<
                     " to " << expr::ast{*operand});
      for (auto& p : r)
        VAST_LOG_DEBUG("  -  " << p);

      operand->accept(*this);
    }
  }

private:
  restriction_map& restrictions_;
};

class index::evaluator : public expr::default_const_visitor
{
public:
  evaluator(index& idx, restriction_map& restrictions)
    : index_{idx},
      restrictions_{restrictions}
  {
  }

  virtual void visit(expr::conjunction const& conj)
  {
    bitstream hits;
    for (auto& operand : conj.operands)
    {
      operand->accept(*this);
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

  virtual void visit(expr::disjunction const& disj)
  {
    bitstream hits;
    for (auto& operand : disj.operands)
    {
      operand->accept(*this);
      if (result_.hits)
        hits |= result_.hits;
    }

    result_.hits = std::move(hits);
  }

  virtual void visit(expr::predicate const& pred)
  {
    result_.hits = {};
    double got = 0.0;
    double need = 0.0;
    double misses = 0.0;

    auto& parts = index_.predicate_cache_[pred].parts;
    VAST_LOG_DEBUG("evaluating " << expr::ast{pred});
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
        index_.send(index_.part_actors_[r], expr::ast{pred}, self);

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

index::evaluation index::evaluate(expr::ast const& ast)
{
  assert(queries_.count(ast));

  restriction_map r;
  builder b{*this, r};
  ast.accept(b);

  for (auto& pair : r)
  {
    VAST_LOG_ACTOR_DEBUG("built restriction of " << ast << " for " << pair.first);
    for (auto& u : pair.second)
      VAST_LOG_DEBUG("  -  " << u);
  }

  pusher p{r};
  ast.accept(p);

  evaluator e{*this, r};
  ast.accept(e);

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
  for (auto& pair : er.predicate_progress)
    VAST_LOG_DEBUG("  -  " << int(pair.second * 100) << "% of " << pair.first);

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
    a = spawn<partition_actor, monitored>(dir, batch_size_, id);

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

std::vector<expr::ast> index::walk(
    expr::ast const& start,
    std::function<bool(expr::ast const&, util::flat_set<expr::ast> const&)> f)
{
  auto i = gqg_.find(start);
  if (i == gqg_.end() || ! f(i->first, i->second))
    return {};

  std::vector<expr::ast> path;
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
    [=](segment const& s)
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

      update_partition(active_part_.first, s.first(), s.last());

      VAST_LOG_ACTOR_DEBUG("forwards segment covering [" <<
                           s.first() << ',' << s.last() << ']');

      forward_to(active_part_.second);
    },
    on(atom("backlog")) >> [=]
    {
      forward_to(active_part_.second);
    },
    on(atom("query"), arg_match) >> [=](expr::ast const& ast, actor sink)
    {
      if (part_map_.empty())
      {
        auto err = error{"has no partitions to answer queries"};
        VAST_LOG_ACTOR_WARN(err);
        return make_message(err);
      }

      if (! queries_.count(ast))
      {
        dissector d{*this, ast};
        ast.accept(d);
      }

      assert(! queries_[ast].subscribers.contains(sink));
      queries_[ast].subscribers.insert(sink);

      send(this, atom("first-eval"), ast, sink);

      return make_message(atom("success"));
    },
    on(atom("first-eval"), arg_match) >> [=](expr::ast const& ast, actor sink)
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
    [=](expr::ast const& pred, uuid const& part, uint64_t n)
    {
      VAST_LOG_ACTOR_DEBUG("expects partition " << part <<
                           " to deliver " << n << " hits for predicate " <<
                           pred);

      predicate_cache_[pred].parts[part].expected = n;
    },
    [=](expr::ast const& pred, uuid const& part, bitstream const& hits)
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

      std::vector<expr::ast> roots;
      walk(
          pred,
          [&](expr::ast const& ast, util::flat_set<expr::ast> const&) -> bool
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
