#include "vast/index.h"

#include <cppa/cppa.hpp>
#include "vast/bitmap_index.h"
#include "vast/segment.h"
#include "vast/expression.h"
#include "vast/partition.h"
#include "vast/print.h"
#include "vast/io/serialization.h"

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
        all_.reserve(index_.partitions_.size());
        for (auto& p : index_.partitions_)
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
      for (auto& p : index_.partitions_)
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

    auto& parts = index_.cache_[pred].parts;
    VAST_LOG_DEBUG("evaluating " << expr::ast{pred});
    for (auto& r : restrictions_[pred])
    {
      auto i = parts.find(r);
      if (i == parts.end())
      {
        ++misses;
        if (index_.on_miss_(pred, r))
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

      assert(parts.count(r)); // FIXME: assumes on_miss_ returns always true.
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

void index::set_on_miss(miss_callback f)
{
  on_miss_ = f;
}

trial<index::evaluation> index::evaluate(expr::ast const& ast)
{
  if (! queries_.contains(ast))
    return error{"not a registered query: ", ast};

  restriction_map r;
  builder b{*this, r};
  pusher p{r};
  evaluator e{*this, r};

  ast.accept(b);

  for (auto& pair : r)
  {
    VAST_LOG_DEBUG("built restriction of " << ast << " for " << pair.first);
    for (auto& u : pair.second)
      VAST_LOG_DEBUG("  -  " << u);
  }

  ast.accept(p);
  ast.accept(e);

  auto& er = e.result_;
  if (er.total_progress != 1.0 && ! er.predicate_progress.empty())
  {
    double sum = 0.0;
    for (auto& p : er.predicate_progress)
      sum += p.second;
    er.total_progress = sum / er.predicate_progress.size();
  }

  VAST_LOG_DEBUG("evaluated " << ast <<
                 " (" << int(er.total_progress * 100) << "%)");
  for (auto& pair : er.predicate_progress)
    VAST_LOG_DEBUG("  -  " << int(pair.second * 100) << "% of " << pair.first);

  return std::move(er);
}

trial<index::evaluation> index::add_query(expr::ast const& qry)
{
  if (! queries_.contains(qry))
  {
    dissector d{*this, qry};
    qry.accept(d);
    queries_.insert(qry);
  }

  return evaluate(qry);
}

void index::expect(expr::ast const& pred, uuid const& part, uint64_t n)
{
  cache_[pred].parts[part].expected = n;
}

void index::update_partition(uuid const& id, time_point first, time_point last)
{
  auto& p = partitions_[id];

  if (p.first == time_range{} || first < p.first)
    p.first = first;

  if (p.last == time_range{} || last > p.last)
    p.last = last;
}

std::vector<expr::ast> index::update_hits(expr::ast const& pred,
                                          uuid const& part,
                                          bitstream const& hits)
{
  assert(cache_.count(pred));

  auto& entry = cache_[pred].parts[part];

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
        if (queries_.contains(ast))
          roots.push_back(ast);

        return true;
      });

  return roots;
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

using namespace cppa;

index_actor::index_actor(path dir, size_t batch_size)
  : dir_{std::move(dir)},
    batch_size_{batch_size}
{
}

char const* index_actor::description() const
{
  return "index";
}

trial<void> index_actor::make_partition(path const& dir)
{
  auto name = dir.basename();

  uuid id;
  auto i = parts_.find(name.str());
  if (i != parts_.end())
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

      index_.update_partition(meta.id, meta.first_event, meta.last_event);

      id = meta.id;
    }
    else
    {
      id = uuid::random();
    }

    parts_.emplace(name.str(), id);
  }

  auto& a = part_actors_[id];
  if (! a)
    a = spawn<partition_actor, monitored>(dir, batch_size_, id);

  return nothing;
}

void index_actor::act()
{
  chaining(false);
  trap_exit(true);

  index_.set_on_miss(
      [&](expr::ast const& pred, uuid const& id) -> bool
      {
        // For now, all partitions reside in memory.
        assert(part_actors_.count(id));

        send(part_actors_[id], pred, self);
        return true;
      });

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

  if (! parts_.empty())
  {
    active_ = *part_actors_.find(parts_.rbegin()->second);
    VAST_LOG_ACTOR_INFO("appends to existing partition " <<
                        parts_.rbegin()->first);
  }

  become(
      on(atom("EXIT"), arg_match) >> [=](uint32_t reason)
      {
        if (part_actors_.empty())
          quit(reason);
        else
          for (auto& p : part_actors_)
            send_exit(p.second, reason);
      },
      on(atom("DOWN"), arg_match) >> [=](uint32_t reason)
      {
        VAST_LOG_ACTOR_DEBUG("got DOWN from " << VAST_ACTOR_ID(last_sender()));

        if (active_.second == last_sender())
          active_.second = nullptr;

        for (auto i = part_actors_.begin(); i != part_actors_.end(); ++i)
          if (i->second == last_sender())
          {
            part_actors_.erase(i);
            break;
          }

        if (reason == exit::stop || reason == exit::done)
        {
          if (part_actors_.empty())
            quit(reason);
        }
        else
        {
          if (reason != exit::error)
            VAST_LOG_ACTOR_WARN(
                "terminates with unknown exit code from " <<
               VAST_ACTOR_ID(last_sender()) << ": " << reason);

          quit(exit::error);
        }
      },
      on(atom("partition"), arg_match) >> [=](std::string const& dir)
      {
        auto r = make_partition(dir_ / path{dir});
        if (! r)
        {
          VAST_LOG_ACTOR_ERROR(r.error());
          send_exit(self, exit::error);
          return;
        }

        assert(parts_.count(dir));
        assert(part_actors_.count(parts_[dir]));

        active_ = *part_actors_.find(parts_[dir]);
        VAST_LOG_ACTOR_INFO("now appends to partition " << dir);

        assert(active_.second);
      },
      on_arg_match >> [=](segment const& s)
      {
        if (parts_.empty())
        {
          auto r = make_partition(dir_ / to_string(uuid::random()).data());
          if (! r)
          {
            VAST_LOG_ACTOR_ERROR(r.error());
            send_exit(self, exit::error);
            return;
          }

          auto first = parts_.begin();
          active_ = *part_actors_.find(first->second);
          VAST_LOG_ACTOR_INFO("created new partition " << first->first);
        }

        VAST_LOG_ACTOR_DEBUG("got segment covering [" <<
                             s.first() << ',' << s.last() << ']');

        index_.update_partition(active_.first, s.first(), s.last());

        forward_to(active_.second);
      },
      on(atom("backlog")) >> [=]
      {
        forward_to(active_.second);
      },
      on(atom("query"), arg_match)
        >> [=](expr::ast const& ast, actor_ptr const& sink)
      {
        if (parts_.empty())
        {
          auto err = error{"has no partitions to answer queries"};
          VAST_LOG_ACTOR_WARN(err);
          return make_any_tuple(err);
        }

        assert(! queries_[ast].subscribers.contains(sink));
        queries_[ast].subscribers.insert(sink);

        auto e = index_.add_query(ast);
        if (e)
        {
          uint64_t count = e->hits ? e->hits.count() : 0;

          if (e->hits && e->hits.find_first() != bitstream::npos)
          {
            VAST_LOG_ACTOR_DEBUG("notifies " << VAST_ACTOR_ID(sink) <<
                                 " with complete result for " << ast);

            send(sink, std::move(e->hits), e->total_progress);
          }

          send(sink, atom("progress"), e->total_progress, count);

          return make_any_tuple(atom("success"));
        }
        else
        {
          return make_any_tuple(e.error());
        }
      },
      on_arg_match >> [=](expr::ast const& pred, uuid const& part, uint64_t n)
      {
        VAST_LOG_ACTOR_DEBUG("expects partition " << part <<
                             " to deliver " << n << " predicates " << pred);

        index_.expect(pred, part, n);
      },
      on_arg_match >> [=](expr::ast const& pred, uuid const& part,
                          bitstream const& hits)
      {
        VAST_LOG_ACTOR_DEBUG(
            "received " << (hits ? hits.count() : 0) <<
            " hits from " << part << " for predicate " << pred);

        for (auto& q : index_.update_hits(pred, part, hits))
        {
          auto e = index_.evaluate(q);
          if (e)
          {
            auto& qs = queries_[q];

            if (e->hits && e->hits.find_first() != bitstream::npos
                && (! qs.hits || e->hits != qs.hits))
            {
              qs.hits = e->hits;
              for (auto& sink : qs.subscribers)
              {
                VAST_LOG_ACTOR_DEBUG("notifies " << VAST_ACTOR_ID(sink) <<
                                     " with new result for " << q << " (" <<
                                     int(e->total_progress * 100) << "%)");

                send(sink, e->hits, e->total_progress);
              }
            }

            uint64_t count = qs.hits ? qs.hits.count() : 0;
            for (auto& s : qs.subscribers)
              send(s, atom("progress"), e->total_progress, count);
          }
          else
          {
            VAST_LOG_ACTOR_ERROR(e.error());
            send_exit(self, exit::error);
            return;
          }
        }
      },
      on(atom("delete")) >> [=]
      {
        if (parts_.empty())
        {
          VAST_LOG_ACTOR_WARN("ignores request to delete empty index");
          return;
        }

        become(
            keep_behavior,
            on(atom("DOWN"), arg_match) >> [=](uint32_t reason)
            {
              if (reason != exit::kill)
                VAST_LOG_ACTOR_WARN(
                    "got DOWN from " << VAST_ACTOR_ID(last_sender()) <<
                    " with unexpected exit code " << reason);

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
                  send_exit(self, exit::error);
                  return;
                }

                VAST_LOG_ACTOR_INFO("deleted index: " << dir_);
                unbecome();
              }
            }
        );

        for (auto& p : part_actors_)
          send_exit(p.second, exit::kill);
      });
}

} // namespace vast
