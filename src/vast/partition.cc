#include "vast/partition.h"

#include <cppa/cppa.hpp>
#include "vast/bitmap_index.h"
#include "vast/event.h"
#include "vast/event_index.h"
#include "vast/segment.h"
#include "vast/io/serialization.h"

using namespace cppa;

namespace vast {

namespace {

// If the given predicate is a name extractor, extracts the name.
struct name_checker : public expr::default_const_visitor
{
  virtual void visit(expr::predicate const& pred)
  {
    pred.lhs().accept(*this);
    if (check_)
      pred.rhs().accept(*this);
  }

  virtual void visit(expr::name_extractor const&)
  {
    check_ = true;
  }

  virtual void visit(expr::constant const& c)
  {
    value_ = c.val;
  }

  bool check_ = false;
  value value_;
};

// Partitions the AST predicates into meta and data predicates. Moreover, it
// assigns each data predicate a name if a it coexists with name-extractor
// predicate that restricts the scope of the data predicate.
struct predicatizer : public expr::default_const_visitor
{
  virtual void visit(expr::conjunction const& conj)
  {
    std::vector<expr::ast> flat;
    std::vector<expr::ast> deep;

    // Partition terms into leaves and others.
    for (auto& operand : conj.operands)
    {
      auto op = expr::ast{*operand};
      if (op.is_predicate())
        flat.push_back(std::move(op));
      else
        deep.push_back(std::move(op));
    }

    // Extract the name for all predicates in this conjunction.
    //
    // TODO: add a check to the semantic pass after constructing queries to
    // flag multiple name/offset/time extractors in the same conjunction.
    name_.clear();
    for (auto& pred : flat)
    {
      name_checker checker;
      pred.accept(checker);
      if (checker.check_)
        name_ = checker.value_.get<string>();
    }

    for (auto& pred : flat)
      pred.accept(*this);

    // Proceed with the remaining conjunctions deeper in the tree.
    name_.clear();
    for (auto& n : deep)
      n.accept(*this);
  }

  virtual void visit(expr::disjunction const& disj)
  {
    for (auto& operand : disj.operands)
      operand->accept(*this);
  }

  virtual void visit(expr::predicate const& pred)
  {
    if (! expr::ast{pred}.is_meta_predicate())
      data_predicates_.emplace(name_, pred);
    else
      meta_predicates_.push_back(pred);
  }

  string name_;
  std::multimap<string, expr::ast> data_predicates_;
  std::vector<expr::ast> meta_predicates_;
};

} // namespace <anonymous>

partition::partition(path dir)
  : dir_{std::move(dir)}
{
}

path const& partition::dir() const
{
  return dir_;
}

bitstream const& partition::coverage() const
{
  return coverage_;
}

void partition::load()
{
  if (exists(dir_ / "coverage"))
    io::unarchive(dir_ / "coverage", coverage_);
}

void partition::save()
{
  if (coverage_)
  {
    if (! exists(dir_))
      mkdir(dir_);

    io::archive(dir_ / "coverage", coverage_);
  }
}

void partition::update(event_id base, size_t n)
{
  default_encoded_bitstream bs;
  assert(base > 0);
  bs.append(base, false);
  bs.append(n, true);
  if (! coverage_)
    coverage_ = std::move(bs);
  else
    coverage_ |= bs;
}

partition_actor::partition_actor(path dir, size_t batch_size)
  : partition_{std::move(dir)},
    batch_size_{batch_size}
{
}

void partition_actor::act()
{
  chaining(false);
  trap_exit(true);

  partition_.load();
  event_meta_index_ =
    spawn<event_meta_index, linked>(partition_.dir() / "meta");

  traverse(partition_.dir() / "event",
           [&](path const& p) -> bool
           {
             VAST_LOG_ACTOR_DEBUG("found existing event index: " << p);
             event_arg_indexes_.emplace(
                 p.basename().str(), spawn<event_arg_index, linked>(p));
             return true;
           });

  become(
      on(atom("EXIT"), arg_match) >> [=](uint32_t reason)
      {
        if (reason != exit::kill)
          partition_.save();

        quit(reason);
      },
      on_arg_match >> [=](expr::ast const& ast, actor_ptr const& sink)
      {
        assert(partition_.coverage());
        VAST_LOG_ACTOR_DEBUG("got AST for " << VAST_ACTOR_ID(sink) <<
                             ": " << ast);

        auto t = make_any_tuple(ast, partition_.coverage(), sink);
        if (ast.is_meta_predicate())
          event_meta_index_ << t;
        else
          // TODO: restrict to relevant events.
          for (auto& p : event_arg_indexes_)
            p.second << t;
      },
      on_arg_match >> [=](segment const& s)
      {
        VAST_LOG_ACTOR_DEBUG(
            "processes " << s.events() << " events from segment " << s.id());

        std::vector<cow<event>> meta_events;
        std::unordered_map<string, std::vector<cow<event>>> arg_events;
        meta_events.reserve(batch_size_);
        arg_events.reserve(batch_size_);

        segment::reader r{&s};
        while (auto e = r.read())
        {
          assert(e);
          auto ce = cow<event>{std::move(*e)};

          meta_events.push_back(ce);
          if (meta_events.size() == batch_size_)
          {
            send(event_meta_index_, std::move(meta_events));
            meta_events.clear();
          }

          auto& v = arg_events[ce->name()];
          v.push_back(ce);
          if (v.size() == batch_size_)
          {
            auto& a = event_arg_indexes_[ce->name()];
            if (! a)
            {
              auto dir = partition_.dir() / "event" / ce->name();
              a = spawn<event_arg_index, linked>(dir);
            }

            send(a, std::move(v));
            v.clear();
          }
        }

        if (! meta_events.empty())
          send(event_meta_index_, std::move(meta_events));

        for (auto& p : arg_events)
        {
          if (! p.second.empty())
          {
            auto& a = event_arg_indexes_[p.first];
            if (! a)
            {
              auto dir = partition_.dir() / "event" / p.first;
              a = spawn<event_arg_index, linked>(dir);
            }

            send(a, std::move(p.second));
          }
        }

        partition_.update(s.base(), s.events());

        send(event_meta_index_, atom("flush"));
        for (auto& p : arg_events)
          send(event_arg_indexes_[p.first], atom("flush"));
      });
}

char const* partition_actor::description() const
{
  return "partition";
}

} // namespace vast
