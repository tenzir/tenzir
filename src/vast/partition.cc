#include "vast/partition.h"

#include <cppa/cppa.hpp>
#include "vast/bitmap_index.h"
#include "vast/event.h"
#include "vast/event_index.h"
#include "vast/segment.h"
#include "vast/io/serialization.h"

namespace vast {

path const partition::part_meta_file = "partition";
path const partition::event_meta_dir = "meta";
path const partition::event_data_dir = "data";

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

partition::meta_data::meta_data(uuid id)
  : id{id}
{
}

void partition::meta_data::update(segment const& s)
{
  if (first_event == time_range{} || s.first() < first_event)
    first_event = s.first();

  if (last_event == time_range{} || s.last() > last_event)
    last_event = s.last();

  last_modified = now();

  assert(s.coverage());
  coverage |= *s.coverage();
}

void partition::meta_data::serialize(serializer& sink) const
{
  sink << id << first_event << last_event << last_modified << coverage;
}

void partition::meta_data::deserialize(deserializer& source)
{
  source >> id >> first_event >> last_event >> last_modified >> coverage;
}

bool operator==(partition::meta_data const& x, partition::meta_data const& y)
{
  return x.id == y.id
      && x.first_event == y.first_event
      && x.last_event == y.last_event
      && x.last_modified == y.last_modified
      && x.coverage == y.coverage;
}


partition::partition(uuid id)
  : meta_{std::move(id)}
{
}

void partition::update(segment const& s)
{
  meta_.update(s);
}

partition::meta_data const& partition::meta() const
{
  return meta_;
}

void partition::serialize(serializer& sink) const
{
  sink << meta_;
}

void partition::deserialize(deserializer& source)
{
  source >> meta_;
}


using namespace cppa;

partition_actor::partition_actor(path dir, size_t batch_size, uuid id)
  : dir_{std::move(dir)},
    batch_size_{batch_size},
    partition_{std::move(id)}
{
}

void partition_actor::act()
{
  chaining(false);
  trap_exit(true);

  if (exists(dir_))
    if (! io::unarchive(dir_ / partition::part_meta_file, partition_))
    {
      VAST_LOG_ACTOR_ERROR("failed to load partition " << dir_);
      quit(exit::error);
      return;
    }

  auto meta_index_dir = dir_ / partition::event_meta_dir;
  meta_index_ = spawn<event_meta_index, linked>(meta_index_dir);

  traverse(dir_ / partition::event_data_dir,
           [&](path const& p) -> bool
           {
             VAST_LOG_ACTOR_DEBUG("found existing event data index: " << p);
             auto a = spawn<event_data_index, linked>(p);
             data_indexes_.emplace(p.basename().str(), std::move(a));
             return true;
           });

  become(
      on(atom("EXIT"), arg_match) >> [=](uint32_t reason)
      {
        if (reason != exit::kill)
          if (partition_.meta().coverage)
            if (! io::archive(dir_ / partition::part_meta_file, partition_))
            {
              VAST_LOG_ACTOR_ERROR("failed to save partition " << dir_);
              quit(exit::error);
              return;
            }

        quit(reason);
      },
      on_arg_match >> [=](expr::ast const& pred, actor_ptr const& sink)
      {
        VAST_LOG_ACTOR_DEBUG("got predicate " << pred <<
                             " for " << VAST_ACTOR_ID(sink));

        auto t = make_any_tuple(pred, partition_.meta().id, sink);
        if (pred.is_meta_predicate())
          meta_index_ << t;
        else
          for (auto& p : data_indexes_)
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
          auto& v = arg_events[ce->name()];
          v.push_back(ce);

          if (meta_events.size() == batch_size_)
          {
            send(meta_index_, std::move(meta_events));
            meta_events.clear();
          }

          if (v.size() == batch_size_)
          {
            auto& a = data_indexes_[ce->name()];
            if (! a)
            {
              auto dir = dir_ / "data" / ce->name();
              a = spawn<event_data_index, linked>(dir);
            }

            send(a, std::move(v));
            v.clear();
          }
        }

        // Send away final events.
        if (! meta_events.empty())
          send(meta_index_, std::move(meta_events));

        for (auto& p : arg_events)
        {
          if (! p.second.empty())
          {
            auto& a = data_indexes_[p.first];
            if (! a)
            {
              auto dir = dir_ / partition::event_data_dir / p.first;
              a = spawn<event_data_index, linked>(dir);
            }

            send(a, std::move(p.second));
          }
        }

        // Record segment and flush both partition meta data and event indexes.
        partition_.update(s);

        assert(partition_.meta().coverage); // Also checked by index_actor.
        if (! io::archive(dir_ / partition::part_meta_file, partition_))
        {
          VAST_LOG_ACTOR_ERROR("failed to save partition " << dir_);
          quit(exit::error);
          return;
        }

        send(meta_index_, atom("flush"));
        for (auto& p : arg_events)
          send(data_indexes_[p.first], atom("flush"));
      });
}

char const* partition_actor::description() const
{
  return "partition";
}

} // namespace vast
