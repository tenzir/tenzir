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

class discriminator : public expr::default_const_visitor
{
public:
  virtual void visit(expr::predicate const& pred)
  {
    pred.lhs().accept(*this);
  }

  virtual void visit(expr::offset_extractor const&)
  {
    meta = false;
  }

  virtual void visit(expr::type_extractor const&)
  {
    meta = false;
  }

  bool meta = true;
};

bool is_meta_query(expr::ast const& ast)
{
  discriminator visitor;
  ast.accept(visitor);
  return visitor.meta;
}

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

partition_actor::partition_actor(path dir)
  : partition_{std::move(dir)}
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
        VAST_LOG_ACTOR_DEBUG("got AST " << ast <<
                             " for " << VAST_ACTOR_ID(sink));

        assert(partition_.coverage());
        auto t = make_any_tuple(ast, partition_.coverage(), sink);
        if (is_meta_query(ast))
          event_meta_index_ << t;
        else
          for (auto& p : event_arg_indexes_)
            p.second << t; // TODO: restrict to relevant events.
      },
      on_arg_match >> [=](segment const& s)
      {
        VAST_LOG_ACTOR_DEBUG(
            "processes " << s.events() << " events from segment " << s.id());

        size_t const batch_size = 5000;  // TODO: Make configurable.
        std::vector<cow<event>> meta_events;
        std::unordered_map<string, std::vector<cow<event>>> arg_events;

        segment::reader r{&s};
        while (auto e = r.read())
        {
          assert(e);
          auto ce = cow<event>{std::move(*e)};

          meta_events.push_back(ce);
          if (meta_events.size() == batch_size)
          {
            send(event_meta_index_, std::move(meta_events));
            meta_events.clear();
          }

          auto& v = arg_events[ce->name()];
          v.push_back(ce);
          if (v.size() == batch_size)
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

        partition_.update(s.base(), s.events());
      });
}

char const* partition_actor::description() const
{
  return "partition";
}

} // namespace vast
