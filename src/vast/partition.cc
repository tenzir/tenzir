#include "vast/partition.h"

#include <cppa/cppa.hpp>
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

} // namespace <anonymous>

bool partition::is_meta_query(expr::ast const& ast)
{
  discriminator visitor;
  ast.accept(visitor);
  return visitor.meta;
}

partition::partition(path dir)
  : dir_{std::move(dir)},
    last_modified_{now()}
{
}

path const& partition::dir() const
{
  return dir_;
}

time_point partition::last_modified() const
{
  return last_modified_;
}

bitstream const& partition::coverage() const
{
  return coverage_;
}

void partition::load()
{
  if (! exists(dir_))
    mkdir(dir_);
  if (exists(dir_ / "last_modified"))
    io::unarchive(dir_ / "last_modified", last_modified_);
  if (exists(dir_ / "coverage"))
    io::unarchive(dir_ / "coverage", coverage_);
}

void partition::save()
{
  assert(exists(dir_));
  io::archive(dir_ / "last_modified", last_modified_);
  io::archive(dir_ / "coverage", coverage_);
}

void partition::update(event_id base, size_t n)
{
  // FIXME: use a compressed bitstream.
  null_bitstream bs;
  assert(base > 0);
  bs.append(base - 1, false);
  bs.append(n, true);
  if (! coverage_)
    coverage_ = std::move(bs);
  else
    coverage_ |= bs;
  last_modified_ = now();
}

partition_actor::partition_actor(path dir)
  : partition_{std::move(dir)}
{
}

void partition_actor::act()
{
  trap_exit(true);
  become(
      on(atom("EXIT"), arg_match) >> [=](uint32_t reason)
      {
        partition_.save();
        quit(reason);
      },
      on(atom("timestamp")) >> [=]
      {
        reply(partition_.last_modified());
      },
      on_arg_match >> [=](expr::ast const& ast, actor_ptr const& sink)
      {
        auto t = make_any_tuple(ast, partition_.coverage(), sink);
        if (partition::is_meta_query(ast))
          event_meta_index_ << t;
        else
          for (auto& p : event_arg_indexes_)
            p.second << t; // TODO: restrict to relevant events.
      },
      on_arg_match >> [=](segment const& s)
      {
        VAST_LOG_ACTOR_DEBUG("processes events from segment " << s.id());
        segment::reader r{&s};
        event e;
        while (r.read(e))
        {
          cow<event> ce{std::move(e)};
          event_meta_index_ << ce;
          auto& a = event_arg_indexes_[ce->name()];
          if (! a)
            a = spawn<event_arg_index, linked>(
                partition_.dir() / "event" / ce->name());
          a << ce;
        }
        partition_.update(s.base(), s.events());
      });

  partition_.load();
  event_meta_index_
    = spawn<event_meta_index, linked>(partition_.dir() / "meta");
  traverse(partition_.dir() / "event",
           [&](path const& p) -> bool
           {
             VAST_LOG_ACTOR_DEBUG("found existing event index: " << p);
             event_arg_indexes_.emplace(
                 p.basename().str(), spawn<event_arg_index, linked>(p));
             return true;
           });
}

char const* partition_actor::description() const
{
  return "partition";
}

} // namespace vast
