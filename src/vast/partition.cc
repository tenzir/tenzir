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
  virtual void visit(expr::relation const& rel)
  {
    rel.operands[0]->accept(*this);
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

partition::partition(path dir)
  : dir_{std::move(dir)},
    last_modified_{now()}
{
}

void partition::act()
{
  trap_exit(true);
  become(
      on(atom("EXIT"), arg_match) >> [=](uint32_t reason)
      {
        io::archive(dir_ / "last_modified", last_modified_);
        io::archive(dir_ / "coverage", coverage_);
        actor<partition>::on_exit();
        quit(reason);
      },
      on(atom("timestamp")) >> [=]
      {
        reply(last_modified_);
      },
      on_arg_match >> [=](expr::ast const& ast, actor_ptr const& sink)
      {
        discriminator visitor;
        ast.accept(visitor);
        auto t = make_any_tuple(ast, coverage_, sink);
        if (visitor.meta)
          event_meta_index_ << t;
        else // TODO: restrict to relevant indexes.
          for (auto& p : event_arg_indexes_)
            p.second << t;
      },
      on_arg_match >> [=](segment const& s)
      {
        segment::reader r{&s};
        event e;
        while (r.read(e))
        {
          auto t = make_any_tuple(std::move(e));
          event_meta_index_ << t;
          auto& a = event_arg_indexes_[e.name()];
          if (! a)
            a = spawn<event_arg_index, linked>(dir_ / "event"/ e.name());
          a << t;
        }

        last_modified_ = now();

        // FIXME: use compressed bitstream.
        null_bitstream bs;
        assert(s.base() > 0);
        bs.append(s.base() - 1, false);
        bs.append(s.events(), true);
        if (! coverage_)
          coverage_ = std::move(bs);
        else
          coverage_ |= bs;
      });

  if (! exists(dir_))
  {
    VAST_LOG_ACTOR_DEBUG("creates new directory " << dir_);
    mkdir(dir_);
  }

  if (exists(dir_ / "last_modified"))
    io::unarchive(dir_ / "last_modified", last_modified_);
  if (exists(dir_ / "coverage"))
    io::unarchive(dir_ / "coverage", last_modified_);

  event_meta_index_ = spawn<event_meta_index, linked>(dir_ / "meta");
  traverse(dir_ / "event",
           [&](path const& p) -> bool
           {
             event_arg_indexes_.emplace(
                 p.basename().str(), spawn<event_arg_index, linked>(p));
             return true;
           });

}

char const* partition::description() const
{
  return "partition";
}

} // namespace vast
