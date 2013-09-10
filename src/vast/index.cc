#include "vast/index.h"

#include "vast/bitmap_index.h"
#include "vast/logger.h"
#include "vast/segment.h"
#include "vast/expression.h"
#include "vast/partition.h"

using namespace cppa;

namespace vast {
namespace {

/// Determines whether a query expression has clauses that may benefit from an
/// index lookup.
class checker : public expr::const_visitor
{
public:
  operator bool() const
  {
    return positive_;
  }

  virtual void visit(expr::node const&)
  {
    assert(! "should never happen");
  }

  virtual void visit(expr::timestamp_extractor const&)
  {
    positive_ = true;
  }

  virtual void visit(expr::name_extractor const&)
  {
    positive_ = true;
  }

  virtual void visit(expr::id_extractor const&)
  {
    /* Do exactly nothing. */
  }

  virtual void visit(expr::offset_extractor const&)
  {
    /* Do exactly nothing. */
  }

  virtual void visit(expr::type_extractor const&)
  {
    /* Do exactly nothing. */
  }

  virtual void visit(expr::conjunction const& conj)
  {
    for (auto& op : conj.operands())
      if (! positive_)
        op->accept(*this);
  }

  virtual void visit(expr::disjunction const& disj)
  {
    for (auto& op : disj.operands())
      if (! positive_)
        op->accept(*this);
  }

  virtual void visit(expr::relation const& rel)
  {
    assert(rel.operands().size() == 2);
    rel.operands()[0]->accept(*this);
  }

  virtual void visit(expr::constant const&)
  {
    /* Do exactly nothing. */
  }

private:
  bool positive_ = false;
};

} // namespace <anonymous>


index::index(path directory)
  : dir_{std::move(directory)}
{
}

void index::init()
{
  VAST_LOG_VERBOSE(VAST_ACTOR("index") << " spawned");

  if (! exists(dir_))
  {
    VAST_LOG_INFO(VAST_ACTOR("index") << " creates new directory " << dir_);
    if (! mkdir(dir_))
    {
      VAST_LOG_ERROR(VAST_ACTOR("index") << " failed to create " << dir_);
      quit();
    }
  }

  become(
      on(atom("load")) >> [=]
      {
        traverse(
            dir_,
            [&](path const& p) -> bool
            {
              VAST_LOG_VERBOSE(VAST_ACTOR("index") << " found partition " << p);
              auto part = spawn<partition>(p);
              auto id = uuid{to_string(p.basename())};
              partitions_.emplace(id, part);
              return true;
            });

        for (auto p : partitions_)
          sync_send(p.second, atom("get"), atom("timestamp")).then(
              on_arg_match >> [=]()
              {
              });

        if (partitions_.empty())
        {
          auto id = uuid::random();
          auto p = spawn<partition>(dir_ / to<string>(id));
          active_ = p;
          partitions_.emplace(std::move(id), std::move(p));
        }

      },
//      on(atom("create"), arg_match) >> [=](schema const& sch)
//      {
//        for (auto& e : sch.events())
//        {
//          if (! e.indexed)
//            continue;
//
//          VAST_LOG_DEBUG("index @" << id() <<
//                         " creates index for event " << e.name);
//
//          for (auto& arg : e.args)
//          {
//            if (! arg.indexed)
//              continue;
//
//            VAST_LOG_DEBUG("index @" << id() <<
//                           " creates index for argument " << arg.name);
//          }
//        }
//      },
      on(atom("query"), arg_match) >> [=](expression const& expr)
      {
        checker chkr;
        expr.accept(chkr);
        if (! chkr)
        {
          reply(atom("impossible"));
          return;
        }

        // TODO: parse expression and hit indexes.
      },
      on(arg_match) >> [=](segment const& s)
      {
        assert(active_);

        segment::reader r(&s);
        event e;
        while (r.read(e))
          send(active_, std::move(e));

        reply(atom("segment"), atom("ack"), s.id());
      },
      on(atom("kill")) >> [=]
      {
        for (auto p : partitions_)
          p.second << last_dequeued();
        quit();
      });
}

void index::on_exit()
{
  VAST_LOG_VERBOSE(VAST_ACTOR("index") << " terminated");
}

void index::load()
{
}

} // namespace vast
