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
  VAST_LOG_ACT_VERBOSE("index", "spawned");

  become(
      on(atom("kill")) >> [=]
      {
        for (auto p : partitions_)
          p.second << last_dequeued();
        quit();
      },
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
        VAST_LOG_ACT_DEBUG("index", "sending events from segment " << s.id() <<
                           " to " << VAST_ACTOR("partition", active_));

        segment::reader r{&s};
        event e;
        while (r.read(e))
          send(active_, std::move(e));

        reply(atom("segment"), atom("ack"), s.id());
      });

  // FIXME: There's a libcppa problem with sync_send's following a become()
  // statement. If we copy the entire body of load() into this context, then
  // the sync_send replies won't arrive. But if we go through a function call,
  // it works. Weird. Race?
  load();
}

void index::on_exit()
{
  VAST_LOG_ACT_VERBOSE("index", "terminated");
}

void index::load()
{
  if (! exists(dir_))
  {
    VAST_LOG_ACT_INFO("index", "creates new directory " << dir_);
    if (! mkdir(dir_))
    {
      VAST_LOG_ACT_ERROR("index", "failed to create " << dir_);
      quit();
    }
  }
  else
  {
    auto latest = std::make_shared<time_point>(0);
    traverse(
        dir_,
        [&](path const& p) -> bool
        {
          VAST_LOG_ACT_VERBOSE("index", "found partition " << p);
          auto part = spawn<partition>(p);
          auto id = uuid{to_string(p.basename())};
          partitions_.emplace(id, part);

          sync_send(part, atom("meta"), atom("timestamp")).then(
              on_arg_match >> [=](time_point tp)
              {
                if (tp >= *latest)
                {
                  VAST_LOG_ACT_DEBUG("index", "marked partition " << p <<
                                     " as active (" << tp << ")");
                  *latest = tp;
                  active_ = part;
                }
              });

          return true;
        });
  }

  if (partitions_.empty())
  {
    auto id = uuid::random();
    auto p = spawn<partition>(dir_ / to<string>(id));
    active_ = p;
    partitions_.emplace(std::move(id), std::move(p));
  }
}

} // namespace vast
